//! `std:secrets` from a Deno guest: `secrets.get` reads only secret values
//! (never a plain env var), `secrets.require` returns the set of them or throws one `secret_missing`
//! error naming every missing secret and the command to set them, the object cannot be dumped
//! (`JSON.stringify` gives nothing, no own keys), and no value reaches an error message.

use crate::common;

use common::*;
use serde_json::json;

const CANARY: &str = "canary-api-key-5c2e";

const SECRETS: &str = "import { secrets } from 'std:secrets';\n\
const code = (f: () => unknown) => { try { f(); return 'ok'; } catch (e) { return (e as { code?: string }).code ?? String(e); } };\n\
export default async () => {\n\
  const out: Record<string, unknown> = {};\n\
  out.get = secrets.get('API_KEY');\n\
  out.plainIsNotASecret = secrets.get('PLAIN');\n\
  out.unset = secrets.get('NOPE');\n\
  out.require = secrets.require('API_KEY', 'DB_URL');\n\
  try {\n\
    secrets.require('API_KEY', 'NOPE', 'OTHER');\n\
  } catch (e) {\n\
    const err = e as { code?: string; message: string; missing?: string[] };\n\
    out.missingCode = err.code;\n\
    out.missingNames = err.missing;\n\
    out.missingMessage = err.message;\n\
  }\n\
  out.jsonIsUndefined = JSON.stringify(secrets) === undefined;\n\
  out.ownKeys = Object.keys(secrets);\n\
  out.badName = code(() => secrets.get('lower'));\n\
  return Response.json(out);\n\
};\n";

#[test]
fn ac_dno_33_std_secrets_reads_only_secret_values_and_never_dumps_them() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(
            svc(1),
            &[("PLAIN", "plain-value")],
            &[("API_KEY", CANARY), ("DB_URL", "postgres://db")],
        );
        let p = h.prepare(&ts_upload(SECRETS)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.status, 200, "{}", got.text());
        let v = got.json();

        assert_eq!(v["get"], json!(CANARY));
        assert_eq!(v["plainIsNotASecret"], json!(null), "a plain env var is not a secret");
        assert_eq!(v["unset"], json!(null));
        assert_eq!(v["require"], json!({ "API_KEY": CANARY, "DB_URL": "postgres://db" }));
        assert_eq!(v["missingCode"], json!("secret_missing"));
        assert_eq!(
            v["missingNames"],
            json!(["NOPE", "OTHER"]),
            "every missing name, in call order"
        );
        let msg = v["missingMessage"].as_str().expect("message");
        assert!(msg.contains("NOPE") && msg.contains("OTHER"), "{msg}");
        assert!(
            msg.contains("21ci secret set <service> NOPE"),
            "the command to set it: {msg}"
        );
        assert!(
            !msg.contains(CANARY) && !msg.contains("API_KEY"),
            "no value in errors: {msg}"
        );
        assert_eq!(
            v["jsonIsUndefined"],
            json!(true),
            "JSON.stringify(secrets) gives nothing"
        );
        assert_eq!(v["ownKeys"], json!([]));
        assert_eq!(v["badName"], json!("validation_failed"));
    });
}

const LOGS_A_SECRET: &str = "import { secrets } from 'std:secrets';\n\
import { log } from 'std:log';\n\
export default async () => {\n\
  const key = secrets.get('API_KEY');\n\
  console.log(`calling with ${key}`);\n\
  console.error('short ' + secrets.get('SHORT'));\n\
  log.info('fields', { token: key, n: 1 });\n\
  return new Response('ok');\n\
};\n";

/// A guest that prints a secret value does not put it in the service's logs (security review
/// finding 7): values of 8 characters or more are replaced by `[secret]`.
#[test]
fn a_secret_value_printed_by_the_guest_is_redacted_in_its_logs() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        h.control.set_env(svc(1), &[], &[("API_KEY", CANARY), ("SHORT", "abc")]);
        let p = h.prepare(&ts_upload(LOGS_A_SECRET)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.status, 200, "{}", got.text());
        h.settle(std::time::Duration::from_millis(20)).await;
        let recs = h.log_records();
        let all = serde_json::to_string(
            &recs
                .iter()
                .map(|r| (r.msg.clone(), r.fields.clone()))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(!all.contains(CANARY), "{all}");
        assert!(all.contains("calling with [secret]"), "{all}");
        assert!(all.contains("short abc"), "short values are left alone: {all}");
        assert!(all.contains(r#""token":"[secret]""#), "{all}");
    });
}
