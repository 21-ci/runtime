//! WSM-33: `std:host/secrets.get` from a component (the `hello` fixture's
//! `/secret` route) reads only secret values, answers `none` for anything else, and refuses a name
//! that is not a secret name with `validation_failed`.

use crate::common;

use common::*;
use rt_types::EnvBundle;

const CANARY: &str = "canary-api-key-5c2e";

#[tokio::test]
async fn ac_wsm_33_secrets_get_reads_only_secret_values() {
    let mut rig = Rig::new();
    *rig.control.env.lock() = EnvBundle {
        env: vec![("PLAIN".to_string(), "plain-value".to_string())],
        secrets: vec![("API_KEY".to_string(), CANARY.to_string())],
    };
    let p = prepared("hello");
    assert_eq!(rig.run(&p, Req::get("/secret?name=API_KEY")).await.text(), CANARY);
    assert_eq!(
        rig.run(&p, Req::get("/secret?name=PLAIN")).await.text(),
        "none",
        "a plain env var is not a secret"
    );
    assert_eq!(rig.run(&p, Req::get("/secret?name=NOPE")).await.text(), "none");
    assert_eq!(
        rig.run(&p, Req::get("/secret?name=lower")).await.text(),
        "error:validation_failed"
    );
}
