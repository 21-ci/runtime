# env-secrets

Configuration through `Deno.env`, and what the guest does and does not see in request headers.

## Shows

- `Deno.env.get(name)` reads both **env** and **secrets**; `Deno.env` is a read-only shim (`set` throws)
- Env is set with `PUT /v1/services/{id}/env` (the whole map; readable through the API); secrets with `PUT /v1/services/{id}/secrets/{NAME}` (write-only, sealed at rest; `GET …/secrets` lists names only)
- Changes apply to the **next request**, no redeploy
- Names match `[A-Z_][A-Z0-9_]*`; the `RT_` prefix is reserved; <= 100 entries, values <= 32 KB
- Header hygiene: public `x-rt-*` request headers are stripped and replaced by runtime-set ones; the credential used for invoke access never reaches the guest

## Deploy

```bash
cd sample/deno/env-secrets
../../scripts/deploy.sh . env-secrets
```

## Configure

```bash
SVC=$(../../scripts/svc-id.sh env-secrets)
AUTH="Authorization: Bearer $RT_KEY"

curl -sS -X PUT "$RT_API/v1/services/$SVC/env" -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"ENVIRONMENT": "staging", "LOG_LEVEL": "debug", "FEATURE_FLAG": "true"}'

curl -sS -X PUT "$RT_API/v1/services/$SVC/secrets/STRIPE_API_KEY" -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"value": "sk_test_not_a_real_key"}'
```

Needs the `env:write` and `secrets:write` scopes (or the root token).

## Invoke

```bash
S=../../scripts/invoke.sh
$S env-secrets /
$S env-secrets /secrets
$S env-secrets /readonly
RT_HEADERS='x-rt-invoker: admin' $S env-secrets /headers     # a spoofed x-rt-* header is dropped
```

## Expected responses

```json
// /
{ "environment": "staging", "vars": { "ENVIRONMENT": "staging", "LOG_LEVEL": "debug", "FEATURE_FLAG": "true" } }

// /secrets  (presence only; the example never returns a secret value)
{ "configured": { "DATABASE_URL": false, "STRIPE_API_KEY": true } }

// /readonly
{ "readonly": true, "error": "TypeError" }

// /headers
{
  "x-rt-task-id": "task_01J…",
  "x-rt-trigger": "http",
  "x-rt-base-path": "/s/env-secrets",
  "x-rt-invoker": null,
  "ctx.invoker": null,
  "hasAuthorization": false
}
```

## Notes

- A secret is as readable as env **inside** the guest: never log or return it. Other services cannot read it.
- `x-rt-invoker` (and `ctx.invoker`) is only set for calls through the internal listener.
