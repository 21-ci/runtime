# blob-upload

Object storage with `std:blob`, streamed in both directions. There is no filesystem API for guests: this is how a
service "writes a file".

## Shows

- `import { blob } from "std:blob"`
- `blob.put(key, body, { contentType })`: the request body (`req.body`, a `ReadableStream`) goes straight to storage, no buffering in the isolate
- `blob.get(key)` -> `{ body: ReadableStream, size, contentType } | null`, returned as the response body (streamed)
- `blob.head(key)`, `blob.delete(key)`, `blob.list({ prefix, cursor, limit })`
- Limits: key <= 1 KB, object <= 100 MB (plan)

## Deploy

```bash
cd sample/deno/blob-upload
../../scripts/deploy.sh . blob-upload
```

## Invoke

The key is the URL path after the slug.

```bash
URL=${RT_INVOKE:-http://127.0.0.1:8080}/s/blob-upload

# upload (streamed)
curl -sS -T ./README.md -H 'Content-Type: text/markdown' "$URL/docs/readme.md"

# metadata, download, list, delete
curl -sSI "$URL/docs/readme.md"
curl -sS  "$URL/docs/readme.md" -o /tmp/readme.md
../../scripts/invoke.sh blob-upload '/?prefix=docs/'
curl -sS -X DELETE "$URL/docs/readme.md"
```

## Expected responses

```json
// PUT /docs/readme.md (201)
{ "key": "docs/readme.md", "size": 1234, "contentType": "text/markdown" }
```

`HEAD` answers with `content-type` and `content-length`; `GET` of a missing key answers `404 {"error": "not found"}`.
`list` returns the SDK result as-is (objects under the prefix and the cursor for the next page).

## Notes

- The default HTTP request body limit is 10 MB (`limits.max_body_bytes`), so larger objects need a higher service limit (within the plan).
- Each service has its own blob namespace; another service reading the same key gets nothing.
