#!/usr/bin/env bash
# E2E 21: std:kv (sample/deno/kv-counter), std:sql (sample/deno/sql-todo) and std:sqlite (sample/deno/sqlite-sync)
# from deployed services.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup_tenant

step "kv-counter: increment / get / list / delete"
kv="$(uniq_slug e2e-kv)"
deploy "$SAMPLE_DIR/deno/kv-counter" "$kv"
for want in 1 2 3; do
  invoke "$kv" "/?key=visits&action=increment"
  expect_code 200 "increment"
  expect_eq "$(jqr .value)" "$want" "counter after increment $want"
done
invoke "$kv" "/?key=visits&action=get"
expect_eq "$(jqr .value)" 3 "stored value"
invoke "$kv" "/?action=list"
expect_code 200 "kv list"
expect_jq '.entries | map(.key) | index("counters/visits") != null' "the key shows up in kv.list"
invoke "$kv" "/?key=visits&action=delete"
expect_code 200 "delete"
invoke "$kv" "/?key=visits&action=get"
expect_eq "$(jqr .value)" 0 "value after delete"

step "kv data is per service: another service does not see it"
kv2="$(uniq_slug e2e-kv2)"
deploy "$SAMPLE_DIR/deno/kv-counter" "$kv2"
invoke "$kv" "/?key=iso&action=increment"
invoke "$kv2" "/?key=iso&action=get"
expect_eq "$(jqr .value)" 0 "another service's KV is separate"

step "sql-todo: schema on demand, CRUD, batch stats"
sq="$(uniq_slug e2e-sql)"
deploy "$SAMPLE_DIR/deno/sql-todo" "$sq"
sqid="$(svc_id "$sq")"
invoke "$sq" "/"
expect_code 200 "list (creates the schema)"
expect_jq '.todos | length == 0' "empty list"
invoke_json POST "$sq" "/" '{"title":"write e2e tests"}'
expect_code 201 "create todo"
todo_id="$(jqr .id)"
invoke_json POST "$sq" "/" '{"title":"ship"}'
expect_code 201 "create second todo"
invoke_json PATCH "$sq" "/$todo_id" '{"done":true}'
expect_code 200 "patch"
expect_jq '.done == true' "todo is done"
invoke "$sq" "/stats"
expect_code 200 "stats"
expect_jq '.total == 2 and .open == 1' "stats from one read batch"
invoke "$sq" "/?done=1"
expect_jq '.todos | length == 1' "filter by done"
invoke_json DELETE "$sq" "/$todo_id"
expect_code 200 "delete"
invoke "$sq" "/$todo_id"
expect_code 404 "deleted todo is gone"
invoke_json POST "$sq" "/" '{"nope":1}'
expect_code 400 "validation error from the guest"

step "sqlite-sync: synchronous std:sqlite CRUD, an all-or-none bulk transaction"
ss="$(uniq_slug e2e-sqlite)"
deploy "$SAMPLE_DIR/deno/sqlite-sync" "$ss"
invoke "$ss" "/"
expect_code 200 "list (creates the schema at module scope)"
expect_jq '.todos | length == 0' "empty list"
invoke_json POST "$ss" "/" '{"title":"one"}'
expect_code 201 "create todo"
lite_id="$(jqr .id)"
invoke_json POST "$ss" "/bulk" '{"titles":["two","three"]}'
expect_code 201 "bulk create in one transaction"
expect_jq '.ids | length == 2' "two ids"
invoke_json POST "$ss" "/bulk" '{"titles":["x"," "]}'
expect_code 400 "an empty title rejects the whole bulk"
invoke_json PATCH "$ss" "/$lite_id" '{"done":true}'
expect_code 200 "patch"
invoke "$ss" "/stats"
expect_jq '.total == 3 and .open == 2' "nothing of the rejected bulk was stored"
invoke_json DELETE "$ss" "/$lite_id"
expect_code 200 "delete"
invoke "$ss" "/$lite_id"
expect_code 404 "deleted todo is gone"

step "data export (scope data:admin) is a SQLite file"
HTTP_TOKEN="$TENANT_KEY" http GET "$RT_API/v1/services/$sqid/data/export"
expect_code 200 "data export"
head -c 15 "$BODY_FILE" | grep -q "SQLite format 3" || fail "export is not a SQLite database"

pass
