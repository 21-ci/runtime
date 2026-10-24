---
name: storage-schema
description: Use when changing an on-disk SQLite schema of this runtime - meta.db (tenants, keys, services, versions, events), queue.db (queue, DLQ, delayed), or the per-service data.db runtime tables (_rt_kv, _rt_durable, _rt_durable_alarm, _rt_blobs) - or when adding a column/table/index in crates/rt-store/migrations or the store code that reads it.
---

# Storage schema changes (`crates/rt-store`)

`0.1.0` has shipped (see `CHANGELOG.md`). Nodes in the field now have real `meta.db` and `queue.db` files. A schema
change must upgrade them in place. Never edit a released migration.

## The three kinds of database

| DB | Defined in | Versioning | Applied |
|---|---|---|---|
| `meta.db` | `crates/rt-store/migrations/meta_0001_init.sql` → `MIGRATIONS` in `crates/rt-store/src/meta.rs` (re-exported as `rt_store::META_MIGRATIONS`) | `PRAGMA user_version`, forward-only | `SqliteMetaStore::init`, before readers open |
| `queue.db` | `migrations/queue_0001_init.sql`, `queue_0002_delayed.sql` → `MIGRATIONS` in `src/queue.rs` (`QUEUE_MIGRATIONS`) | `PRAGMA user_version` | on open in `queue.rs` |
| per-service `data.db` | `migrations/service_data_0001_init.sql` as `SERVICE_DATA_BOOTSTRAP`, plus `BLOB_BOOTSTRAP`, both in `src/data.rs` | **none**: idempotent `CREATE ... IF NOT EXISTS`, run on every open | each open of a service DB |

`sqlutil::migrate` (`crates/rt-store/src/sqlutil.rs`) runs each pending `(version, sql)` in its own transaction and
sets `user_version`. A database **newer** than the binary is refused with `StoreError::Corrupt`.

## Adding a meta.db or queue.db change

1. Add a new file `crates/rt-store/migrations/<db>_000N_<what>.sql`, where N is the next number.
2. Append `(N, include_str!("../migrations/<db>_000N_<what>.sql"))` to that store's `MIGRATIONS`.
3. Write SQL that works on a **populated** database:
   - `ALTER TABLE ... ADD COLUMN` with a default, or a backfill `UPDATE`;
   - new tables and indexes.

   SQLite cannot drop or alter a constraint in place. If you need to, rebuild the table (create new, copy,
   drop, rename) inside the migration.
4. Update the store code (`meta.rs`, `queue.rs`) and the row types. Readers are pooled read-only connections
   (`open_reader`), so keep queries index-backed. `meta.db` must stay off the per-invocation path.
5. Tests, in `crates/rt-store/tests/it/`:
   - `meta_store.rs` `ac_meta_01_schema_version_matches_newest_embedded_migration` follows automatically;
   - **add an upgrade test**: build a DB at version N-1 with rows in it, open it with the current store, and assert
     that the data survived and the new column or table works. No such test exists yet; the first one sets the
     pattern;
   - a test that a DB newer than the binary is refused, if you touch `migrate`;
   - fault paths with `Faults::fail_next` (`tests/it/faults.rs`) if the change adds a write path.
6. If an operator must act (back up first, or a one-way change), note it in `docs/deploy.md`, which already has
   the backup and upgrade notes.

Before 0.1.0, unreleased schema was folded into `*_0001_init.sql` (`5bfb7c7`). Do not do that any more.

## Adding a data.db runtime table or index

- Add it to `service_data_0001_init.sql` (or `BLOB_BOOTSTRAP`) as `CREATE TABLE/INDEX IF NOT EXISTS`. This is how
  the durable-object tables were added (`aedb65e`). Existing service DBs get it on their next open.
- **Names must start with `_rt_`.** The SQLite authorizer (`guest_auth` in `src/data.rs`) hides `_rt_*` from guest
  SQL and refuses renames into that namespace (`daba865`). Add a test in `tests/it/sql_store.rs` that guest SQL
  cannot read or write the new table.
- **You cannot change an existing `_rt_*` table** with `IF NOT EXISTS`. A column change needs a real version.
  - Do **not** use `PRAGMA user_version` for this: it is not in `DENIED_PRAGMAS`, so guest SQL can set it.
  - Keep the version in an `_rt_` table instead (e.g. `_rt_schema`), and make the migration idempotent, because
    it runs per service file on open, concurrently with other services.
- Rows count against the tenant's `storage_mb` (`max_page_count` ceiling). Keep runtime overhead per row small.
- Durable and KV writes go through the group-commit writer (`run_group` in `data.rs`; `durable.rs` uses one
  savepoint per job). New write paths should coalesce the same way. See the `performance` skill.

## Checklist
- [ ] There is a new numbered migration file; no released file was edited.
- [ ] The upgrade-from-previous-version test runs with data present.
- [ ] Guest isolation is tested for any new `_rt_*` table; quota impact is considered.
- [ ] `cargo test -p rt-store --test it <module>::`, `make lint` and `make coverage MODULES="store"` pass.
- [ ] Upgrade notes are added to `docs/deploy.md` if operators must act.
