# Changelog

## [Unreleased](https://github.com/zifter/clickhouse-migrations/compare/v0.13.0...main)

**What's Changed:**
- Add a `new` subcommand that scaffolds the next migration file (`clickhouse-migrations new "add events"` → `migrations/004_add_events.sql`), with `--down` for the paired rollback file, `--version` to force a version and `--dir` to pick the directory. It is purely local and never connects to ClickHouse. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/86. Closes #74.
- Make the migrations table configurable: `--migrations-table` / `MIGRATIONS_TABLE` (accepts a `database.table` form) and `--migrations-table-engine` / `MIGRATIONS_TABLE_ENGINE` (a full engine clause, passed through verbatim, which wins over the `--cluster-name` default). This unblocks `Replicated` database engines and custom ZooKeeper layouts. Defaults are unchanged. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/87. Closes #76.
- Add `migrate --to VERSION` (Python: `migrate(to_version=...)`) to apply pending migrations only up to a target version, for staged rollouts. It fails if the version is unknown or below the highest applied one (`migrate` never rolls back, use `down`), is a no-op when equal to it, works with `--dry-run` and `--fake`, and cannot be combined with `--migrations`. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/89. Closes #81.
- Accept `db_url` / `--db-url` / `DB_URL` with the `clickhouse-connect` driver as well (it is how ClickHouse Cloud hands out credentials, and the natural single secret for the GitHub Action and Docker image). `clickhouse://` maps to `http://` (port 8123), `clickhouses://` to `https://` (port 8443), `http(s)://` passes through; `http(s)://` with `clickhouse-driver` is rejected with a clear error. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/90. Closes #80.
- Add an opt-in migration lock so concurrent runs cannot interleave (e.g. a Kubernetes `Job` with several replicas): with `--lock` / `LOCK=true`, `migrate` and `down` hold a per-database lock in a `KeeperMap` table (`<migrations table>_lock`), acquired with `keeper_map_strict_mode = 1` so only one run can win, released in a `finally` and only for its own `owner` (`<host>:<pid>:<uuid>`). `--lock-timeout` (`LOCK_TIMEOUT`, default 300s) waits for a lock held by another run, `--lock-ttl` (`LOCK_TTL`, default 3600s) takes over a stale one race-free, and the new `unlock` subcommand force-releases a lock left by a dead run; `status`, `--dry-run` and `new` never lock. Off by default: nothing changes for existing users, and a run without `--lock` does no lock-related work at all. `--lock` needs ClickHouse 22.9+ with Keeper and `<keeper_map_path_prefix>` configured, and fails with a clear message when they are missing. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/92. Closes #75, #38.
- Add `status --strict` (exit code 1 on `md5-mismatch` / `unknown` rows), `--exit-code-pending` (exit code 1 on `pending` rows), `--format json` for machine-readable output and a `has_down` column showing whether a `.down.sql` exists. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/88. Closes #77.
- Add a `dump` subcommand (Python: `ClickhouseCluster.dump()`) that prints every table, view, materialized view and dictionary of a database as portable, dependency-ordered SQL: no `UUID`, no database prefix, `Replicated*MergeTree` path/replica arguments removed (`--keep-replicated-paths` keeps them), the migrations and lock tables and `.inner` tables excluded. `--out FILE` writes it atomically, `--check FILE` exits 1 with a unified diff on drift for CI, `--tables` limits the objects. Read-only; works with both drivers. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/91. Part of #83.
- Add transport options for both drivers, with host/port arguments and with `--db-url`, on every subcommand that connects: `--ca-cert`, `--cert`, `--key` (custom CA / mutual TLS), `--verify` / `--no-verify` (logs a warning), `--connect-timeout`, `--query-timeout` and a repeatable `--setting name=value` that is sent with every statement the tool runs — the fix for a `SET` at the top of a multi-statement file not reaching the next statement over HTTP. Environment variables are `CLICKHOUSE_CA_CERT`, `CLICKHOUSE_CERT`, `CLICKHOUSE_KEY`, `CLICKHOUSE_VERIFY`, `CLICKHOUSE_CONNECT_TIMEOUT`, `CLICKHOUSE_QUERY_TIMEOUT` and `CLICKHOUSE_SETTINGS="a=1,b=2"`; in Python they are new `ClickhouseCluster` parameters (`ca_cert`, `cert`, `key`, `verify`, `connect_timeout`, `query_timeout`, `settings`). Extra `ClickhouseCluster` keyword arguments now also reach `clickhouse_driver.Client` with `db_url`. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/94. Closes #79.
- Add opt-in `${NAME}` substitution in migration and `.down.sql` files for per-environment SQL (cluster names, dictionary sources): `--var NAME=VALUE` (repeatable, `MIGRATION_VARS`) enables it on its own, `--substitute-env` (`SUBSTITUTE_ENV`) also uses the process environment, `--var` wins (Python: `variables=` / `substitute_env=` on `migrate` and `rollback`). Unset, malformed and unterminated placeholders fail before any migration SQL runs, `$${NAME}` is a literal `${NAME}`, and errors never print values. The md5 and the stored `script` stay the raw file text, so changing a variable does not re-run a migration; our logs show raw SQL, the server's `query_log` still sees substituted values. Without the flags files run byte for byte as before. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/95. Closes #78.


## [v0.13.0](https://github.com/zifter/clickhouse-migrations/tree/v0.13.0) (2026-07-08)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.12.0...v0.13.0)

**What's Changed:**
- Add naive rollback support: optional paired `{VERSION}_{name}.down.sql` files and a `down` subcommand (`--steps` / `--to` / `--dry-run`) to reverse applied migrations. There is no automatic rollback (ClickHouse has no transactional DDL) — down scripts are explicit and hand-written. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/69. Closes #36, #67.


## [v0.12.0](https://github.com/zifter/clickhouse-migrations/tree/v0.12.0) (2026-07-02)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.11.0...v0.12.0)

**What's Changed:**
- Support the official `clickhouse-connect` (HTTP) driver alongside the native `clickhouse-driver`, selectable with `--driver` / `DRIVER` (install via `pip install 'clickhouse-migrations[connect]'`). Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/59, https://github.com/zifter/clickhouse-migrations/pull/62
- Add a `status` subcommand to show applied vs pending migrations without applying anything. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/58
- Add a GitHub Action and a Docker image (GHCR) for running migrations; both bundle the native and `clickhouse-connect` drivers. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/54, https://github.com/zifter/clickhouse-migrations/pull/64
- Derive the package version from the git tag via setuptools-scm. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/48
- Fix boolean CLI argument parsing on Python 3.14. Done by @UnoYakshi in https://github.com/zifter/clickhouse-migrations/pull/47
- Upload coverage to Codecov, add community health files, and improve the README (quick start, fixed badges, social preview). Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/52, https://github.com/zifter/clickhouse-migrations/pull/51, https://github.com/zifter/clickhouse-migrations/pull/63


## [v0.11.0](https://github.com/zifter/clickhouse-migrations/tree/v0.11.0) (2026-06-19)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.10.0...v0.11.0)

**What's Changed:**
- Add `--migration-log-format` option (`full`/`compact`) to control migration log verbosity. Done by @MaximTar in https://github.com/zifter/clickhouse-migrations/pull/44


## [v0.10.0](https://github.com/zifter/clickhouse-migrations/tree/v0.10.0) (2026-04-19)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.9.1...v0.10.0)

**What's Changed:**
- Add Python 3.14 support. Done by @zifter
- Improve README: fix typos, add migration example, document all parameters. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/41
- Add star history chart to README. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/42


## [v0.8.0](https://github.com/zifter/clickhouse-migrations/tree/v0.8.0) (2024-08-18)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.7.1...v0.8.0)

**What's Changed:**
- Add option --fake/--no-fake, which can help update schema_version without executing statements from migration files #27. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/28
- Add option --migrations which can help to specify explicitly migrations to apply. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/28

**Breaking changes:**
- Drop python 3.8 support. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/28
- Option --multi-statement, --dry-run, --secure now working without passing value. Just use --multi-statement/--no-multi-statement, --dry-run/--no-dry-run, --secure/--no-secure for enabling or disabling option. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/28


## [v0.7.1](https://github.com/zifter/clickhouse-migrations/tree/v0.7.1) (2024-07-01)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.7.0...v0.7.1)

**What's Changed:**
- Allow default db name #24. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/26


## [v0.7.0](https://github.com/zifter/clickhouse-migrations/tree/v0.7.0) (2024-07-01)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.6.0...v0.7.0)

**What's Changed:**
- #24 Allow connection string for initialization of ClickhouseCluster. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/25
