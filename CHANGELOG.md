# Changelog

## [Unreleased](https://github.com/zifter/clickhouse-migrations/compare/v0.13.0...main)

**What's Changed:**
- Add a `new` subcommand that scaffolds the next migration file (`clickhouse-migrations new "add events"` → `migrations/004_add_events.sql`), with `--down` for the paired rollback file, `--version` to force a version and `--dir` to pick the directory. It is purely local and never connects to ClickHouse. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/86. Closes #74.
- Make the migrations table configurable: `--migrations-table` / `MIGRATIONS_TABLE` (accepts a `database.table` form) and `--migrations-table-engine` / `MIGRATIONS_TABLE_ENGINE` (a full engine clause, passed through verbatim, which wins over the `--cluster-name` default). This unblocks `Replicated` database engines and custom ZooKeeper layouts. Defaults are unchanged. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/87. Closes #76.
- Add `migrate --to VERSION` (Python: `migrate(to_version=...)`) to apply pending migrations only up to a target version, for staged rollouts. It fails if the version is unknown or below the highest applied one (`migrate` never rolls back, use `down`), is a no-op when equal to it, works with `--dry-run` and `--fake`, and cannot be combined with `--migrations`. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/89. Closes #81.
- Accept `db_url` / `--db-url` / `DB_URL` with the `clickhouse-connect` driver as well (it is how ClickHouse Cloud hands out credentials, and the natural single secret for the GitHub Action and Docker image). `clickhouse://` maps to `http://` (port 8123), `clickhouses://` to `https://` (port 8443), `http(s)://` passes through; `http(s)://` with `clickhouse-driver` is rejected with a clear error. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/90. Closes #80.
- Add `status --strict` (exit code 1 on `md5-mismatch` / `unknown` rows), `--exit-code-pending` (exit code 1 on `pending` rows), `--format json` for machine-readable output and a `has_down` column showing whether a `.down.sql` exists. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/88. Closes #77.
- Add a `dump` subcommand (Python: `ClickhouseCluster.dump()`) that prints every table, view, materialized view and dictionary of a database as portable, dependency-ordered SQL: no `UUID`, no database prefix, `Replicated*MergeTree` path/replica arguments removed (`--keep-replicated-paths` keeps them), the migrations and lock tables and `.inner` tables excluded. `--out FILE` writes it atomically, `--check FILE` exits 1 with a unified diff on drift for CI, `--tables` limits the objects. Read-only; works with both drivers. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/91. Part of #83.


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
