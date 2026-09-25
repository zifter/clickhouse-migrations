# Changelog

## [Unreleased]

**What's Changed:**
- Fix `--lock` on servers with asynchronous inserts on (the server default on ClickHouse 26.3 and newer): concurrent inserts of the lock row were batched into one and every run was told it took the lock, so concurrent runs were not serialised. The lock row is now always inserted with `async_insert = 0` (it overrides the server default, a user profile and `--setting`). Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/111.
- Fix `--lock` with `clickhouse-driver` on ClickHouse 23.8: a lock held by another run comes back as `Code: 999` without "Node exists" in the message and was reported as an error instead of being waited for; a conflict is now recognised by its error code as well. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/111.
- Document the supported ClickHouse versions (README, "Supported ClickHouse versions"): the minimum is **23.3**; `--lock` needs 23.8+, and `diff` changing the query of a materialized view (`MODIFY QUERY`) needs 24.3+. A new CI job runs the whole suite against every LTS release from 23.3 to 26.8 and against the newest release (26.9), and the dev cluster's server version is configurable with `CLICKHOUSE_VERSION` (default unchanged, 25.7.4). Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/111.

**Behaviour changes:**
- `--lock` now fails up front, before any migration runs, on servers without the `keeper_map_strict_mode` setting (ClickHouse older than 23.8, e.g. 23.3, which has `KeeperMap` but not the setting), with a message naming the requirement; it used to fail on the first lock insert with `Unknown setting keeper_map_strict_mode`. The documented requirement for `--lock` goes from 22.9+ to 23.8+. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/111.
- Drop Python 3.9 (EOL since October 2025); the last release supporting it is 0.15.x. pip on Python 3.9 keeps resolving 0.15.x because of `requires-python = ">=3.10, <4"`. The minimum-driver-versions CI job (`tox -e py310-mindeps`) now runs on Python 3.10 with the same pins (`clickhouse-driver==0.2.9`, `clickhouse-connect==0.7.9`). Closes #112.


## [v0.15.0](https://github.com/zifter/clickhouse-migrations/tree/v0.15.0) (2026-09-25)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.14.0...v0.15.0)

**What's Changed:**
- Refresh the migration lock while it is held: with `--lock`, a daemon heartbeat thread moves `acquired_at` forward every `--lock-ttl / 3` seconds (at least every second) over its own connection, so a run longer than `--lock-ttl` no longer looks stale and can no longer be taken over while it is still running; `--lock-ttl` now only matters for runs that died. The refresh is a compare-and-set on our own row (`ALTER TABLE … UPDATE … WHERE owner = <ours> SETTINGS keeper_map_strict_mode = 1`), a failed refresh is retried at the next tick, and a lock found gone or taken over is logged as an error (the migration continues). The heartbeat stops before the lock is released. Nothing changes without `--lock`. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/108. Closes #100.
- Publish releases to PyPI with [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (`pypa/gh-action-pypi-publish`, short-lived OIDC tokens) instead of a stored PyPI username and password. Done by @UsmanGhias in https://github.com/zifter/clickhouse-migrations/pull/105. Closes #103.
- Internal: split the CLI into a `cli` package (one module per subcommand plus shared option, rendering and dispatch modules); `clickhouse_migrations.command_line` keeps working: its public functions and constants are still importable from it, the `clickhouse-migrations` console script is unchanged and so is the command line itself. Code that patched `command_line` attributes in tests should patch the `cli` module that uses them (e.g. `clickhouse_migrations.cli.common.create_cluster`). Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/109. Closes #101.

**Behaviour changes:**
- Raise the minimum driver versions to what the code actually needs: `clickhouse-driver>=0.2.9` (was `>=0.2.2`; older versions have no `clickhouse_driver.util.helpers.parse_url` and fail on import) and `clickhouse-connect>=0.7.9` (was `>=0.7`; older versions break on a `--db-url` with client parameters in the query string such as `?connect_timeout=5`). Upgrading may make pip upgrade an older installed driver. A new CI job (`tox -e py39-mindeps`) runs the whole suite against these minimums. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/107. Closes #102.


## [v0.14.0](https://github.com/zifter/clickhouse-migrations/tree/v0.14.0) (2026-09-24)

[Full Changelog](https://github.com/zifter/clickhouse-migrations/compare/v0.13.0...v0.14.0)

**What's Changed:**
- Add a `new` subcommand that scaffolds the next migration file (`clickhouse-migrations new "add events"` → `migrations/004_add_events.sql`), with `--down` for the paired rollback file, `--version` to force a version and `--dir` to pick the directory. It is purely local and never connects to ClickHouse. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/86. Closes #74.
- Make the migrations table configurable: `--migrations-table` / `MIGRATIONS_TABLE` (accepts a `database.table` form) and `--migrations-table-engine` / `MIGRATIONS_TABLE_ENGINE` (a full engine clause, passed through verbatim, which wins over the `--cluster-name` default). This unblocks `Replicated` database engines and custom ZooKeeper layouts. Defaults are unchanged. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/87. Closes #76.
- Add `migrate --to VERSION` (Python: `migrate(to_version=...)`) to apply pending migrations only up to a target version, for staged rollouts. It fails if the version is unknown or below the highest applied one (`migrate` never rolls back, use `down`), is a no-op when equal to it, works with `--dry-run` and `--fake`, and cannot be combined with `--migrations`. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/89. Closes #81.
- Accept `db_url` / `--db-url` / `DB_URL` with the `clickhouse-connect` driver as well (it is how ClickHouse Cloud hands out credentials, and the natural single secret for the GitHub Action and Docker image). `clickhouse://` maps to `http://` (port 8123), `clickhouses://` to `https://` (port 8443), `http(s)://` passes through. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/90. Closes #80.
- Add an opt-in migration lock so concurrent runs cannot interleave (e.g. a Kubernetes `Job` with several replicas): with `--lock` / `LOCK=true`, `migrate` and `down` hold a per-database lock in a `KeeperMap` table (`<migrations table>_lock`), acquired with `keeper_map_strict_mode = 1` so only one run can win, released in a `finally` and only for its own `owner` (`<host>:<pid>:<uuid>`). `--lock-timeout` (`LOCK_TIMEOUT`, default 300s) waits for a lock held by another run, `--lock-ttl` (`LOCK_TTL`, default 3600s) takes over a stale one race-free, and the new `unlock` subcommand force-releases a lock left by a dead run; `status`, `--dry-run` and `new` never lock. Off by default: nothing changes for existing users, and a run without `--lock` does no lock-related work at all. `--lock` needs ClickHouse 22.9+ with Keeper and `<keeper_map_path_prefix>` configured, and fails with a clear message when they are missing. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/92. Closes #75, #38.
- Add `status --strict` (exit code 1 on `md5-mismatch` / `unknown` rows), `--exit-code-pending` (exit code 1 on `pending` rows), `--format json` for machine-readable output and a `has_down` column showing whether a `.down.sql` exists. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/88. Closes #77.
- Add a `dump` subcommand (Python: `ClickhouseCluster.dump()`) that prints every table, view, materialized view and dictionary of a database as portable, dependency-ordered SQL: no `UUID`, no database prefix, `Replicated*MergeTree` path/replica arguments removed (`--keep-replicated-paths` keeps them), the migrations and lock tables and `.inner` tables excluded. `--out FILE` writes it atomically, `--check FILE` exits 1 with a unified diff on drift for CI, `--tables` limits the objects. Read-only; works with both drivers. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/91. Part of #83.
- Add transport options for both drivers, with host/port arguments and with `--db-url`, on every subcommand that connects: `--ca-cert`, `--cert`, `--key` (custom CA / mutual TLS), `--verify` / `--no-verify` (logs a warning), `--connect-timeout`, `--query-timeout` and a repeatable `--setting name=value` that is sent with every statement the tool runs — the fix for a `SET` at the top of a multi-statement file not reaching the next statement over HTTP. Environment variables are `CLICKHOUSE_CA_CERT`, `CLICKHOUSE_CERT`, `CLICKHOUSE_KEY`, `CLICKHOUSE_VERIFY`, `CLICKHOUSE_CONNECT_TIMEOUT`, `CLICKHOUSE_QUERY_TIMEOUT` and `CLICKHOUSE_SETTINGS="a=1,b=2"`; in Python they are new `ClickhouseCluster` parameters (`ca_cert`, `cert`, `key`, `verify`, `connect_timeout`, `query_timeout`, `settings`). Extra `ClickhouseCluster` keyword arguments now also reach `clickhouse_driver.Client` with `db_url`. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/94. Closes #79.
- Add opt-in `${NAME}` substitution in migration and `.down.sql` files for per-environment SQL (cluster names, dictionary sources): `--var NAME=VALUE` (repeatable, `MIGRATION_VARS`) enables it on its own, `--substitute-env` (`SUBSTITUTE_ENV`) also uses the process environment, `--var` wins (Python: `variables=` / `substitute_env=` on `migrate` and `rollback`). Unset, malformed and unterminated placeholders fail before any migration SQL runs, `$${NAME}` is a literal `${NAME}`, and errors never print values. The md5 and the stored `script` stay the raw file text, so changing a variable does not re-run a migration; our logs show raw SQL, the server's `query_log` still sees substituted values. Without the flags files run byte for byte as before. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/95. Closes #78.
- Add `baseline` and `repair` subcommands (Python: `ClickhouseCluster.baseline()` / `.repair()`). `baseline --to VERSION` adopts an existing database: it records every local migration up to `VERSION` as applied (md5 and script, like `--fake`) without executing anything, refuses to run unless the migrations table is absent or empty, and `--dry-run` prints the resulting status and writes nothing. `repair` reports applied migrations whose file changed (`md5-mismatch`) or disappeared (`unknown`) and exits 1 if there are any; `--write` updates the stored md5 and script of exactly those rows (insert, then a synchronous `ALTER TABLE … DELETE … SETTINGS mutations_sync = 2` that waits for every replica), `--version N` (repeatable) narrows it, and `--prune --write` deletes `unknown` rows. Both print the `status` table (or `--format json`) and honour `--lock` for real writes. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/93. Closes #82.
- Add an offline `validate` subcommand (Python: `validate_migrations()`) that checks a migrations directory without any database. Errors (exit code 1): bad file names, duplicate versions, orphan `.down.sql`, empty files (only whitespace or comments), unterminated strings / quoted identifiers / block comments, non-UTF-8 files. Warnings (exit code 1 with `--strict`): version gaps, destructive statements (`DROP TABLE`, `TRUNCATE`, `ALTER … DELETE/UPDATE`, `DROP COLUMN`, …; never for `.down.sql`), missing `.down.sql` (`--require-down` makes it an error), a standalone `SET` in a multi-statement file, inconsistent `ON CLUSTER`. It reuses the `migrate` tokenizer, so strings and comments never trigger a check; `--format json` for machines. Ships a `.pre-commit-hooks.yaml` with the `clickhouse-migrations-validate` hook. `migrate` is unchanged. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/96. Closes #84.
- Add a `diff` subcommand (Python: `ClickhouseCluster.diff()` / `diff_plan()`) that compares the database with a target schema file (`dump` output or equivalent) and writes the next numbered migration for review, never applying anything: `clickhouse-migrations diff --to schema.sql --migrations-dir migrations/ [--name] [--allow-destructive] [--dry-run]`. The file is replayed into a throwaway `_chm_diff_<uuid>` database (always dropped; needs `CREATE`/`DROP DATABASE`) and both sides are compared through the system tables. Generates new tables/views/materialized views/dictionaries, `ADD`/`DROP`/`MODIFY COLUMN` (keeping positions), index, TTL and comment changes, `CREATE OR REPLACE` for views and dictionaries and `MODIFY QUERY` for materialized views; `DROP`s are commented out unless `--allow-destructive`; engine, `ORDER BY`/`PARTITION BY`/`PRIMARY KEY`/`SAMPLE BY`, settings and other rebuild-only changes are refused with an explanation (exit code 1). Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/97. Closes #83.
- Compute migration checksums with `hashlib.md5(..., usedforsecurity=False)` so the tool works on FIPS-mode Python builds, which refuse a plain `md5()`. The digest is unchanged, so checksums already stored in `schema_versions` stay valid. Done by @akalabyshau and @zifter in https://github.com/zifter/clickhouse-migrations/pull/98. Closes #73.
- Actually test Python 3.14 in CI (the 3.14 job used to run the suite on the runner's Python 3.12), run CI on every pull request, and refresh the README: a command overview, an updated comparison with other tools and Python examples for every subcommand. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/99.

**Behaviour changes:**
- `migrate` / `down` skip comment-only chunks (a commented-out statement, a note after the last `;`) instead of sending them to the server and failing with "Empty query". A file with nothing but comments now applies as a no-op (`validate` reports it as `empty-file`). Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/97.
- With `clickhouse-driver`, a `db_url` with an `http://` / `https://` scheme is now rejected with a clear error listing the accepted schemes; it used to be silently treated as a native connection. Use `clickhouse://` / `clickhouses://`, or `--driver clickhouse-connect`. Done by @zifter in https://github.com/zifter/clickhouse-migrations/pull/90.


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
