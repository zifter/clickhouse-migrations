[![ci](https://github.com/zifter/clickhouse-migrations/actions/workflows/ci.yaml/badge.svg)](https://github.com/zifter/clickhouse-migrations/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/zifter/clickhouse-migrations/branch/main/graph/badge.svg)](https://codecov.io/gh/zifter/clickhouse-migrations)
[![release](https://img.shields.io/github/release/zifter/clickhouse-migrations.svg)](https://github.com/zifter/clickhouse-migrations/releases)
[![PyPI version](https://badge.fury.io/py/clickhouse-migrations.svg)](https://pypi.org/project/clickhouse-migrations/)
[![supported versions](https://img.shields.io/pypi/pyversions/clickhouse-migrations.svg)](https://pypi.org/project/clickhouse-migrations/)
[![downloads](https://img.shields.io/pypi/dm/clickhouse-migrations.svg)](https://pypi.org/project/clickhouse-migrations/)
[![GitHub stars](https://img.shields.io/github/stars/zifter/clickhouse-migrations.svg)](https://github.com/zifter/clickhouse-migrations/stargazers)
[![my site](https://img.shields.io/badge/site-my%20blog-yellow.svg)](https://zifter.github.io/)

# ClickHouse Migrations

**Simple, file-based schema migrations for ClickHouse** — the most actively maintained ClickHouse migration tool for Python. Write plain `.sql` files, apply them from the CLI, your code, CI, or Docker. Cluster-aware, with support for both the native [`clickhouse-driver`](https://github.com/mymarilyn/clickhouse-driver) and the official [`clickhouse-connect`](https://github.com/ClickHouse/clickhouse-connect) driver.

## Quick start

```bash
pip install clickhouse-migrations

# create a migration file (./migrations/001_init.sql), then write your SQL into it:
clickhouse-migrations new "init" --dir ./migrations

# apply every pending migration:
clickhouse-migrations --db-host localhost --db-name mydb --migrations-dir ./migrations
```

📖 **Background:** [Managing ClickHouse migrations in production](https://medium.com/@zifter/managing-clickhouse-migrations-in-production-cluster-support-and-multi-statement-files-07d46c1de275) — why this tool exists, cluster support, and multi-statement migration files.

## Features
* **Multi-statement migrations** — more than one query per `.sql` file
* **Cluster-aware** — keeps migration state consistent across all cluster nodes
* **Zero-config file format** — `{VERSION}_{name}.sql`, applied in order
* **Run anywhere** — CLI, Python API, [GitHub Action](#in-ci-github-action), or [Docker image](#with-docker)
* **Two drivers** — native `clickhouse-driver` (TCP) or official `clickhouse-connect` (HTTP)
* **Inspect before you apply** — [`status`](#migration-status) and `--dry-run` show applied vs pending migrations without touching data
* **Scaffolding** — [`new`](#creating-a-migration) creates the next migration file for you, offline
* **Configurable bookkeeping** — [rename the migrations table](#the-migrations-table) or set its engine (`--migrations-table` / `--migrations-table-engine`)
* **Schema dump** — [`dump`](#dumping-the-schema) prints the live schema as normalised, dependency-ordered SQL, with `--check` for drift detection in CI
* **Safe concurrent runs** — an opt-in [migration lock](#concurrent-runs-and-locking) (`--lock`) so several replicas of a Kubernetes `Job` cannot interleave
* **Per-environment SQL** — opt-in [`${NAME}` substitution](#variable-substitution) (`--var` / `--substitute-env`) for cluster names, dictionary sources and the like, with checksums taken from the raw file
* **Naive rollbacks** — optional paired [`{VERSION}_{name}.down.sql`](#rollbacks-down-migrations) files and a `down` subcommand to reverse applied migrations

## Known alternatives
This package originally forked from [clickhouse-migrator](https://github.com/delium/clickhouse-migrator).

Package | Differences
-------|---------
[clickhouse-migrator](https://github.com/delium/clickhouse-migrator) | Doesn't support multistatement in a single file , to heavy because of pandas, looks like abandoned
[django-clickhouse](https://github.com/carrotquest/django-clickhouse) | Need django
[clickhouse-migrate](https://github.com/trushad0w/clickhouse-migrate) | Doesn't support multistatement

## Installation

You can install from pypi using `pip install clickhouse-migrations`.

By default it uses the native [`clickhouse-driver`](https://github.com/mymarilyn/clickhouse-driver) (TCP, port 9000). To use the official HTTP [`clickhouse-connect`](https://github.com/ClickHouse/clickhouse-connect) driver instead, install the extra and pass `--driver clickhouse-connect`:

```bash
pip install 'clickhouse-migrations[connect]'
```

With `clickhouse-connect` the default port is `8123` (HTTP). `--db-url` works with both drivers (see [URL schemes](#url-schemes)).

### URL schemes

`--db-url` / `DB_URL` accepts a single URL such as `https://user:pass@host:8443/db`, which is how ClickHouse Cloud hands out credentials. The URL wins over `--db-host`/`--db-port`/`--db-user`/`--db-password`. The scheme is normalised per driver:

Scheme | `clickhouse-driver` | `clickhouse-connect`
--- | --- | ---
`clickhouse://` | native TCP, default port 9000 | mapped to `http://`, default port **8123**
`clickhouses://` | native TCP over TLS, default port 9440 | mapped to `https://`, default port **8443**
`http://`, `https://` | rejected | used as is

Note that with `clickhouse-connect` the `clickhouse://` mapping changes the port from 9000 to 8123: an explicit port in the URL is always kept, so `clickhouse://host:9000` would talk HTTP to port 9000. The resolved scheme, host and port (never the password) are logged at `INFO` level. `--secure` upgrades `http`/`clickhouse` URLs to TLS and never downgrades `https://`/`clickhouses://` ones.

### Transport and TLS

These options work the same with both drivers, with `--db-host`/`--db-port` and with `--db-url`, on every subcommand that connects (`migrate`, `status`, `down`, `dump`, `unlock`). Their variables carry a `CLICKHOUSE_` prefix because names like `KEY` or `SETTINGS` are too likely to be set for something else.

CLI flag | Environment variable | `clickhouse-driver` parameter | `clickhouse-connect` parameter
--- | --- | --- | ---
`--ca-cert PATH` | `CLICKHOUSE_CA_CERT` | `ca_certs` | `ca_cert`
`--cert PATH` | `CLICKHOUSE_CERT` | `certfile` | `client_cert`
`--key PATH` | `CLICKHOUSE_KEY` | `keyfile` | `client_cert_key`
`--verify` / `--no-verify` (default on) | `CLICKHOUSE_VERIFY` | `verify` | `verify`
`--connect-timeout SECONDS` | `CLICKHOUSE_CONNECT_TIMEOUT` | `connect_timeout` | `connect_timeout`
`--query-timeout SECONDS` | `CLICKHOUSE_QUERY_TIMEOUT` | `send_receive_timeout` | `send_receive_timeout`
`--setting NAME=VALUE` (repeatable) | `CLICKHOUSE_SETTINGS="a=1,b=2"` | `settings` | `settings`

```bash
clickhouse-migrations migrate --db-url "clickhouses://user:pass@ch.internal:9440/app" \
  --ca-cert /tls/ca.pem --cert /tls/client.pem --key /tls/client.key \
  --query-timeout 1800 --setting allow_experimental_json_type=1
```

- TLS itself is still turned on by `--secure` or a `clickhouses://`/`https://` URL; the certificate options only configure it, and a warning is logged when they are given for a plain connection. `--key` needs `--cert`. `--no-verify` disables certificate (and host name) verification and logs a warning.
- Unset options keep the driver defaults (both: 10 s to connect, 300 s query timeout). `--query-timeout` is the socket read timeout: over HTTP it bounds the wait for a statement's response; over the native protocol it bounds the silence between two packets, and the server sends progress packets while a query runs, so there it is an inactivity timeout rather than a limit on the total duration. Raise it for long `ALTER ... MATERIALIZE` or `CREATE TABLE ... AS SELECT` migrations.
- `--setting` values go to the client itself, so they reach **every** statement the tool sends: the migrations, the bookkeeping queries, the lock and `status`/`dump` queries. Values are passed to the server as text and it converts them. An unknown setting fails the run on both drivers (for `clickhouse-driver` the settings are sent as "important"). `--setting` flags win over `CLICKHOUSE_SETTINGS`, which cannot hold a value containing a comma (use the flag). An explicit option also wins over the same parameter in the `--db-url` query string.

**`SET` inside a multi-statement file.** Over the native protocol a file's statements share one session, so `SET x = 1;` at the top applies to the rest of the file. Over HTTP (`clickhouse-connect`) every statement is a separate request: the `SET` only carries over while the HTTP session does, and sessions live on one server, so behind a load balancer or a multi-replica endpoint (ClickHouse Cloud) statement 2 fails with no hint why. Pass the setting with `--setting` (or `settings=` in Python) instead.

## Migration files

Migration files follow the naming convention `{VERSION}_{name}.sql`, e.g. `001_init.sql`, `002_add_users.sql`. Versions are plain integers applied in ascending order; [`new`](#creating-a-migration) picks the next one for you.

Each file contains one or more SQL statements separated by semicolons:

```sql
-- migrations/001_init.sql
CREATE TABLE mydb.events (
    id     UInt32,
    name   String
) ENGINE = MergeTree()
ORDER BY id;

ALTER TABLE mydb.events ADD COLUMN created_at DateTime DEFAULT now();
```

Optionally, add a paired rollback file `{VERSION}_{name}.down.sql` next to a migration
(e.g. `001_init.down.sql`) to make it reversible — see [Rollbacks](#rollbacks-down-migrations).
`clickhouse-migrations new "<name>" --down` creates both files at once.

## Usage

### In command line
```bash
clickhouse-migrations --db-host localhost \
    --db-port 9000 \
    --db-user default \
    --db-password secret \
    --db-name test \
    --migrations-dir ./migrations
```

Alternatively, connect via URL:
```bash
clickhouse-migrations --db-url clickhouse://default:secret@localhost:9000/test \
    --migrations-dir ./migrations
```

All options can also be set via environment variables:

CLI flag | Environment variable | Default
---------|---------------------|--------
`--db-host` | `DB_HOST` | `localhost`
`--db-port` | `DB_PORT` | `9000`
`--db-user` | `DB_USER` | `default`
`--db-password` | `DB_PASSWORD` | *(empty)*
`--db-name` | `DB_NAME` | —
`--db-url` | `DB_URL` | —
`--migrations-dir` | `MIGRATIONS_DIR` | `./migrations`
`--cluster-name` | `CLUSTER_NAME` | —
`--migrations-table` | `MIGRATIONS_TABLE` | `schema_versions`
`--migrations-table-engine` | `MIGRATIONS_TABLE_ENGINE` | —
`--multi-statement` | `MULTI_STATEMENT` | `true`
`--create-db-if-not-exists` | `CREATE_DB_IF_NOT_EXISTS` | `true`
`--dry-run` | `DRY_RUN` | `false`
`--fake` | `FAKE` | `false`
`--to` | — | —
`--lock` / `--no-lock` | `LOCK` | `false`
`--lock-timeout` | `LOCK_TIMEOUT` | `300`
`--lock-ttl` | `LOCK_TTL` | `3600`
`--var NAME=VALUE` (repeatable, `migrate` / `down`) | `MIGRATION_VARS` (`A=1,B=2`) | —
`--substitute-env` / `--no-substitute-env` (`migrate` / `down`) | `SUBSTITUTE_ENV` | `false`
`--secure` | `SECURE` | `false`
`--ca-cert`, `--cert`, `--key`, `--verify`, `--connect-timeout`, `--query-timeout`, `--setting` | `CLICKHOUSE_*` | see [Transport and TLS](#transport-and-tls)
`--log-level` | `LOG_LEVEL` | `WARNING`
`--migration-log-format` | `MIGRATION_LOG_FORMAT` | `full`
`--driver` | `DRIVER` | `clickhouse-driver`

### Migrating up to a version

By default `migrate` applies every pending migration. Pass `--to VERSION` to stop at a target version, e.g. for a staged rollout or to reproduce a bug at a specific production version:

```bash
clickhouse-migrations migrate --to 2 ...   # apply pending migrations with version <= 2, then stop
clickhouse-migrations migrate --to 3 ...   # later: continue up to 3
clickhouse-migrations migrate ...          # finally: everything that is left
```

Python: `cluster.migrate(db_name="test", migration_path="./migrations", to_version=2)`.

* The version must exist among the local migrations, otherwise the run fails.
* If `VERSION` equals the highest applied version there is nothing to do and the run succeeds. If it is *below* it, the run fails and points you at `down` - `migrate` never rolls anything back.
* The md5, missing-migration and unknown-migration checks still cover the whole local set, so a problem above the target is still reported.
* Works with `--dry-run` and `--fake`; cannot be combined with `--migrations`. `--to` has no environment variable, and is unrelated to `down --to`.

### Creating a migration

Create the next migration file with the `new` subcommand instead of counting file names by eye:

```bash
clickhouse-migrations new "add events"
# Created migrations/004_add_events.sql

clickhouse-migrations new "add events" --down
# Created migrations/004_add_events.sql
# Created migrations/004_add_events.down.sql
```

The version is the highest existing one plus one, zero-padded to the width of the widest existing file (`003` when the directory is empty), and the name is slugified (lowercase, non-alphanumerics collapsed into `_`). Each file gets a two-line header comment and nothing else.

```bash
clickhouse-migrations new "add events" --dir ./db/migrations   # defaults to --migrations-dir / MIGRATIONS_DIR
clickhouse-migrations new "add events" --version 42            # force a version; fails if it is taken
```

CLI flag | Environment variable | Default
---------|---------------------|--------
`--dir` (alias `--migrations-dir`) | `MIGRATIONS_DIR` | `./migrations`
`--down` | — | `false`
`--version` | — | *(next available)*

This subcommand is purely local: it **never connects to ClickHouse** and therefore takes none of the `--db-*` options. The migrations directory is created if it does not exist. A version is considered taken if either the migration or its `.down.sql` file already uses it.

### Migration status

Show which migrations are applied vs pending, without applying anything, using the `status` subcommand:

```bash
clickhouse-migrations status --db-name test --migrations-dir ./migrations
```

```
VERSION  STATUS   MD5                               APPLIED AT           HAS DOWN
1        applied  6172991b15b0852bc895e09b3e91ade4  2024-01-01 12:00:00  yes
2        pending  1a79a4d60de6718e8e5b326e338ae533                       no
```

States: `applied`, `pending`, `md5-mismatch` (a file changed after being applied), and `unknown` (applied but no longer present locally). `HAS DOWN` shows whether a paired `{VERSION}_{name}.down.sql` exists locally (always `no` for `unknown`). It is read-only and never creates the database.

Options specific to `status`:

Option | Env variable | Default
-------|--------------|--------
`--strict` | `STRICT` | `false`
`--exit-code-pending` | `EXIT_CODE_PENDING` | `false`
`--format {table,json}` | `STATUS_FORMAT` | `table`

* `--strict` exits with code `1` if any migration is `md5-mismatch` or `unknown` (an applied migration was edited or deleted, so the next deploy would fail). `pending` is not a failure.
* `--exit-code-pending` exits with code `1` if any migration is `pending`, e.g. as a post-deploy smoke-test gate. It composes with `--strict`: either condition gives exit code `1`.
* Without these flags the exit code is `0`, as before.
* `--format json` prints only a JSON document to stdout (logs go to stderr), with ISO-8601 timestamps and `null` for unapplied migrations:

```json
{
  "database": "test",
  "migrations": [
    {"version": 1, "state": "applied", "md5": "6172991b15b0852bc895e09b3e91ade4", "applied_at": "2024-01-01T12:00:00", "has_down": true},
    {"version": 2, "state": "pending", "md5": "1a79a4d60de6718e8e5b326e338ae533", "applied_at": null, "has_down": false}
  ]
}
```

CI example: fail the job on drift and list the offending versions:

```bash
clickhouse-migrations status --strict --format json | jq -r '.migrations[] | select(.state != "applied" and .state != "pending") | "\(.version) \(.state)"'
```

### Rollbacks (down migrations)

Rollbacks are **explicit and hand-written**. For any migration you want to be reversible, add a paired file `{VERSION}_{name}.down.sql` next to it:

```sql
-- migrations/001_init.sql
CREATE TABLE mydb.events (id UInt32, name String) ENGINE = MergeTree() ORDER BY id;

-- migrations/001_init.down.sql
DROP TABLE mydb.events;
```

Roll back with the `down` subcommand. By default it reverses the single most recent applied migration:

```bash
clickhouse-migrations down --db-name test --migrations-dir ./migrations
clickhouse-migrations down --steps 3 ...        # the 3 most recent, newest first
clickhouse-migrations down --to 5 ...           # everything with a version > 5
clickhouse-migrations down --dry-run ...        # print what would run, change nothing
```

For each migration in range (newest first) it runs the statements from the `.down.sql` file and then removes the row from `schema_versions`, so `status` reports the migration as `pending` again. If a `.down.sql` file is missing for any migration in the range, `down` fails without changing anything.

> **This is deliberately naive.** ClickHouse has no transactional DDL, so there is no *automatic* rollback and no all-or-nothing guarantee across statements. Reversible changes (`CREATE TABLE` ↔ `DROP TABLE`, `ADD COLUMN` ↔ `DROP COLUMN`) roll back cleanly; **destructive** operations (data-losing drops, `ALTER … DELETE/UPDATE` mutations) are your responsibility — nothing can bring dropped data back. For a *failed* migration you usually don't need `down` at all: a migration is recorded only after its statements succeed, so a failed one stays `pending` — just fix the SQL and re-run.

`--steps` (default `1`), `--to`, `--dry-run` and `--multi-statement` apply to the `down` subcommand.

### Variable substitution

Some SQL cannot be committed literally because it differs per environment: the cluster name of
`ON CLUSTER`, the host and credentials of a dictionary source, … For these, migration files
(and `.down.sql` files) may contain `${NAME}` placeholders:

```sql
-- migrations/003_events.sql
CREATE TABLE events ON CLUSTER ${CLUSTER_NAME} (id UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}')
ORDER BY id;

CREATE DICTIONARY users_dict ON CLUSTER ${CLUSTER_NAME} (id UInt64, name String)
PRIMARY KEY id
SOURCE(POSTGRESQL(HOST '${PG_HOST}' PORT 5432 USER 'reader' PASSWORD '${PG_PASSWORD}' DB 'app' TABLE 'users'))
LAYOUT(HASHED()) LIFETIME(300);
```

Substitution is **off by default** — without the options below files are executed byte for byte,
so existing migrations that happen to contain `${...}` text keep working. It is enabled by:

* `--var NAME=VALUE` (repeatable; env `MIGRATION_VARS=A=1,B=2`, ignored when `--var` is given) —
  on its own it substitutes **only** the given variables;
* `--substitute-env` (env `SUBSTITUTE_ENV=true`) — additionally takes values from the process
  environment. `--var` wins over the environment.

```bash
clickhouse-migrations migrate --db-name test --cluster-name company_cluster \
    --var CLUSTER_NAME=company_cluster --substitute-env   # PG_HOST, PG_PASSWORD from the env
clickhouse-migrations down --var CLUSTER_NAME=company_cluster ...
```

In Python: `cluster.migrate(..., variables={"CLUSTER_NAME": "company_cluster"}, substitute_env=False)`
(the same two parameters exist on `rollback`). Only `migrate` and `down` substitute; `status` and
`dump` never read variables.

Rules:

* `${NAME}` is replaced with the value of `NAME`; a valid name matches `[A-Za-z_][A-Za-z0-9_]*`.
* An **unset** `NAME` fails the run. Every pending migration (or every down script in range) is
  substituted before the first one executes, so an unset variable fails before any migration SQL runs.
* `$${NAME}` is an escape and becomes a literal `${NAME}`. Any other `$` is left alone.
* A malformed (`${PG-HOST}`, `${}`) or unterminated (`${PG_HOST` — no `}` on the same line)
  placeholder fails too. Errors name the file, the line and the placeholder, never a value.
* `--fake` executes nothing, so it substitutes nothing and needs no variables.
* `--dry-run` substitutes (so it fails on unset variables exactly like a real run) but logs the
  statements **with their placeholders**, not the substituted SQL.

> **Quoting is your responsibility.** Substitution is textual (`envsubst`-style): SQL is not parsed
> and values are not escaped. A value inside `'...'` must not contain `'` or `\`; a value used as
> an identifier must be a valid identifier. A value can even add statements (`1; DROP ...`), so
> only pass values you trust.

**The checksum is taken from the raw file, before substitution**, and `schema_versions.script`
stores the raw text too. A migration therefore has the same md5 in every environment and values
(possibly secrets) never land in `schema_versions`. The consequences:

* an applied migration is **not re-run when a variable changes** — ship a new migration instead;
* changing a variable never makes `status` report `md5-mismatch`.

**Where values can still show up.** Our own log messages never contain substituted values (the
`full` migration log format shows the raw script, statements are logged raw). But the substituted
SQL is what the server receives, so:

* it is recorded by the server, e.g. in `system.query_log`, and may appear in server-side error
  messages (a failing statement's error text can quote it);
* with `--log-level DEBUG` the driver's own debug log (`clickhouse-driver` logs every query it
  sends) prints it — the CLI warns when DEBUG is combined with substitution.

Keep that in mind before putting passwords into variables; for dictionary sources consider
[named collections](https://clickhouse.com/docs/en/operations/named-collections) instead.

### Dumping the schema

`dump` prints the definition of every table, view, materialized view and dictionary of a database as portable, diffable SQL. It is strictly **read-only** (it never creates a database or a table) and works with both drivers and `--db-url`.

```bash
clickhouse-migrations dump --db-name test > schema.sql             # stdout carries only the SQL
clickhouse-migrations dump --db-name test --out schema.sql         # atomic write, short confirmation on stderr
clickhouse-migrations dump --db-name test --check schema.sql       # exit 1 + unified diff on drift (for CI)
clickhouse-migrations dump --db-name test --tables events v_events # only these objects
```

Statements come in **dependency order** (a view, materialized view or dictionary always after the tables it reads from or writes to, based on the server's dependency columns plus the references found in the definitions; ties are broken by name, so the output is deterministic) and each one ends with `;`, separated by a blank line, with a trailing newline. A dependency cycle fails with a message naming it. The file replays into an empty database: `clickhouse-client --database other_db --multiquery < schema.sql`.

| Flag | Env | Meaning |
| --- | --- | --- |
| `--db-url`, `--db-host`, `--db-port`, `--db-user`, `--db-password`, `--db-name`, `--driver`, `--secure`, `--log-level` | same as the other subcommands | Connection (the migrate-only flags such as `--dry-run` or `--migrations-dir` are not accepted) |
| `--migrations-table` | `MIGRATIONS_TABLE` | Bookkeeping table to exclude (default `schema_versions`) |
| `--tables NAME [NAME ...]` | `DUMP_TABLES` (comma separated) | Dump only these objects. Dependencies are **not** pulled in: a warning on stderr names each listed object that depends on an unlisted one. An unknown or excluded name is an error |
| `--keep-replicated-paths` | `KEEP_REPLICATED_PATHS` | Keep the ZooKeeper path and replica arguments of `Replicated*MergeTree` |
| `--include-migrations-table` | `INCLUDE_MIGRATIONS_TABLE` | Also dump the migrations table and the lock tables |
| `--out FILE` | | Write to FILE (temp file + rename) instead of stdout |
| `--check FILE` | | Compare with FILE instead of printing |

`--out` and `--check` are mutually exclusive. **Exit codes:** `0` success (with `--check`: no drift); `1` drift with `--check` (unified diff, file to database, on stderr), or any error (missing database, unreadable `--check` file, dependency cycle, unknown `--tables` name, ...); `2` invalid arguments. Only whitespace at line ends and line endings are ignored when comparing. Logs, warnings and diffs go to stderr, so stdout is only ever the SQL.

**What is normalised** (token based, never a blind text replace: string literals, comments and quoted identifiers are recognised):

* `UUID '...'` (and `TO INNER UUID '...'`) is removed.
* The database qualifier is removed from references to objects **of the dumped database**: `CREATE TABLE db.events` becomes `CREATE TABLE events`, and so do `db.events` inside `AS SELECT` bodies, `TO db.totals` of a materialized view, and `db`.`events` written with quotes. Only `db.name` where `name` is an existing object of that database is rewritten; other databases, `db.name(...)` calls, longer paths such as `x.db.name` and everything inside string literals or comments are left alone.
* `Replicated*MergeTree('<zookeeper path>', '<replica>', ...)` (and `Shared*MergeTree`) loses its first two arguments unless `--keep-replicated-paths`; the remaining engine arguments stay (`ReplicatedReplacingMergeTree('/p', '{replica}', ver)` becomes `ReplicatedReplacingMergeTree(ver)`). The server fills in `default_replica_path`/`default_replica_name` (with `{shard}`/`{replica}` macros) when such a table is created, so the definition is portable across clusters. The table's own path (for the default it contains `{uuid}`) is not part of the dump; replaying an argument-less Replicated table needs `ON CLUSTER` or a `Replicated` database, exactly as if you had written it yourself. With `--keep-replicated-paths` the arguments are kept as the server shows them (with macros like `{shard}`, `{replica}` and `{uuid}` unexpanded).
* The database argument of `Distributed('cluster', 'db', 'table')`, `Merge('db', ...)` and `Buffer('db', ...)`, when it names the dumped database, becomes `currentDatabase()` (the server evaluates it when the table is created, so replaying into another database points at that database).
* `DB '<dumped db>'` is dropped from a *local* `SOURCE(CLICKHOUSE(... TABLE '...'))` dictionary source (one without `HOST`/`PORT`), which then reads from the dictionary's own database.
* Trailing whitespace is removed. The text is otherwise what `SHOW CREATE TABLE` returns, so it is as stable as the server's own formatting.

**Excluded:** the migrations table (`--migrations-table`), the lock tables (`schema_lock` and `<migrations table>_lock`) unless `--include-migrations-table`; materialized view storage (`.inner.*` / `.inner_id.*`, the view's own `CREATE` covers it) and temporary tables.

**Known limitations**

* Replay expects the target database to be the connection's default database (`--database` / `USE`): after normalisation references have no database prefix.
* Database names inside string literals are never rewritten: `dictGet('db.dict', ...)`, `SOURCE(CLICKHOUSE(HOST ... DB 'db'))` (remote source), `Distributed` arguments given as anything but a plain string, dictionary `QUERY '...'` text, column comments. Such definitions still point at the original database after a replay into another one.
* Secrets are masked by the server in `SHOW CREATE` (`[HIDDEN]`), so dictionaries or engines with passwords / keys will not replay as is.
* The output follows the server's formatting, which differs between ClickHouse versions: compare dumps taken from the same server version (`--check` in CI against a fixed version). Tested on ClickHouse 25.7.
* Only tables, views, materialized views and dictionaries are dumped (no users, roles, grants, functions or databases). Dependencies are per-object, so a dependency on an object of another database is not followed.
* `diff` (turning a schema file into migrations) is not part of this command.

Drift detection in CI (fails the job when the live schema no longer matches the committed `schema.sql`):

```yaml
name: schema-drift
on: [pull_request]
jobs:
  schema:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: pip install clickhouse-migrations
      - run: clickhouse-migrations dump --db-name mydb --check schema.sql
        env:
          DB_HOST: ${{ secrets.CLICKHOUSE_HOST }}
          DB_PASSWORD: ${{ secrets.CLICKHOUSE_PASSWORD }}
```

From Python: `ClickhouseCluster(...).dump(db_name="mydb", tables=None, keep_replicated_paths=False, include_migrations_table=False)` returns the SQL text.

### In code
```python
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster

cluster = ClickhouseCluster(
    db_host="localhost",
    db_port=9000,
    db_user="default",
    db_password="secret",
)
cluster.migrate(
    db_name="test",
    migration_path="./migrations",
    cluster_name=None,
    create_db_if_no_exists=True,
    multi_statement=True,
    dryrun=False,
    fake=False,
    migration_log_format="full",
)
```

Alternatively, connect via URL:
```python
cluster = ClickhouseCluster(db_url="clickhouse://default:secret@localhost:9000/test")
cluster.migrate(db_name="test", migration_path="./migrations")
```

Parameter | Description | Default
-------|-------------|--------
`db_host` | ClickHouse database hostname | `localhost`
`db_port` | ClickHouse database port | `9000`
`db_user` | ClickHouse user | `default`
`db_password` | ClickHouse password | *(empty)*
`db_url` | ClickHouse connection URL (alternative to individual params) | —
`db_name` | ClickHouse database name | —
`migration_path` | Path to directory with migration files | `./migrations`
`explicit_migrations` | Explicit list of migrations to apply | `[]`
`cluster_name` | Name of ClickHouse topology cluster from `<remote_servers>` | —
`migrations_table` | Table recording applied migrations; accepts `database.table` | `schema_versions`
`migrations_table_engine` | Full engine clause for that table, used verbatim | —
`create_db_if_no_exists` | Create the database if it does not exist | `True`
`multi_statement` | Allow multiple statements per migration file | `True`
`dryrun` | Print migrations without executing them | `False`
`fake` | Mark migrations as applied without executing SQL | `False`
`to_version` | Apply pending migrations only up to and including this version; mutually exclusive with `explicit_migrations` | `None`
`lock` | Take the [migration lock](#concurrent-runs-and-locking) for the run (opt-in; fails if the server cannot provide it) | `False`
`lock_timeout` | Seconds to wait for a lock held by another run (`0` fails immediately) | `300`
`lock_ttl` | Seconds after which a lock is considered stale and may be taken over | `3600`
`variables` | `{"NAME": "value"}` for [`${NAME}` substitution](#variable-substitution); enables it on its own (also on `rollback`) | `None`
`substitute_env` | Also substitute from the process environment; `variables` win (also on `rollback`) | `False`
`secure` | Use secure (TLS) connection | `False`
`ca_cert`, `cert`, `key`, `verify`, `connect_timeout`, `query_timeout`, `settings` | Constructor parameters of `ClickhouseCluster`, same meaning as the [transport options](#transport-and-tls) (`settings` is a dict); `None` keeps the driver default. Other keyword arguments still go to `clickhouse_driver.Client` as is (now also with `db_url`), and an explicit parameter wins over a keyword argument for the same driver parameter | `None`
`migration_log_format` | Migration log format `full` logs the full Migration object, `compact` logs only version and md5 | `full`

### The migrations table

Applied migrations are recorded in a bookkeeping table, by default `schema_versions` in the migrated database, with `ENGINE = MergeTree` (or `ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}')` when `--cluster-name` is set).

Both the name and the engine are configurable:

```bash
# rename it, or keep it in a dedicated database (that database is NOT created for you)
clickhouse-migrations --migrations-table meta.my_versions ...

# take full control of the engine clause, e.g. a different ZooKeeper layout
clickhouse-migrations --cluster-name company_cluster \
    --migrations-table-engine "ReplicatedMergeTree('/ch/{shard}/tables/{database}/{table}', '{replica}')" ...
```

CLI flag | Environment variable | Default
---------|---------------------|--------
`--migrations-table` | `MIGRATIONS_TABLE` | `schema_versions`
`--migrations-table-engine` | `MIGRATIONS_TABLE_ENGINE` | *(MergeTree / ReplicatedMergeTree)*

`--migrations-table` accepts a `database.table` form so the table can live outside the migrated database; a bare name means the migrated database. Both parts are quoted, so names with dots work when you quote them yourself (`"my.db".versions`). The database is **never created implicitly** — create it first, otherwise the run fails with an explicit error.

`--migrations-table-engine` is a **full engine clause** passed to the `CREATE TABLE` verbatim, with no validation, and it wins over the engine derived from `--cluster-name`. `{database}`, `{table}`, `{shard}` and `{replica}` in it are ClickHouse macros, expanded by the server.

> **`Replicated` database engine caveat:** a database created with `ENGINE = Replicated(...)` injects its own ZooKeeper path and replica arguments into every `ReplicatedMergeTree` table, and conflicts with an explicit path. There, set `--migrations-table-engine "ReplicatedMergeTree"` (no arguments) and leave `--cluster-name` unset — the database engine replicates the DDL itself.

### Concurrent runs and locking

ClickHouse has no transactional DDL, so two migration runs started at the same time (say a Kubernetes `Job` with several replicas, or CI and a deploy hook racing each other) both read `schema_versions`, both compute the same pending list and both execute it — interleaving statements and writing duplicate bookkeeping rows.

**Without `--lock`, concurrent runs are unsafe — run migrations from one place at a time.** Pass `--lock` (or `LOCK=true`) and `migrate` / `down` take a **lock per migrated database** for the duration of the run:

```bash
clickhouse-migrations --lock --db-name mydb --migrations-dir ./migrations
```

```sql
CREATE TABLE IF NOT EXISTS schema_versions_lock (name String, owner String, acquired_at DateTime)
ENGINE = KeeperMap('/clickhouse-migrations/<database>') PRIMARY KEY name
```

* The lock is a single row inserted with `keeper_map_strict_mode = 1`. `KeeperMap` is backed by Keeper/ZooKeeper and that setting turns the insert into a compare-and-set, so the second run **fails** instead of overwriting the row.
* `owner` is `<hostname>:<pid>:<uuid>`, so the error message names the run that is holding the lock:
  `Could not take the migration lock on "mydb"."schema_versions_lock" within 300s: it is held by migrator-abc:1:…, which has held it for 42s.`
* The lock is released in a `finally`, deleting **only** rows whose `owner` matches — a run never drops somebody else's lock, even after a failure or a `Ctrl-C`.
* A lock older than `--lock-ttl` is stale and is taken over with a warning. The takeover is a compare-and-delete on `(owner, acquired_at)` followed by the normal strict insert, so of two runs seeing the same stale lock only one can win.
* The lock table (`<migrations table>_lock`, next to the bookkeeping table) is created on demand, only when `--lock` is used. A run without `--lock` does no lock-related work at all and is not blocked by a lock somebody else holds.
* `status` is read-only and never locks, `--dry-run` never locks, and `new` never touches the database at all.

CLI flag | Environment variable | Default | Meaning
---------|---------------------|---------|--------
`--lock` / `--no-lock` | `LOCK` | `false` | Take the migration lock for this run; the run **fails** if the server cannot provide it (see below)
`--lock-timeout` | `LOCK_TIMEOUT` | `300` | Seconds to wait for a lock held by another run; `0` fails immediately
`--lock-ttl` | `LOCK_TTL` | `3600` | Seconds after which a lock counts as stale and may be taken over

#### If a run dies while holding the lock

A pod that is OOM-killed mid-migration leaves the row behind. Either wait for `--lock-ttl` to expire, or force-release it:

```bash
clickhouse-migrations unlock --db-name mydb
# Released the migration lock held by migrator-abc:1:… for 42s.
```

`unlock` never rolls anything back — check what the dead run managed to apply with `clickhouse-migrations status` first.

#### Server requirements

`KeeperMap` needs ClickHouse 22.9+, a Keeper/ZooKeeper ensemble **and** `<keeper_map_path_prefix>` in the server configuration:

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

Without it the engine is disabled, and a run started with `--lock` fails with an explicit message instead of silently migrating unprotected. Drop `--lock` to run as before, knowing that concurrent runs are then unsafe.

### In CI (GitHub Action)

Apply migrations from a GitHub workflow with the composite action:

```yaml
- uses: zifter/clickhouse-migrations@v1
  with:
    migrations-dir: ./migrations
    db-host: localhost
    db-user: default
    db-password: ${{ secrets.CLICKHOUSE_PASSWORD }}
    db-name: mydb
    # driver: clickhouse-connect   # optional; official HTTP driver (both are bundled). Defaults to native clickhouse-driver.
    # db-port: "9000"              # optional; defaults to 9000 (clickhouse-driver) / 8123 (clickhouse-connect)
    # or connect via a single URL instead of the db-* inputs (works with both drivers):
    # db-url: ${{ secrets.CLICKHOUSE_URL }}
    # any extra raw CLI flags:
    # extra-args: --secure --create-db-if-not-exists
```

Both drivers are bundled, so `driver: clickhouse-connect` works without extra setup. Inputs: `migrations-dir`, `db-url`, `db-host`, `db-port`, `db-user`, `db-password`, `db-name`, `cluster-name`, `driver`, `extra-args`, `version` (pin the package version), `python-version`. You can also pin an exact release, e.g. `zifter/clickhouse-migrations@v0.12.0`.

### With Docker

An image is published to the GitHub Container Registry. Mount your migrations directory at `/migrations`:

```bash
docker run --rm \
    -v "$PWD/migrations:/migrations" \
    ghcr.io/zifter/clickhouse-migrations:latest \
    --db-url clickhouse://default:secret@clickhouse:9000/mydb
```

The image bundles **both drivers**. It uses the native `clickhouse-driver` by default; to use the official HTTP `clickhouse-connect` driver, pass `--driver clickhouse-connect` (default port `8123`; `--db-url` works with both drivers, see [URL schemes](#url-schemes)):

```bash
docker run --rm \
    -v "$PWD/migrations:/migrations" \
    ghcr.io/zifter/clickhouse-migrations:latest \
    --driver clickhouse-connect --db-host clickhouse --db-name mydb
```

Run migrations as a Kubernetes `Job`, e.g. before rolling out a deployment:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: clickhouse-migrations
spec:
  backoffLimit: 3
  # Retries and several replicas are safe only with the migration lock enabled
  # below, see "Concurrent runs and locking".
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrations
          image: ghcr.io/zifter/clickhouse-migrations:latest
          args: ["--create-db-if-not-exists"]
          env:
            - name: DB_URL
              valueFrom:
                secretKeyRef:
                  name: clickhouse
                  key: url
            # Serialise concurrent replicas/retries on the migration lock
            # (needs Keeper + <keeper_map_path_prefix> on the server).
            - name: LOCK
              value: "true"
            # Wait up to 10 minutes for a migration started by another replica.
            - name: LOCK_TIMEOUT
              value: "600"
          volumeMounts:
            - name: migrations
              mountPath: /migrations
      volumes:
        - name: migrations
          configMap:
            name: clickhouse-migrations
```

With `LOCK: "true"` a retried or parallel `Job` replica waits for the running one and then finds nothing left to apply. Without the lock (the default, or a server without Keeper) keep the `Job` to a single replica at a time — concurrent runs can interleave.

Migrations are provided here via a ConfigMap; alternatively bake them into your own image with `FROM ghcr.io/zifter/clickhouse-migrations`.

### Notes
The ClickHouse driver does not natively support executing multiple statements in a single query.
To allow for multiple statements in a single migration, you can use the `multi_statement` param.
This mode splits the migration text into separately-executed statements on the semicolon `;`. Semicolons inside string literals (`'...'`), quoted identifiers (`` `...` `` and `"..."`) and SQL comments (`-- ...` and `/* ... */`) are recognised and do not split a statement.

One important caveat:
* The queries are not executed in any sort of transaction/batch, meaning you are responsible for fixing partial migrations.

## Star History

<a href="https://www.star-history.com/?type=date&repos=zifter%2Fclickhouse-migrations">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=zifter/clickhouse-migrations&type=date&theme=dark&legend=top-left&sealed_token=Lg0-ckR6MostLY6zlv6N28heM0N_Ydc4NL-tKY3aExJD1B-Old0XKK2DhXNUgDM05YqAsK6FflkMH2hf4_AFsLMbFkXxxe4bU6P5yG1gmWVRVNI2jCchOnC4ftJLi2Zd9XVoBtYY3n0rpkCsfB-WoKVZaJW2GKgt-jygoj5XjoTHMxZWpXdNJ1QPaZu4" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=zifter/clickhouse-migrations&type=date&legend=top-left&sealed_token=Lg0-ckR6MostLY6zlv6N28heM0N_Ydc4NL-tKY3aExJD1B-Old0XKK2DhXNUgDM05YqAsK6FflkMH2hf4_AFsLMbFkXxxe4bU6P5yG1gmWVRVNI2jCchOnC4ftJLi2Zd9XVoBtYY3n0rpkCsfB-WoKVZaJW2GKgt-jygoj5XjoTHMxZWpXdNJ1QPaZu4" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=zifter/clickhouse-migrations&type=date&legend=top-left&sealed_token=Lg0-ckR6MostLY6zlv6N28heM0N_Ydc4NL-tKY3aExJD1B-Old0XKK2DhXNUgDM05YqAsK6FflkMH2hf4_AFsLMbFkXxxe4bU6P5yG1gmWVRVNI2jCchOnC4ftJLi2Zd9XVoBtYY3n0rpkCsfB-WoKVZaJW2GKgt-jygoj5XjoTHMxZWpXdNJ1QPaZu4" />
 </picture>
</a>
