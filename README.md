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
`--secure` | `SECURE` | `false`
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
VERSION  STATUS   MD5                               APPLIED AT
1        applied  6172991b15b0852bc895e09b3e91ade4  2024-01-01 12:00:00
2        pending  1a79a4d60de6718e8e5b326e338ae533
```

States: `applied`, `pending`, `md5-mismatch` (a file changed after being applied), and `unknown` (applied but no longer present locally). It is read-only and never creates the database.

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
`secure` | Use secure (TLS) connection | `False`
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
          volumeMounts:
            - name: migrations
              mountPath: /migrations
      volumes:
        - name: migrations
          configMap:
            name: clickhouse-migrations
```

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
