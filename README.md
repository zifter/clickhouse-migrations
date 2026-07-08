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

# put versioned .sql files in ./migrations (e.g. 001_init.sql), then apply them:
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

With `clickhouse-connect` the default port is `8123` (HTTP). Connecting via `--db-url` is only supported with the default `clickhouse-driver`; use `--db-host`/`--db-port` for `clickhouse-connect`.

## Migration files

Migration files follow the naming convention `{VERSION}_{name}.sql`, e.g. `001_init.sql`, `002_add_users.sql`.

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
`--multi-statement` | `MULTI_STATEMENT` | `true`
`--create-db-if-not-exists` | `CREATE_DB_IF_NOT_EXISTS` | `true`
`--dry-run` | `DRY_RUN` | `false`
`--fake` | `FAKE` | `false`
`--secure` | `SECURE` | `false`
`--log-level` | `LOG_LEVEL` | `WARNING`
`--migration-log-format` | `MIGRATION_LOG_FORMAT` | `full`
`--driver` | `DRIVER` | `clickhouse-driver`

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
`create_db_if_no_exists` | Create the database if it does not exist | `True`
`multi_statement` | Allow multiple statements per migration file | `True`
`dryrun` | Print migrations without executing them | `False`
`fake` | Mark migrations as applied without executing SQL | `False`
`secure` | Use secure (TLS) connection | `False`
`migration_log_format` | Migration log format `full` logs the full Migration object, `compact` logs only version and md5 | `full`

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
    # or connect via a single URL instead of the db-* inputs (clickhouse-driver only):
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

The image bundles **both drivers**. It uses the native `clickhouse-driver` by default; to use the official HTTP `clickhouse-connect` driver, pass `--driver clickhouse-connect` (default port `8123`, and note `--db-url` is `clickhouse-driver` only):

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

[![Star History Chart](assets/star-history.svg)](https://star-history.com/#zifter/clickhouse-migrations&Date)

<sub>Chart is a self-contained SVG (`assets/star-history.svg`) generated from the GitHub API and refreshed by the [`star-history`](.github/workflows/star-history.yml) workflow, so it never breaks when a third-party embed service is rate-limited.</sub>
