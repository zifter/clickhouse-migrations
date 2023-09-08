[![ci](https://github.com/zifter/clickhouse-migrations/actions/workflows/ci.yaml/badge.svg)](https://github.com/zifter/clickhouse-migrations/actions/workflows/ci.yaml)
[![release](https://img.shields.io/github/release/zifter/clickhouse-migrations.svg)](https://github.com/zifter/clickhouse-migrations/releases)
[![supported versions](https://img.shields.io/pypi/pyversions/clickhouse-migrations.svg)](https://pypi.org/project/clickhouse-migrations/)
[![downloads](https://img.shields.io/pypi/dm/clickhouse-migrations.svg)](https://pypi.org/project/clickhouse-migrations/)
[![my site](https://img.shields.io/badge/site-my%20blog-yellow.svg)](https://zifter.github.io/)

# Clickhouse Migrations

Python library for creating and applying migrations in ClickHouse database.

Development and Maintenance of large-scale db systems many times requires constant changes to the actual DB system.
Holding off the scripts to migrate these will be painful.

## Features:
* Supports multi statements - more than one query per migration file.
* Allow running migrations out-of-box
* Simple file migrations format: {VERSION}_{name}.sql
* Supports Cluster deployments, makes sure that migrations state is consistent on all cluster nodes

## Known alternatives
This package originally forked from [clickhouse-migrator](https://github.com/delium/clickhouse-migrator).

Package | Differences
-------|---------
[clickhouse-migrator](https://github.com/delium/clickhouse-migrator) | Doesn't support multistatement in a single file , to heavy because of pandas, looks like abandoned
[django-clickhouse](https://github.com/carrotquest/django-clickhouse) | Need django
[clickhouse-migrate](https://github.com/trushad0w/clickhouse-migrate) | Doesn't support multistatement

## Installation

You can install from pypi using `pip install clickhouse-migrations`.

## Usage

### In command line
```bash
clickhouse-migrations --db-host localhost \
    --db-user default \
    --db-password secret \
    --db-name test \
    --migrations-dir ./migrations
```

### In code
```python
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster

cluster = ClickhouseCluster(db_host, db_user, db_password)
cluster.migrate(db_name, migrations_home, cluster_name=None,create_db_if_no_exists=True, multi_statement=True)
```

Parameter | Description                                                       | Default
-------|-------------------------------------------------------------------|---------
db_host | Clickhouse database hostname                                      | localhost
db_port | Clickhouse database port                                          | 9000
db_user | Clickhouse user                                                   | default
db_password | Clichouse password                                                | default
db_name| Clickhouse database name                                          | None
migrations_home | Path to list of migration files                                   | <project_root>
cluster_name | Name of Clickhouse topology cluster from <remote_servers>            | None
create_db_if_no_exists | If the `db_name` is not present, enabling this will create the db | True
multi_statement | Allow multiple statements in migration files                      | True

### Notes
The Clickhouse driver does not natively support executing multipe statements in a single query.
To allow for multiple statements in a single migration, you can use the multi_statement param.
There are two important caveats:
* This mode splits the migration text into separately-executed statements by a semi-colon ;. Thus cannot be used when a statement in the migration contains a string with a semi-colon.
* The queries are not executed in any sort of transaction/batch, meaning you are responsible for fixing partial migrations.
