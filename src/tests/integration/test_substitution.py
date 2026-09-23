import hashlib
import logging
import sys
from uuid import uuid4

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migrator import STATUS_APPLIED

# The cluster name of the dev cluster (dev/CH-TEMPLATE/config.xml).
CLUSTER_NAME = "company_cluster"
CLICKHOUSE_SERVERS = ("clickhouse01", "clickhouse02", "clickhouse03", "clickhouse04")
DRIVERS = ("clickhouse-driver", "clickhouse-connect")
SECRET = "s3cr3t-pa55w0rd"

CREATE_EVENTS = """CREATE TABLE pytest.events ON CLUSTER ${CLUSTER_NAME}
(
    id UInt64,
    note String DEFAULT 'not a placeholder: $${CLUSTER_NAME}'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}')
ORDER BY id;
"""
DROP_EVENTS = "DROP TABLE pytest.events ON CLUSTER ${CLUSTER_NAME} SYNC;\n"


@pytest.fixture(name="driver_cluster", params=DRIVERS)
def driver_cluster_fixture(request) -> ClickhouseCluster:
    return ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        driver=request.param,
    )


def _write(directory, files):
    directory.mkdir(exist_ok=True)
    for name, text in files.items():
        (directory / name).write_text(text, encoding="utf8")
    return directory


def _rows(cluster: ClickhouseCluster, query: str):
    with cluster.connection("") as conn:
        return conn.query(query)


def _servers_with_table(cluster: ClickhouseCluster, table: str) -> int:
    return sum(
        _rows(
            cluster,
            f"SELECT count() AS n FROM remote('{server}', 'system.tables') "
            f"WHERE database = 'pytest' AND name = '{table}'",
        )[0]["n"]
        for server in CLICKHOUSE_SERVERS
    )


def _run(monkeypatch, cluster: ClickhouseCluster, *args) -> int:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "clickhouse-migrations",
            *args,
            "--db-name",
            "pytest",
            "--driver",
            cluster.driver,
        ],
    )
    return main()


def test_templated_cluster_name(_schema, driver_cluster, tmp_path, monkeypatch):
    migrations = _write(tmp_path / "m", {"001_events.sql": CREATE_EVENTS})

    assert (
        _run(
            monkeypatch,
            driver_cluster,
            "migrate",
            "--migrations-dir",
            str(migrations),
            "--cluster-name",
            CLUSTER_NAME,
            "--var",
            f"CLUSTER_NAME={CLUSTER_NAME}",
        )
        == 0
    )

    assert _servers_with_table(driver_cluster, "events") == len(CLICKHOUSE_SERVERS)
    default = _rows(
        driver_cluster,
        "SELECT default_expression AS d FROM system.columns "
        "WHERE database = 'pytest' AND table = 'events' AND name = 'note'",
    )[0]["d"]
    assert default == "'not a placeholder: ${CLUSTER_NAME}'"

    # md5 and script are recorded from the raw file, placeholders included.
    stored = _rows(
        driver_cluster, "SELECT version, md5, script FROM pytest.schema_versions"
    )
    assert stored == [
        {
            "version": 1,
            "md5": hashlib.md5(CREATE_EVENTS.encode("utf8")).hexdigest(),
            "script": CREATE_EVENTS,
        }
    ]
    states = [r.state for r in driver_cluster.status("pytest", migrations)]
    assert states == [STATUS_APPLIED]


def test_changing_a_variable_does_not_rerun(_schema, driver_cluster, tmp_path):
    migrations = _write(tmp_path / "m", {"001_events.sql": CREATE_EVENTS})
    variables = {"CLUSTER_NAME": CLUSTER_NAME}

    applied = driver_cluster.migrate(
        "pytest", migrations, CLUSTER_NAME, variables=variables
    )
    assert [m.version for m in applied] == [1]

    # A different value (or no substitution at all) keeps the md5 stable, so
    # the applied migration is neither re-run nor reported as changed.
    assert not driver_cluster.migrate(
        "pytest", migrations, CLUSTER_NAME, variables={"CLUSTER_NAME": "renamed"}
    )
    assert not driver_cluster.migrate("pytest", migrations, CLUSTER_NAME)
    assert (
        _rows(driver_cluster, "SELECT count() AS n FROM pytest.schema_versions")[0]["n"]
        == 1
    )


def test_unset_variable_fails_before_anything_executes(
    driver_cluster, tmp_path, monkeypatch, caplog
):
    migrations = _write(
        tmp_path / "m",
        {
            "001_first.sql": "CREATE TABLE first (x String) ENGINE = Memory;",
            "002_second.sql": "CREATE TABLE second (x String DEFAULT '${PASSWORD}') "
            "ENGINE = Memory;\nSELECT '${MISSING}';",
        },
    )

    with caplog.at_level(logging.DEBUG):
        code = _run(
            monkeypatch,
            driver_cluster,
            "migrate",
            "--migrations-dir",
            str(migrations),
            "--var",
            f"PASSWORD={SECRET}",
        )

    assert code == 1
    assert "002_second.sql: variable(s) not set: MISSING (line 2)" in caplog.text
    assert SECRET not in caplog.text
    assert "first" not in driver_cluster.show_tables("pytest")
    assert (
        _rows(driver_cluster, "SELECT count() AS n FROM pytest.schema_versions")[0]["n"]
        == 0
    )


def test_substitution_is_off_without_flags(driver_cluster, tmp_path):
    raw = "CREATE TABLE raw (x String DEFAULT '${NOT_A_VAR} $${X} ${bad-name') ENGINE = Memory;"
    migrations = _write(tmp_path / "m", {"001_raw.sql": raw})

    driver_cluster.migrate("pytest", migrations)

    default = _rows(
        driver_cluster,
        "SELECT default_expression AS d FROM system.columns "
        "WHERE database = 'pytest' AND table = 'raw'",
    )[0]["d"]
    assert default == "'${NOT_A_VAR} $${X} ${bad-name'"


def test_substitute_env_and_var_precedence(driver_cluster, tmp_path, monkeypatch):
    migrations = _write(
        tmp_path / "m",
        {
            "001_t.sql": "CREATE TABLE ${TABLE_A} (x String DEFAULT '${NOTE}') "
            "ENGINE = Memory;",
        },
    )
    monkeypatch.setenv("TABLE_A", "from_env")
    monkeypatch.setenv("NOTE", "env note")

    assert (
        _run(
            monkeypatch,
            driver_cluster,
            "migrate",
            "--migrations-dir",
            str(migrations),
            "--substitute-env",
            "--var",
            "TABLE_A=from_var",
        )
        == 0
    )

    assert "from_var" in driver_cluster.show_tables("pytest")
    assert "from_env" not in driver_cluster.show_tables("pytest")
    default = _rows(
        driver_cluster,
        "SELECT default_expression AS d FROM system.columns "
        "WHERE database = 'pytest' AND table = 'from_var'",
    )[0]["d"]
    assert default == "'env note'"


def test_values_stay_out_of_our_logs_but_reach_query_log(
    driver_cluster, tmp_path, caplog
):
    # Unique per run, so query_log rows of earlier runs cannot satisfy the test.
    secret = f"{SECRET}-{uuid4().hex}"
    script = (
        "CREATE TABLE secret_holder (x String DEFAULT '${PASSWORD}') ENGINE = Memory;"
    )
    migrations = _write(tmp_path / "m", {"001_s.sql": script})

    with caplog.at_level(logging.DEBUG):
        driver_cluster.migrate("pytest", migrations, variables={"PASSWORD": secret})

    # Our own logs only: clickhouse-driver's own DEBUG log prints every query
    # it sends (documented, and the CLI warns about it).
    ours = "\n".join(
        r.getMessage()
        for r in caplog.records
        if not r.name.startswith(("clickhouse_driver", "clickhouse_connect"))
    )
    assert secret not in ours
    assert "DEFAULT '${PASSWORD}'" in ours
    assert (
        _rows(driver_cluster, "SELECT script FROM pytest.schema_versions")[0]["script"]
        == script
    )

    # The documented caveat: the server logs the substituted statement.
    with driver_cluster.connection("") as conn:
        conn.command("SYSTEM FLUSH LOGS")
        seen = conn.query(
            "SELECT count() AS n FROM system.query_log "
            "WHERE query LIKE 'CREATE TABLE secret_holder%' "
            f"AND query LIKE '%{secret}%' AND event_date >= yesterday()"
        )[0]["n"]
    assert seen > 0


def test_dry_run_validates_and_changes_nothing(driver_cluster, tmp_path, caplog):
    migrations = _write(
        tmp_path / "m",
        {"001_s.sql": "CREATE TABLE dry (x String DEFAULT '${P}') ENGINE = Memory;"},
    )

    with pytest.raises(MigrationException, match="001_s.sql: variable.*not set: P"):
        driver_cluster.migrate("pytest", migrations, dryrun=True, variables={})

    with caplog.at_level(logging.INFO):
        driver_cluster.migrate(
            "pytest", migrations, dryrun=True, variables={"P": SECRET}
        )

    assert "would have executed: CREATE TABLE dry (x String DEFAULT '${P}')" in (
        caplog.text
    )
    assert SECRET not in caplog.text
    assert "dry" not in driver_cluster.show_tables("pytest")


def test_templated_down(_schema, driver_cluster, tmp_path, monkeypatch):
    migrations = _write(
        tmp_path / "m",
        {"001_events.sql": CREATE_EVENTS, "001_events.down.sql": DROP_EVENTS},
    )
    common = ["--migrations-dir", str(migrations), "--cluster-name", CLUSTER_NAME]
    var = ["--var", f"CLUSTER_NAME={CLUSTER_NAME}"]

    assert _run(monkeypatch, driver_cluster, "migrate", *common, *var) == 0
    assert _servers_with_table(driver_cluster, "events") == len(CLICKHOUSE_SERVERS)

    # Without the variable the down script fails before touching anything.
    assert _run(monkeypatch, driver_cluster, "down", *common, "--var", "OTHER=1") == 1
    assert _servers_with_table(driver_cluster, "events") == len(CLICKHOUSE_SERVERS)

    assert _run(monkeypatch, driver_cluster, "down", *common, *var) == 0
    assert _servers_with_table(driver_cluster, "events") == 0
    assert (
        _rows(driver_cluster, "SELECT count() AS n FROM pytest.schema_versions")[0]["n"]
        == 0
    )
