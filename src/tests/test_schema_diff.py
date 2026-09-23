"""Unit tests of the pure diff code, on canned system-table rows."""

import pytest

from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.schema_diff import (
    KIND_DICTIONARY,
    KIND_MATERIALIZED_VIEW,
    KIND_TABLE,
    KIND_VIEW,
    Column,
    DbObject,
    Index,
    SchemaDiff,
    build_object,
    diff_schemas,
    engine_full_clauses,
    prepare_target,
    quote_name,
    split_elements,
    split_engine_prefix,
    write_diff_migration,
)

# --- prepare_target --------------------------------------------------------


def test_prepare_target_accepts_dump_output():
    prepared = prepare_target(
        "-- a comment\nCREATE TABLE events\n(\n    `id` UInt64\n)\n"
        "ENGINE = MergeTree\nORDER BY id;\n\n"
        "CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT 1;\n\n"
        "CREATE MATERIALIZED VIEW mv TO events AS SELECT 1 AS id;\n\n"
        "CREATE DICTIONARY `d` (id UInt64) PRIMARY KEY id "
        "SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);\n"
    )

    assert [(p.name, p.kind, p.engine_prefix) for p in prepared] == [
        ("events", KIND_TABLE, ""),
        ("v", KIND_VIEW, ""),
        ("mv", KIND_MATERIALIZED_VIEW, ""),
        ("d", KIND_DICTIONARY, ""),
    ]
    assert prepared[0].sql.endswith("ORDER BY id")


def test_prepare_target_makes_replicated_local_and_drops_on_cluster():
    prepared = prepare_target(
        "CREATE TABLE r ON CLUSTER '{cluster}' (id UInt64, ver UInt32) "
        "ENGINE = ReplicatedReplacingMergeTree('/p/{uuid}', '{replica}', ver) "
        "ORDER BY id;\n"
        "CREATE TABLE s (id UInt64) ENGINE = SharedMergeTree ORDER BY id;\n"
        "CREATE MATERIALIZED VIEW m ENGINE = ReplicatedMergeTree ORDER BY id "
        "AS SELECT 1 AS id;\n"
    )

    assert prepared[0].sql == (
        "CREATE TABLE r (id UInt64, ver UInt32) "
        "ENGINE = ReplacingMergeTree(ver) ORDER BY id"
    )
    assert prepared[0].engine_prefix == "Replicated"
    assert "ENGINE = MergeTree ORDER BY" in prepared[1].sql
    assert prepared[1].engine_prefix == "Shared"
    assert "ENGINE = MergeTree ORDER BY id AS" in prepared[2].sql
    assert prepared[2].engine_prefix == "Replicated"


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO t VALUES (1)",
        "DROP TABLE t",
        "CREATE DATABASE x",
        "CREATE TEMPORARY TABLE t (id UInt8) ENGINE = Memory",
        "CREATE LIVE VIEW v AS SELECT 1",
        "CREATE TABLE",
        "CREATE TABLE (id UInt8)",
        "ALTER TABLE t ADD COLUMN x UInt8",
    ],
)
def test_prepare_target_refuses_other_statements(statement):
    with pytest.raises(MigrationException, match="Statement 1 .* is not supported"):
        prepare_target(statement + ";")


def test_prepare_target_refuses_qualified_names():
    with pytest.raises(MigrationException, match=r"Statement 2 .*database-qualified"):
        prepare_target(
            "CREATE TABLE ok (id UInt8) ENGINE = Memory;\n"
            "-- note\nCREATE TABLE other.t (id UInt8) ENGINE = Memory;"
        )


def test_prepare_target_error_shows_the_statement_not_its_comment():
    with pytest.raises(MigrationException, match=r": DROP TABLE t;$"):
        prepare_target("-- remove it\nDROP TABLE t;")


def test_prepare_target_refuses_duplicates():
    with pytest.raises(MigrationException, match="defines 't' twice"):
        prepare_target("CREATE VIEW t AS SELECT 1; CREATE VIEW t AS SELECT 2;")


def test_prepare_target_of_an_empty_file():
    assert not prepare_target("-- nothing\n")


# --- model helpers --------------------------------------------------------

TABLE_TEXT = """CREATE TABLE db.events
(
    `id` UInt64,
    `name` String DEFAULT 'x' COMMENT 'c' CODEC(ZSTD(1)),
    `v` UInt8 TTL ts + toIntervalDay(1),
    INDEX ix id TYPE minmax GRANULARITY 1,
    PROJECTION p (SELECT id ORDER BY id),
    CONSTRAINT c CHECK id > 0
)
ENGINE = ReplicatedReplacingMergeTree('/p', '{replica}', v)
ORDER BY id
TTL ts + toIntervalDay(1)
SETTINGS index_granularity = 8192
COMMENT 'tbl'"""


def test_split_elements():
    columns, indices, others = split_elements(TABLE_TEXT)

    assert columns == {
        "id": "`id` UInt64",
        "name": "`name` String DEFAULT 'x' COMMENT 'c' CODEC(ZSTD(1))",
        "v": "`v` UInt8 TTL ts + toIntervalDay(1)",
    }
    assert indices == ["INDEX ix id TYPE minmax GRANULARITY 1"]
    assert others == [
        "PROJECTION p (SELECT id ORDER BY id)",
        "CONSTRAINT c CHECK id > 0",
    ]


@pytest.mark.parametrize(
    "text",
    [
        "CREATE TABLE t AS other ENGINE = Memory",
        "CREATE TABLE t ENGINE = Memory",
        "CREATE TABLE t",
    ],
)
def test_split_elements_without_a_column_list(text):
    assert split_elements(text) == ({}, [], [])


def test_split_engine_prefix():
    assert split_engine_prefix("CREATE TABLE t (a UInt8) ENGINE = Memory") == (
        "",
        "CREATE TABLE t (a UInt8) ENGINE = Memory",
    )
    assert split_engine_prefix("CREATE VIEW v AS SELECT 1") == (
        "",
        "CREATE VIEW v AS SELECT 1",
    )
    assert split_engine_prefix(
        "CREATE TABLE t (a UInt8) ENGINE = ReplicatedMergeTree ORDER BY a"
    ) == ("Replicated", "CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a")


def test_engine_full_clauses():
    assert engine_full_clauses(
        "MergeTree ORDER BY (id, x) TTL ts + toIntervalDay(1) DELETE "
        "SETTINGS index_granularity = 8192"
    ) == {"TTL": "ts + toIntervalDay(1) DELETE", "SETTINGS": "index_granularity = 8192"}
    assert not engine_full_clauses("MergeTree ORDER BY id")
    assert not engine_full_clauses("")


def test_quote_name():
    assert quote_name("a`b\\c") == "`a\\`b\\\\c`"


def _row(**values):
    row = {
        "name": "events",
        "engine": "ReplicatedReplacingMergeTree",
        "engine_full": "ReplicatedReplacingMergeTree('/p', '{replica}', v) ORDER BY id "
        "TTL ts + toIntervalDay(1) SETTINGS index_granularity = 8192",
        "sorting_key": "id",
        "partition_key": "",
        "primary_key": "id",
        "sampling_key": "",
        "comment": "tbl",
    }
    row.update(values)
    return row


def test_build_object_of_a_table():
    known = {"events", "dict"}
    obj = build_object(
        _row(),
        TABLE_TEXT,
        [
            {"name": "v", "position": 3, "type": "UInt8"},
            {
                "name": "name",
                "position": 2,
                "type": "String",
                "default_kind": "DEFAULT",
                "default_expression": "dictGet(db.dict, 'a', id)",
                "compression_codec": "CODEC(ZSTD(1))",
                "comment": "c",
            },
            {"name": "id", "position": 1, "type": "UInt64"},
        ],
        [
            {
                "name": "ix",
                "expr": "db.events.id",
                "type_full": "minmax",
                "granularity": 1,
            }
        ],
        "db",
        known,
    )

    assert obj.kind == KIND_TABLE
    assert obj.text.startswith("CREATE TABLE events\n")
    assert "ENGINE = ReplacingMergeTree(v)" in obj.text
    assert obj.engine == "ReplacingMergeTree(v)"
    assert obj.engine_prefix == "Replicated"
    assert (obj.sorting_key, obj.primary_key, obj.partition_key) == ("id", "id", "")
    assert obj.ttl == "ts + toIntervalDay(1)"
    assert obj.settings == "index_granularity = 8192"
    assert obj.comment == "tbl"
    assert [c.name for c in obj.columns] == ["id", "name", "v"]
    assert obj.columns[1] == Column(
        "name",
        "String",
        "DEFAULT",
        "dictGet(dict, 'a', id)",
        "CODEC(ZSTD(1))",
        "c",
        "`name` String DEFAULT 'x' COMMENT 'c' CODEC(ZSTD(1))",
    )
    assert obj.columns[0] == Column("id", "UInt64", definition="`id` UInt64")
    assert obj.indices == (Index("ix", "events.id", "minmax", 1),)
    assert len(obj.other_elements) == 2


def test_build_object_of_a_view():
    obj = build_object(
        {"name": "v", "engine": "View"},
        "CREATE VIEW db.v AS SELECT * FROM db.events",
        [{"name": "x", "position": 1, "type": "UInt8"}],
        [],
        "db",
        {"v", "events"},
    )

    assert obj == DbObject(
        "v",
        KIND_VIEW,
        "CREATE VIEW v AS SELECT * FROM events",
        references=frozenset({"events", "v"}),
    )


# --- diff_schemas ---------------------------------------------------------


def _table(name="t", columns=None, **values):
    columns = columns or [col("id", "UInt64")]
    values.setdefault("engine", "MergeTree")
    values.setdefault("sorting_key", "id")
    values.setdefault("primary_key", "id")
    return DbObject(
        name,
        KIND_TABLE,
        f"CREATE TABLE {name} (...) ENGINE = {values['engine']}",
        columns=tuple(columns),
        **values,
    )


def col(name, type_, kind="", expr="", codec="", comment="", extra=""):
    definition = f"`{name}` {type_}"
    if kind:
        definition += f" {kind} {expr}"
    if comment:
        definition += f" COMMENT '{comment}'"
    if codec:
        definition += f" {codec}"
    return Column(name, type_, kind, expr, codec, comment, definition + extra)


def _diff(live, target):
    live_map = {o.name: o for o in live}
    target_map = {o.name: o for o in target}
    return diff_schemas(live_map, target_map, list(live_map), list(target_map))


def _sql(plan):
    return [s.sql for change in plan.changes for s in change.statements]


def test_no_changes():
    plan = _diff([_table()], [_table()])

    assert plan == SchemaDiff()
    assert plan.is_empty
    assert plan.render() == ""


def test_create_uses_target_text_and_restores_the_replicated_prefix():
    target = _table("r", engine="ReplacingMergeTree(v)", engine_prefix="Replicated")
    target = target._replace(
        text="CREATE TABLE r\n(...)\nENGINE = ReplacingMergeTree(v)"
    )
    view = DbObject("v", KIND_VIEW, "CREATE VIEW v AS SELECT 1")

    plan = _diff([], [target, view])

    assert _sql(plan) == [
        "CREATE TABLE r\n(...)\nENGINE = ReplicatedReplacingMergeTree(v)",
        "CREATE VIEW v AS SELECT 1",
    ]
    assert [c.summary for c in plan.changes] == ["create table `r`", "create view `v`"]


def test_drop_objects_dependents_first_and_destructive():
    live = [
        _table("a"),
        DbObject("v", KIND_VIEW, "CREATE VIEW v AS SELECT 1"),
        DbObject("m", KIND_MATERIALIZED_VIEW, "CREATE MATERIALIZED VIEW m AS SELECT 1"),
        DbObject("d", KIND_DICTIONARY, "CREATE DICTIONARY d"),
    ]

    plan = _diff(live, [])

    assert _sql(plan) == [
        "DROP DICTIONARY `d`",
        "DROP VIEW `m`",
        "DROP VIEW `v`",
        "DROP TABLE `a`",
    ]
    assert all(s.destructive for c in plan.changes for s in c.statements)


def test_rename_hints():
    plan = _diff([_table("old")], [_table("new")])

    assert "RENAME TABLE" in plan.notes[0]

    plan = _diff(
        [_table(columns=[col("id", "UInt64"), col("a", "UInt8")])],
        [_table(columns=[col("id", "UInt64"), col("b", "UInt8")])],
    )
    assert "RENAME COLUMN" in plan.notes[0]


def test_add_columns_first_middle_last():
    plan = _diff(
        [_table(columns=[col("id", "UInt64"), col("v", "UInt8")])],
        [
            _table(
                columns=[
                    col("f", "UInt8"),
                    col("id", "UInt64"),
                    col("m", "String", "DEFAULT", "'x'"),
                    col("v", "UInt8"),
                    col("l", "UInt8"),
                ]
            )
        ],
    )

    assert _sql(plan) == [
        "ALTER TABLE `t` ADD COLUMN `f` UInt8 FIRST",
        "ALTER TABLE `t` ADD COLUMN `m` String DEFAULT 'x' AFTER `id`",
        "ALTER TABLE `t` ADD COLUMN `l` UInt8 AFTER `v`",
    ]
    assert plan.changes[0].summary == (
        "table `t`: add column `f`; add column `m`; add column `l`"
    )


def test_add_column_without_a_definition_falls_back_to_the_type():
    target = _table(columns=[col("id", "UInt64"), Column("x", "UInt8")])

    assert _sql(_diff([_table()], [target])) == [
        "ALTER TABLE `t` ADD COLUMN `x` UInt8 AFTER `id`"
    ]


def test_drop_column_is_destructive():
    plan = _diff([_table(columns=[col("id", "UInt64"), col("x", "UInt8")])], [_table()])

    assert plan.changes[0].statements[0].sql == "ALTER TABLE `t` DROP COLUMN `x`"
    assert plan.changes[0].statements[0].destructive
    assert "-- ALTER TABLE `t` DROP COLUMN `x`;" in plan.render()
    assert "\nALTER TABLE `t` DROP COLUMN `x`;" in plan.render(allow_destructive=True)


@pytest.mark.parametrize(
    "before,after,expected,summary",
    [
        (
            col("c", "UInt8"),
            col("c", "UInt16"),
            ["MODIFY COLUMN `c` UInt16"],
            "type (rewrites the column data)",
        ),
        (
            col("c", "UInt8"),
            col("c", "UInt8", "DEFAULT", "1"),
            ["MODIFY COLUMN `c` UInt8 DEFAULT 1"],
            "default",
        ),
        (
            col("c", "UInt8", "DEFAULT", "1"),
            col("c", "UInt16", "MATERIALIZED", "2"),
            ["MODIFY COLUMN `c` UInt16 MATERIALIZED 2"],
            "default, type (rewrites the column data)",
        ),
        (
            col("c", "UInt8", "ALIAS", "1"),
            col("c", "UInt8"),
            ["MODIFY COLUMN `c` REMOVE ALIAS"],
            "default",
        ),
        (
            col("c", "UInt8", "DEFAULT", "1"),
            col("c", "UInt16"),
            ["MODIFY COLUMN `c` REMOVE DEFAULT", "MODIFY COLUMN `c` UInt16"],
            "default, type (rewrites the column data)",
        ),
        (
            col("c", "UInt8"),
            col("c", "UInt8", "EPHEMERAL", "defaultValueOfTypeName('UInt8')"),
            ["MODIFY COLUMN `c` UInt8 EPHEMERAL"],
            "default",
        ),
        (
            col("c", "UInt8", codec="CODEC(ZSTD(1))"),
            col("c", "UInt8", codec="CODEC(LZ4)"),
            ["MODIFY COLUMN `c` UInt8 CODEC(LZ4)"],
            "codec",
        ),
        (
            col("c", "UInt8", codec="CODEC(ZSTD(1))"),
            col("c", "UInt8"),
            ["MODIFY COLUMN `c` REMOVE CODEC"],
            "codec",
        ),
        (
            col("c", "UInt8"),
            col("c", "UInt8", comment="it's"),
            ["COMMENT COLUMN `c` 'it\\'s'"],
            "comment",
        ),
        (
            col("c", "UInt8", comment="x"),
            col("c", "UInt8"),
            ["COMMENT COLUMN `c` ''"],
            "comment",
        ),
    ],
)
def test_modify_column(before, after, expected, summary):
    plan = _diff(
        [_table(columns=[col("id", "UInt64"), before])],
        [_table(columns=[col("id", "UInt64"), after])],
    )

    assert _sql(plan) == [f"ALTER TABLE `t` {sql}" for sql in expected]
    assert plan.changes[0].summary == f"table `t`: modify column `c` ({summary})"
    assert not plan.has_destructive


def test_reorder_columns():
    a, b, c = col("a", "UInt8"), col("b", "String"), col("c", "UInt16")
    plan = _diff([_table(columns=[a, b, c])], [_table(columns=[c, a, b])])

    assert _sql(plan) == ["ALTER TABLE `t` MODIFY COLUMN `c` UInt16 FIRST"]
    assert plan.changes[0].summary == "table `t`: move column `c`"

    plan = _diff([_table(columns=[a, b, c])], [_table(columns=[b, c, a])])
    assert _sql(plan) == [
        "ALTER TABLE `t` MODIFY COLUMN `b` String FIRST",
        "ALTER TABLE `t` MODIFY COLUMN `c` UInt16 AFTER `b`",
    ]


def test_indices():
    keep = Index("keep", "id", "minmax", 1)
    gone = Index("gone", "id", "minmax", 1)
    changed_before = Index("changed", "s", "minmax", 1)
    changed_after = Index("changed", "s", "bloom_filter", 3)
    new = Index("new", "lower(s)", "ngrambf_v1(3, 256, 2, 0)", 4)

    plan = _diff(
        [_table(indices=(gone, keep, changed_before))],
        [_table(indices=(new, keep, changed_after))],
    )

    statements = plan.changes[0].statements
    assert [(s.sql, s.destructive) for s in statements] == [
        ("ALTER TABLE `t` DROP INDEX `gone`", True),
        ("ALTER TABLE `t` DROP INDEX `changed`", True),
        (
            "ALTER TABLE `t` ADD INDEX `new` lower(s) TYPE ngrambf_v1(3, 256, 2, 0) "
            "GRANULARITY 4 FIRST",
            False,
        ),
        (
            "ALTER TABLE `t` ADD INDEX `changed` s TYPE bloom_filter GRANULARITY 3 "
            "AFTER `keep`",
            True,
        ),
    ]
    summary = plan.changes[0].summary
    assert "drop index `gone`" in summary
    assert "add index `new` (only new data is indexed" in summary
    assert "replace index `changed`" in summary


def test_indices_come_before_dropped_columns():
    plan = _diff(
        [
            _table(
                columns=[col("id", "UInt64"), col("x", "UInt8")],
                indices=(Index("ix", "x", "minmax", 1),),
            )
        ],
        [_table()],
    )

    assert _sql(plan) == [
        "ALTER TABLE `t` DROP INDEX `ix`",
        "ALTER TABLE `t` DROP COLUMN `x`",
    ]


def test_ttl_and_comment():
    plan = _diff([_table()], [_table(ttl="ts + toIntervalDay(1)", comment="it's")])
    assert _sql(plan) == [
        "ALTER TABLE `t` MODIFY TTL ts + toIntervalDay(1)",
        "ALTER TABLE `t` MODIFY COMMENT 'it\\'s'",
    ]
    assert "expired rows are deleted" in plan.changes[0].summary

    plan = _diff([_table(ttl="ts + toIntervalDay(1)")], [_table()])
    assert _sql(plan) == ["ALTER TABLE `t` REMOVE TTL"]
    assert plan.changes[0].summary == "table `t`: remove TTL"


def test_replace_view_and_dictionary():
    plan = _diff(
        [
            DbObject("v", KIND_VIEW, "CREATE VIEW v AS SELECT 1"),
            DbObject("d", KIND_DICTIONARY, "CREATE DICTIONARY d (a UInt8) LIFETIME(0)"),
        ],
        [
            DbObject("v", KIND_VIEW, "CREATE VIEW v AS SELECT 2"),
            DbObject("d", KIND_DICTIONARY, "CREATE DICTIONARY d (a UInt8) LIFETIME(1)"),
        ],
    )

    assert _sql(plan) == [
        "CREATE OR REPLACE VIEW v AS SELECT 2",
        "CREATE OR REPLACE DICTIONARY d (a UInt8) LIFETIME(1)",
    ]
    assert [c.summary for c in plan.changes] == [
        "replace view `v`",
        "replace dictionary `d`",
    ]


def _mv(text, prefix=""):
    return DbObject("mv", KIND_MATERIALIZED_VIEW, text, engine_prefix=prefix)


def test_materialized_view_to_table_modify_query():
    plan = _diff(
        [
            _mv(
                "CREATE MATERIALIZED VIEW mv TO t\n(\n    `id` UInt64\n)\nAS SELECT id\nFROM e"
            )
        ],
        [
            _mv(
                "CREATE MATERIALIZED VIEW mv TO t\n(\n    `id` UInt64,\n    `x` UInt8\n)\n"
                "AS SELECT\n    id,\n    x\nFROM e"
            )
        ],
    )

    assert _sql(plan) == [
        "ALTER TABLE `mv` MODIFY QUERY SELECT\n    id,\n    x\nFROM e"
    ]
    assert plan.changes[0].summary == "materialized view `mv`: modify query"


def test_materialized_view_with_inner_table_modify_query():
    head = "CREATE MATERIALIZED VIEW mv\n(\n    `id` UInt64\n)\nENGINE = MergeTree\n"
    plan = _diff(
        [_mv(head + "AS SELECT id FROM e")], [_mv(head + "AS SELECT id FROM f")]
    )

    assert _sql(plan) == ["ALTER TABLE `mv` MODIFY QUERY SELECT id FROM f"]


@pytest.mark.parametrize(
    "before,after",
    [
        (
            _mv("CREATE MATERIALIZED VIEW mv TO t (id UInt64) AS SELECT id FROM e"),
            _mv("CREATE MATERIALIZED VIEW mv TO u (id UInt64) AS SELECT id FROM e"),
        ),
        (
            _mv(
                "CREATE MATERIALIZED VIEW mv (id UInt64) ENGINE = Log AS SELECT id FROM e"
            ),
            _mv(
                "CREATE MATERIALIZED VIEW mv (id UInt8) ENGINE = Log AS SELECT id FROM e"
            ),
        ),
        (
            _mv(
                "CREATE MATERIALIZED VIEW mv (id UInt64) ENGINE = MergeTree AS SELECT 1"
            ),
            _mv(
                "CREATE MATERIALIZED VIEW mv (id UInt64) ENGINE = MergeTree AS SELECT 1",
                "Replicated",
            ),
        ),
        (
            _mv("CREATE MATERIALIZED VIEW mv (id UInt64) ENGINE = Log AS SELECT 1"),
            _mv("CREATE MATERIALIZED VIEW mv (id UInt64) ENGINE = Memory"),
        ),
    ],
)
def test_materialized_view_refusals(before, after):
    plan = _diff([before], [after])

    assert not plan.changes
    assert "MODIFY QUERY cannot" in plan.refusals[0]


def test_kind_change_is_refused():
    plan = _diff([_table("x")], [DbObject("x", KIND_VIEW, "CREATE VIEW x AS SELECT 1")])

    assert plan.refusals == (
        "`x` is a table in the database but a view in the schema file: "
        "drop and recreate it by hand.",
    )


@pytest.mark.parametrize(
    "changes,expected,rebuild",
    [
        (
            {"engine": "ReplacingMergeTree"},
            "the engine changes from MergeTree to ReplacingMergeTree",
            True,
        ),
        (
            {"engine_prefix": "Replicated"},
            "the engine changes from MergeTree to ReplicatedMergeTree",
            True,
        ),
        ({"sorting_key": "id, ts"}, "ORDER BY changes from id to id, ts", True),
        (
            {"partition_key": "toYYYYMM(ts)"},
            "PARTITION BY changes from (none) to toYYYYMM(ts)",
            True,
        ),
        ({"primary_key": ""}, "PRIMARY KEY changes from id to (none)", True),
        ({"sampling_key": "id"}, "SAMPLE BY changes from (none) to id", True),
        (
            {"settings": "index_granularity = 1"},
            "SETTINGS change from (none) to index_granularity = 1",
            False,
        ),
        (
            {"other_elements": ("PROJECTION p (SELECT 1)",)},
            "its projections or constraints change",
            False,
        ),
    ],
)
def test_table_refusals(changes, expected, rebuild):
    plan = _diff([_table(ttl="x")], [_table(**changes)])

    assert not plan.changes
    assert plan.refusals[0].startswith(f"table `t`: {expected}")
    assert ("ClickHouse cannot change this in place" in plan.refusals[0]) is rebuild
    sql = plan.render()
    assert all(line.startswith("--") for line in sql.splitlines() if line)
    assert "ALTER" not in sql.replace("ALTER TABLE ... MODIFY SETTING", "")


def test_unknown_column_difference_is_refused():
    plan = _diff(
        [_table(columns=[col("id", "UInt64", extra=" TTL ts + toIntervalDay(1)")])],
        [_table()],
    )

    assert "column `id` changes in a way diff does not handle" in plan.refusals[0]


def test_index_order_change_is_refused():
    a, b = Index("a", "id", "minmax", 1), Index("b", "id", "minmax", 1)

    plan = _diff([_table(indices=(a, b))], [_table(indices=(b, a))])

    assert "the order of its data skipping indices changes" in plan.refusals[0]


# --- render ---------------------------------------------------------------


def test_render():
    plan = _diff(
        [
            _table("gone"),
            _table(columns=[col("id", "UInt64")]),
            _table("bad"),
        ],
        [
            _table(columns=[col("id", "UInt64"), col("x", "UInt8")]),
            _table("bad", sorting_key="x"),
        ],
    )

    assert plan.render() == (
        "-- Generated by `clickhouse-migrations diff`: brings the database to the "
        "target schema.\n"
        "-- Review (and edit) it before applying: this is a proposal, not a "
        "verified plan.\n"
        "--\n"
        "-- Changes:\n"
        "--   * drop table `gone`\n"
        "--   * table `t`: add column `x`\n"
        "--\n"
        "-- Destructive statements are commented out: review them and uncomment, or "
        "rerun with --allow-destructive.\n"
        "--\n"
        "-- Refused (no SQL generated for these, write them by hand):\n"
        "--   * table `bad`: ORDER BY changes from id to x. ClickHouse cannot change "
        "this in place: create a new table with the target definition, copy the data "
        "(INSERT ... SELECT) and swap the tables (EXCHANGE TABLES / RENAME), by hand.\n"
        "\n"
        "-- DROP TABLE `gone`;\n"
        "\n"
        "ALTER TABLE `t` ADD COLUMN `x` UInt8 AFTER `id`;\n"
    )
    assert "-- Destructive statements are included (--allow-destructive).\n" in (
        plan.render(allow_destructive=True)
    )


def test_render_notes_and_multiline_comments():
    plan = SchemaDiff(refusals=("two\nlines",), notes=("a note",))

    assert plan.render().endswith(
        "-- Refused (no SQL generated for these, write them by hand):\n"
        "--   * two lines\n--\n-- Notes:\n--   * a note\n"
    )


def test_write_diff_migration(tmp_path):
    (tmp_path / "001_init.sql").write_text("", encoding="utf8")

    path = write_diff_migration(tmp_path, "SELECT 1;\n", name="add stuff")

    assert path == tmp_path / "002_add_stuff.sql"
    lines = path.read_text(encoding="utf8").splitlines()
    assert lines[0] == "-- add stuff"
    assert lines[1].startswith("-- created: ")
    assert lines[2:] == ["", "SELECT 1;"]
