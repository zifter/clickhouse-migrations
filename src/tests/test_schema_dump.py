import os
import stat

import pytest

from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.schema_dump import (
    KIND_PUNCT,
    KIND_QUOTED,
    KIND_SPACE,
    KIND_STRING,
    KIND_WORD,
    Token,
    diff_dumps,
    identifier_value,
    normalize_dump_text,
    normalize_statement,
    render_dump,
    sort_by_dependency,
    tokenize,
    write_text_atomic,
)

KNOWN = {"events", "totals", "mv", "v1", "names", "my-table", "prod"}
UUID = "12345678-1234-1234-1234-123456789abc"


def norm(statement, db="prod", keep=False, known=None):
    return normalize_statement(statement, db, known or KNOWN, keep).text


def test_tokenize_round_trips_and_classifies():
    sql = "SELECT `a``b`, \"c\", 'it''s \\' x' /* c */ -- tail\n$$ raw ' $$ 1.5"

    tokens = tokenize(sql)

    assert "".join(t.text for t in tokens) == sql
    kinds = {t.kind for t in tokens}
    assert kinds == {KIND_WORD, KIND_QUOTED, KIND_STRING, KIND_SPACE, KIND_PUNCT}
    assert Token(KIND_STRING, "'it''s \\' x'") in tokens
    assert Token(KIND_STRING, "$$ raw ' $$") in tokens


def test_identifier_value():
    assert identifier_value(Token(KIND_WORD, "abc")) == "abc"
    assert identifier_value(Token(KIND_QUOTED, "`a``b`")) == "a`b"
    assert identifier_value(Token(KIND_QUOTED, '"a\\"b"')) == 'a"b'
    assert identifier_value(Token(KIND_STRING, "'x'")) is None


def test_uuid_is_removed_everywhere_in_the_header():
    assert (
        norm(f"CREATE TABLE prod.events UUID '{UUID}' (`id` UInt8) ENGINE = Memory")
        == "CREATE TABLE events (`id` UInt8) ENGINE = Memory"
    )
    assert (
        norm(
            f"CREATE MATERIALIZED VIEW prod.mv UUID '{UUID}' TO INNER UUID '{UUID}' AS SELECT 1"
        )
        == "CREATE MATERIALIZED VIEW mv AS SELECT 1"
    )
    assert (
        norm(f"CREATE VIEW prod.v1 UUID '{UUID}' AS SELECT 1")
        == "CREATE VIEW v1 AS SELECT 1"
    )


def test_uuid_lookalikes_are_kept():
    # Not a UUID string, and a UUID literal in the body, are not touched.
    assert "UUID 'abc'" in norm(
        "CREATE TABLE prod.events UUID 'abc' (`id` UInt8) ENGINE = Memory"
    )
    body = f"CREATE VIEW v1 AS SELECT UUID '{UUID}'"
    assert norm(body) == body
    assert norm("CREATE TABLE t UUID") == "CREATE TABLE t UUID"


def test_database_prefix_is_removed_from_references():
    statement = (
        "CREATE MATERIALIZED VIEW prod.mv TO prod.totals (`id` UInt64) AS "
        "SELECT id FROM prod.events AS e JOIN `prod`.`my-table` ON e.id = prod.events.id"
    )

    normalized = normalize_statement(statement, "prod", KNOWN)

    assert normalized.text == (
        "CREATE MATERIALIZED VIEW mv TO totals (`id` UInt64) AS "
        "SELECT id FROM events AS e JOIN `my-table` ON e.id = events.id"
    )
    assert normalized.references == {"mv", "totals", "events", "my-table"}


def test_database_prefix_leaves_unsafe_cases_alone():
    cases = [
        # string literals and comments
        "SELECT 'prod.events', \"prod.events\" FROM other.events",
        "SELECT 1 -- prod.events\n/* prod.events */",
        # another database, unknown object, longer path, function call
        "SELECT * FROM other.events, prod.unknown, x.prod.events, prod.events(1)",
        # a longer identifier that merely ends with the database name
        "SELECT * FROM my_prod.events, `my.prod`.events",
    ]
    for case in cases:
        assert norm(case) == case

    # Truncated input never fails.
    assert norm("SELECT prod.") == "SELECT prod."
    assert norm("SELECT prod.events") == "SELECT events"


def test_database_prefix_with_special_database_name():
    assert (
        norm("CREATE TABLE `my-db`.events (`id` UInt8) ENGINE = Memory", db="my-db")
        == "CREATE TABLE events (`id` UInt8) ENGINE = Memory"
    )


def test_replicated_arguments_are_removed():
    template = "CREATE TABLE events (`id` UInt8) ENGINE = {engine} ORDER BY id"
    cases = {
        "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')": "ReplicatedMergeTree",
        "ReplicatedMergeTree('/ch/{shard}/prod/events', '{replica}')": "ReplicatedMergeTree",
        "ReplicatedReplacingMergeTree('/p', '{replica}', ver)": "ReplicatedReplacingMergeTree(ver)",
        "ReplicatedVersionedCollapsingMergeTree('/p', 'r', sign,\n  ver)": (
            "ReplicatedVersionedCollapsingMergeTree(sign, ver)"
        ),
        "ReplicatedSummingMergeTree('/p', '{replica}', (a, b))": "ReplicatedSummingMergeTree((a, b))",
        "SharedMergeTree('/p', '{replica}')": "SharedMergeTree",
        # already argument-less or not a plain string path: untouched
        "ReplicatedMergeTree": "ReplicatedMergeTree",
        "ReplicatedMergeTree('/only/path')": "ReplicatedMergeTree('/only/path')",
        "ReplicatedMergeTree(path_expr(), '{replica}')": "ReplicatedMergeTree(path_expr(), '{replica}')",
        "MergeTree": "MergeTree",
    }
    for engine, expected in cases.items():
        assert norm(template.format(engine=engine)) == template.format(engine=expected)


def test_replicated_arguments_are_kept_on_request():
    statement = (
        "CREATE TABLE events (`id` UInt8) ENGINE = "
        "ReplicatedMergeTree('/ch/{shard}/events', '{replica}') ORDER BY id"
    )

    assert norm(statement, keep=True) == statement


def test_replicated_in_a_view_body_is_not_an_engine():
    statement = "CREATE VIEW v1 AS SELECT engine = ReplicatedMergeTree('a', 'b') FROM t"

    assert norm(statement) == statement
    assert norm("CREATE TABLE t (`c` Nested(a UInt8)) ENGINE = Memory") == (
        "CREATE TABLE t (`c` Nested(a UInt8)) ENGINE = Memory"
    )


def test_database_arguments_of_engines_become_current_database():
    assert (
        norm(
            "CREATE TABLE d (`id` UInt8) ENGINE = Distributed('cl', 'prod', 'events', rand())"
        )
        == "CREATE TABLE d (`id` UInt8) ENGINE = Distributed('cl', currentDatabase(), 'events', rand())"
    )
    assert (
        norm("CREATE TABLE d (`id` UInt8) ENGINE = Merge('prod', '^events')")
        == "CREATE TABLE d (`id` UInt8) ENGINE = Merge(currentDatabase(), '^events')"
    )
    assert (
        norm("CREATE TABLE d (`id` UInt8) ENGINE = Buffer('prod', 'events', 1, 2)")
        == "CREATE TABLE d (`id` UInt8) ENGINE = Buffer(currentDatabase(), 'events', 1, 2)"
    )


def test_database_arguments_of_other_databases_are_kept():
    for engine in (
        "Distributed('cl', 'other', 'events', rand())",
        "Distributed('cl', prod, 'events')",
        "Distributed('cl')",
        "Merge('other', '^x')",
        "Merge(currentDatabase(), '^x')",
        "Log",
    ):
        statement = f"CREATE TABLE d (`id` UInt8) ENGINE = {engine}"
        assert norm(statement) == statement


def test_engine_argument_layout_edge_cases():
    # Leading whitespace inside the parentheses, and a truncated statement.
    assert (
        norm("CREATE TABLE d (`id` UInt8) ENGINE = Merge( 'prod' , '^e')")
        == "CREATE TABLE d (`id` UInt8) ENGINE = Merge( currentDatabase() , '^e')"
    )
    truncated = "CREATE TABLE d (`id` UInt8) ENGINE = Merge('prod', '^e'"
    assert norm(truncated) == truncated


def test_dictionary_source_database():
    template = (
        "CREATE DICTIONARY names (`id` UInt64) PRIMARY KEY id "
        "SOURCE(CLICKHOUSE({source})) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)"
    )
    local = "DB 'prod' TABLE 'events' USER 'default'"
    normalized = normalize_statement(
        template.format(source=local).replace("names", "prod.names"), "prod", KNOWN
    )
    assert normalized.text == template.format(source="TABLE 'events' USER 'default'")
    assert normalized.references == {"events", "names"}

    # DB last, still tidy.
    assert norm(template.format(source="TABLE 'events' DB 'prod'")) == template.format(
        source="TABLE 'events'"
    )
    # Remote sources, other databases and sources without a database stay.
    for source in (
        "HOST 'h' PORT 9000 DB 'prod' TABLE 'events'",
        "DB 'other' TABLE 'events'",
        "TABLE 'events'",
        "QUERY 'SELECT 1'",
    ):
        assert norm(template.format(source=source)) == template.format(source=source)


def test_dictionary_source_edge_cases():
    # CLICKHOUSE not followed by a call, and no CLICKHOUSE source at all.
    for statement in (
        "CREATE DICTIONARY names (`id` UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE) LAYOUT(FLAT())",
        "CREATE DICTIONARY names (`id` UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT())",
    ):
        assert norm(statement) == statement
    assert (
        normalize_statement(
            "CREATE DICTIONARY names (`id` UInt64) PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(TABLE 'nope' DB 'prod'))",
            "prod",
            KNOWN,
        ).references
        == set()
    )


def test_multiline_statement_loses_trailing_whitespace():
    statement = (
        "CREATE TABLE prod.events   \n(\n    `id` UInt8  \n)\nENGINE = Memory\n\n"
    )

    assert (
        norm(statement) == "CREATE TABLE events\n(\n    `id` UInt8\n)\nENGINE = Memory"
    )


def test_sort_by_dependency_is_stable_and_alphabetical():
    order = sort_by_dependency(
        {
            "b_view": {"a_table", "z_table"},
            "a_table": set(),
            "z_table": {"unknown", "z_table"},
            "c": set(),
            "a_over_view": {"b_view"},
        }
    )

    assert order == ["a_table", "c", "z_table", "b_view", "a_over_view"]


def test_sort_by_dependency_reports_the_cycle():
    with pytest.raises(MigrationException) as excinfo:
        sort_by_dependency(
            {"ok": set(), "tail": {"b"}, "a": {"b"}, "b": {"c"}, "c": {"a", "ok"}}
        )

    assert "Circular dependency" in str(excinfo.value)
    assert "a -> b -> c -> a" in str(excinfo.value)


def test_render_dump():
    assert render_dump([]) == ""
    assert render_dump(["A", "B"]) == "A;\n\nB;\n"


def test_normalize_dump_text():
    assert normalize_dump_text("A;  \r\n\r\nB;\r\n\r\n\r\n") == "A;\n\nB;\n"
    assert normalize_dump_text("\n \n") == ""
    assert normalize_dump_text("") == ""


def test_diff_dumps():
    assert diff_dumps("A;\n", "A;  \r\n", "file", "db") == ""

    diff = diff_dumps("A;\n\nB;\n", "A;\n\nC;\n", "file.sql", "database x")

    assert diff.splitlines()[:2] == ["--- file.sql", "+++ database x"]
    assert "-B;" in diff and "+C;" in diff


def test_write_text_atomic_creates_and_replaces(tmp_path):
    target = tmp_path / "schema.sql"

    write_text_atomic(target, "one\n")
    assert target.read_text(encoding="utf8") == "one\n"
    assert stat.S_IMODE(target.stat().st_mode) == 0o644

    os.chmod(target, 0o600)
    write_text_atomic(target, "two\n")
    assert target.read_text(encoding="utf8") == "two\n"
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert [p.name for p in tmp_path.iterdir()] == ["schema.sql"]


def test_write_text_atomic_cleans_up_on_failure(tmp_path):
    target = tmp_path / "dir"
    target.mkdir()
    (target / "keep").write_text("x", encoding="utf8")

    with pytest.raises(OSError):
        write_text_atomic(target, "text")

    assert [p.name for p in tmp_path.iterdir()] == ["dir"]
