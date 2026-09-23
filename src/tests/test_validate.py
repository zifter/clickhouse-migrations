import json
import sys
from pathlib import Path

import pytest

from clickhouse_migrations.command_line import SUBCOMMANDS, get_context, main
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migrator import (
    Migrator,
    find_unterminated_token,
    split_statement_tokens,
)
from clickhouse_migrations.validate import (
    CHECK_BAD_FILENAME,
    CHECK_DESTRUCTIVE,
    CHECK_DUPLICATE_VERSION,
    CHECK_EMPTY_FILE,
    CHECK_EMPTY_STATEMENT,
    CHECK_ENCODING,
    CHECK_MISSING_DOWN,
    CHECK_ON_CLUSTER,
    CHECK_ORPHAN_DOWN,
    CHECK_STANDALONE_SET,
    CHECK_UNTERMINATED,
    CHECK_VERSION_GAP,
    LEVEL_ERROR,
    LEVEL_WARNING,
    Finding,
    ValidationReport,
    destructive_operations,
    format_validate,
    format_validate_json,
    validate_exit_code,
    validate_migrations,
)

FIXTURES = Path(__file__).parent / "validate"


def _findings(name, **kwargs):
    return validate_migrations(FIXTURES / name, **kwargs).findings


def _summary(findings):
    return [(f.file, f.line, f.level, f.check) for f in findings]


# --- tokenizer extension -------------------------------------------------


@pytest.mark.parametrize(
    "script",
    [
        "",
        "SELECT 1; ;; SELECT 2",
        "SELECT 'a;b'; -- c;\nSELECT `x;y`; /* ; */ SELECT \"q;\"",
        "CREATE TABLE t (x Int8);\n-- trailing\n",
        (FIXTURES / "valid" / "001_create_events.sql").read_text(encoding="utf8"),
    ],
)
def test_split_statement_tokens_matches_script_to_statements(script):
    joined = [
        "".join(t.text for t in tokens).strip() + ";"
        for tokens in split_statement_tokens(script)
    ]

    assert joined == Migrator.script_to_statements(script, True)


def test_statement_tokens_keep_their_offsets():
    script = "SELECT 1;\n  SELECT 'x'"
    tokens = split_statement_tokens(script)[1]

    assert all(script[t.offset : t.offset + len(t.text)] == t.text for t in tokens)
    assert [t.kind for t in tokens] == ["other", "single"]


@pytest.mark.parametrize(
    "script,offset,text",
    [
        ("SELECT 'abc", 7, "'"),
        ('SELECT "abc', 7, '"'),
        ("SELECT `abc", 7, "`"),
        ("SELECT 1 /* abc", 9, "/"),
        ("SELECT 'abc\\'", 7, "'abc\\'"),
        ('SELECT "abc\\"', 7, '"abc\\"'),
    ],
)
def test_find_unterminated_token(script, offset, text):
    token = find_unterminated_token(script)

    assert (token.offset, token.text) == (offset, text)


@pytest.mark.parametrize(
    "script",
    [
        "SELECT 'it''s', 'a\\'b', 'c\\\\', \"x\"\"y\", `a``b` /* ' */ -- '\n",
        "SELECT 1 / 2",
        "",
    ],
)
def test_find_unterminated_token_accepts_closed_tokens(script):
    assert find_unterminated_token(script) is None


# --- destructive detection -----------------------------------------------


@pytest.mark.parametrize(
    "code,expected",
    [
        ("DROP TABLE a", ["DROP TABLE"]),
        ("drop temporary table a", ["DROP TABLE"]),
        ("DROP DATABASE IF EXISTS d", ["DROP DATABASE"]),
        ("DROP DICTIONARY d", ["DROP DICTIONARY"]),
        ("DROP VIEW v", ["DROP VIEW"]),
        ("TRUNCATE a", ["TRUNCATE"]),
        ("DELETE FROM a WHERE 1", ["DELETE FROM"]),
        ("ALTER TABLE a DELETE WHERE 1", ["ALTER ... DELETE"]),
        ("ALTER TABLE db.a ON CLUSTER c UPDATE x = 1 WHERE 1", ["ALTER ... UPDATE"]),
        (
            "ALTER TABLE a UPDATE x = 1 WHERE 1, DELETE WHERE 2, DROP COLUMN y",
            ["ALTER ... DELETE", "ALTER ... UPDATE", "DROP COLUMN"],
        ),
        ("ALTER TABLE _ DELETE WHERE 1", ["ALTER ... DELETE"]),
        ("ALTER TABLE a MODIFY TTL d + INTERVAL 1 DAY DELETE", []),
        ("ALTER TABLE a DROP PARTITION 1", []),
        ("SELECT truncate(1.5)", []),
        ("CREATE TABLE a (x Int8) TTL d DELETE", []),
    ],
)
def test_destructive_operations(code, expected):
    assert destructive_operations(code) == expected


# --- one fixture directory per failure mode -------------------------------


def test_valid_directory_has_no_findings():
    report = validate_migrations(FIXTURES / "valid")

    # README.md is not a *.sql file, so it is neither counted nor reported.
    assert report.files == 4
    assert not report.findings
    assert validate_exit_code(report, strict=True) == 0


def test_bad_filename():
    assert _summary(_findings("bad_filename")) == [
        ("2_.sql", None, LEVEL_ERROR, CHECK_BAD_FILENAME),
        ("init.sql", None, LEVEL_ERROR, CHECK_BAD_FILENAME),
        ("003.down.sql", None, LEVEL_ERROR, CHECK_BAD_FILENAME),
    ]


def test_duplicate_version_reports_every_file_of_the_group():
    findings = _findings("duplicate_version")

    assert _summary(findings) == [
        ("001_first.sql", None, LEVEL_ERROR, CHECK_DUPLICATE_VERSION),
        ("1_second.sql", None, LEVEL_ERROR, CHECK_DUPLICATE_VERSION),
    ]
    assert "1_second.sql" in findings[0].message


def test_duplicate_down_version(tmp_path):
    for name in ("001_a.sql", "001_a.down.sql", "1_b.down.sql"):
        (tmp_path / name).write_text("SELECT 1;", encoding="utf8")

    assert _summary(validate_migrations(tmp_path).findings) == [
        ("001_a.down.sql", None, LEVEL_ERROR, CHECK_DUPLICATE_VERSION),
        ("1_b.down.sql", None, LEVEL_ERROR, CHECK_DUPLICATE_VERSION),
    ]


def test_orphan_down():
    findings = _findings("orphan_down")

    assert _summary(findings) == [
        ("002_create_b.down.sql", None, LEVEL_ERROR, CHECK_ORPHAN_DOWN),
    ]


def test_empty_file():
    assert _summary(_findings("empty_file")) == [
        ("002_blank.sql", None, LEVEL_ERROR, CHECK_EMPTY_FILE),
        ("003_scaffold_only.sql", None, LEVEL_ERROR, CHECK_EMPTY_FILE),
        ("004_block_comment_only.sql", None, LEVEL_ERROR, CHECK_EMPTY_FILE),
    ]


def test_empty_statement():
    assert _summary(_findings("empty_statement")) == [
        ("001_trailing_comment.sql", 2, LEVEL_ERROR, CHECK_EMPTY_STATEMENT),
    ]


def test_unterminated():
    findings = _findings("unterminated")

    assert _summary(findings) == [
        ("001_string.sql", 1, LEVEL_ERROR, CHECK_UNTERMINATED),
        ("002_backtick.sql", 3, LEVEL_ERROR, CHECK_UNTERMINATED),
        ("003_double_quote.sql", 2, LEVEL_ERROR, CHECK_UNTERMINATED),
        ("004_block_comment.sql", 2, LEVEL_ERROR, CHECK_UNTERMINATED),
        ("005_escaped_quote.sql", 1, LEVEL_ERROR, CHECK_UNTERMINATED),
    ]
    assert findings[0].message.startswith("string literal is never closed")
    assert findings[1].message.startswith("quoted identifier is never closed")
    assert findings[3].message.startswith("block comment is never closed")
    assert "escaped quote" in findings[4].message


def test_encoding():
    assert _summary(_findings("encoding")) == [
        ("002_latin1.sql", None, LEVEL_ERROR, CHECK_ENCODING),
    ]


def test_version_gap():
    findings = _findings("version_gap")

    assert _summary(findings) == [
        ("005_m.sql", None, LEVEL_WARNING, CHECK_VERSION_GAP),
        ("007_m.sql", None, LEVEL_WARNING, CHECK_VERSION_GAP),
    ]
    assert findings[0].message == "versions 3-4 are missing before this file"
    assert findings[1].message == "version 6 is missing before this file"


def test_destructive_ignores_strings_comments_and_down_files():
    findings = _findings("destructive")

    # Statements 10-14 only mention destructive keywords in strings, comments
    # or quoted names (or are a TTL DELETE); the .down.sql files are expected
    # to be destructive and are never reported.
    assert _summary(findings) == [
        ("002_destroy.sql", line, LEVEL_WARNING, CHECK_DESTRUCTIVE)
        for line in range(1, 10)
    ]
    assert findings[7].message == "destructive statement (DROP COLUMN)"


def test_missing_down_is_a_warning_once_down_files_are_used():
    assert _summary(_findings("missing_down")) == [
        ("002_create_b.sql", None, LEVEL_WARNING, CHECK_MISSING_DOWN),
    ]


def test_missing_down_is_not_reported_without_any_down_file():
    checks = {f.check for f in _findings("standalone_set")}

    assert CHECK_MISSING_DOWN not in checks


def test_require_down_promotes_missing_down_to_error():
    findings = _findings("standalone_set", require_down=True)

    missing = [f for f in findings if f.check == CHECK_MISSING_DOWN]
    assert [f.file for f in missing] == [
        "001_set_then_create.sql",
        "002_only_set.sql",
        "003_not_standalone.sql",
    ]
    assert {f.level for f in missing} == {LEVEL_ERROR}


def test_standalone_set_only_in_multi_statement_files():
    # 002 is a single SET (nothing to carry over to); 003 only has SETTINGS
    # clauses and SET inside a string or a comment.
    assert _summary(_findings("standalone_set")) == [
        ("001_set_then_create.sql", 1, LEVEL_WARNING, CHECK_STANDALONE_SET),
    ]


def test_on_cluster_flags_the_files_without_it():
    findings = _findings("on_cluster")

    # 003 mentions ON CLUSTER only in a comment and a string; 004 has no DDL.
    assert _summary(findings) == [
        ("003_c_forgotten.sql", None, LEVEL_WARNING, CHECK_ON_CLUSTER),
    ]
    assert findings[0].message == (
        "has no ON CLUSTER, while 3 of 4 DDL migrations use it"
    )


def test_on_cluster_flags_the_minority_that_uses_it(tmp_path):
    ddl = "CREATE TABLE {} (id UInt32) ENGINE = Memory;"
    (tmp_path / "001_a.sql").write_text(ddl.format("a"), encoding="utf8")
    (tmp_path / "002_b.sql").write_text(ddl.format("b"), encoding="utf8")
    (tmp_path / "003_c.sql").write_text(ddl.format("c ON CLUSTER x"), encoding="utf8")

    findings = validate_migrations(tmp_path).findings

    assert _summary(findings) == [
        ("003_c.sql", None, LEVEL_WARNING, CHECK_ON_CLUSTER),
    ]
    assert findings[0].message == (
        "uses ON CLUSTER, while 2 of 3 DDL migrations do not"
    )


def test_directories_and_non_sql_files_are_ignored(tmp_path):
    (tmp_path / "001_a.sql").write_text("SELECT 1;", encoding="utf8")
    (tmp_path / "002_dir.sql").mkdir()
    (tmp_path / "notes.txt").write_text("", encoding="utf8")

    report = validate_migrations(tmp_path)

    assert report.files == 1
    assert not report.findings


def test_empty_directory_is_valid(tmp_path):
    report = validate_migrations(tmp_path)

    assert (report.files, report.findings) == (0, [])


def test_missing_directory_raises(tmp_path):
    with pytest.raises(MigrationException, match="does not exist"):
        validate_migrations(tmp_path / "nope")


# --- exit codes and rendering ---------------------------------------------

_WARNING = Finding("002_b.sql", None, LEVEL_WARNING, CHECK_VERSION_GAP, "gap")
_ERROR = Finding("001_a.sql", 3, LEVEL_ERROR, CHECK_UNTERMINATED, "boom")


@pytest.mark.parametrize(
    "findings,strict,expected",
    [
        ([], False, 0),
        ([], True, 0),
        ([_WARNING], False, 0),
        ([_WARNING], True, 1),
        ([_ERROR], False, 1),
        ([_ERROR, _WARNING], True, 1),
    ],
)
def test_validate_exit_code(findings, strict, expected):
    report = ValidationReport("m", 2, findings)

    assert validate_exit_code(report, strict) == expected


def test_format_validate_table():
    text = format_validate(ValidationReport("m", 2, [_ERROR, _WARNING]))

    assert text.splitlines() == [
        "FILE         LEVEL    CHECK         MESSAGE",
        "001_a.sql:3  error    unterminated  boom",
        "002_b.sql    warning  version-gap   gap",
        "",
        "1 error(s), 1 warning(s) in 2 migration file(s) in m.",
    ]


def test_format_validate_without_findings():
    text = format_validate(ValidationReport("m", 2, []))

    assert text == "No problems found in 2 migration file(s) in m."


def test_format_validate_json_has_stable_keys():
    data = json.loads(format_validate_json(ValidationReport("m", 2, [_ERROR])))

    assert data == {
        "migrations_dir": "m",
        "files": 2,
        "errors": 1,
        "warnings": 0,
        "findings": [
            {
                "file": "001_a.sql",
                "line": 3,
                "level": "error",
                "check": "unterminated",
                "message": "boom",
            }
        ],
    }


# --- command line ---------------------------------------------------------


def test_validate_is_a_subcommand():
    assert "validate" in SUBCOMMANDS


def test_validate_parses_without_db_arguments(tmp_path):
    ctx = get_context(["validate", "--dir", str(tmp_path)])

    assert ctx.command == "validate"
    assert ctx.migrations_dir == tmp_path
    assert (ctx.strict, ctx.require_down, ctx.format) == (False, False, "table")
    assert not hasattr(ctx, "db_host")


def test_validate_rejects_db_arguments():
    with pytest.raises(SystemExit):
        get_context(["validate", "--db-host", "localhost"])


def test_validate_migrations_dir_alias_and_env(monkeypatch, tmp_path):
    ctx = get_context(["validate", "--migrations-dir", str(tmp_path)])
    assert ctx.migrations_dir == tmp_path

    monkeypatch.setenv("MIGRATIONS_DIR", str(tmp_path / "env"))
    assert get_context(["validate"]).migrations_dir == tmp_path / "env"


def _run(monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", "validate", *argv])
    return main()


@pytest.mark.parametrize(
    "fixture,flags,expected",
    [
        ("valid", [], 0),
        ("valid", ["--strict", "--require-down"], 0),
        ("version_gap", [], 0),
        ("version_gap", ["--strict"], 1),
        ("missing_down", ["--no-strict"], 0),
        ("missing_down", ["--require-down"], 1),
        ("unterminated", [], 1),
        ("bad_filename", [], 1),
    ],
)
def test_main_validate_exit_codes(monkeypatch, fixture, flags, expected):
    assert _run(monkeypatch, "--dir", str(FIXTURES / fixture), *flags) == expected


def test_main_validate_table_output(monkeypatch, capsys):
    assert _run(monkeypatch, "--dir", str(FIXTURES / "version_gap")) == 0

    out = capsys.readouterr().out
    assert out.splitlines()[0].split() == ["FILE", "LEVEL", "CHECK", "MESSAGE"]
    assert "005_m.sql  warning  version-gap" in out


def test_main_validate_json_stdout_is_only_json(monkeypatch, capsys):
    exit_code = _run(
        monkeypatch, "--dir", str(FIXTURES / "orphan_down"), "--format", "json"
    )

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert exit_code == 1
    assert (data["errors"], data["warnings"]) == (1, 0)
    assert data["findings"][0]["check"] == CHECK_ORPHAN_DOWN
    assert captured.err == ""


def test_main_validate_missing_dir(monkeypatch, capsys, caplog, tmp_path):
    exit_code = _run(monkeypatch, "--dir", str(tmp_path / "nope"), "--format", "json")

    assert exit_code == 1
    assert capsys.readouterr().out == ""
    assert "Validation failed" in caplog.text
    assert "does not exist" in caplog.text
