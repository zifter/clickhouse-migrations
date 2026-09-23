import pytest

from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import MigrationStorage
from clickhouse_migrations.substitution import (
    is_valid_name,
    parse_assignment,
    resolve_variables,
    substitute,
)

SECRET = "s3cr3t-value"


def _sub(text, **variables):
    return substitute(text, variables, "001_init.sql")


def test_replaces_placeholder():
    assert _sub("ON CLUSTER ${CLUSTER}", CLUSTER="prod") == "ON CLUSTER prod"


def test_text_without_placeholders_is_unchanged():
    text = "SELECT 1;\n-- nothing here\n"
    assert _sub(text) == text


def test_same_placeholder_twice():
    assert _sub("${A}-${A}", A="x") == "x-x"


def test_adjacent_placeholders():
    assert _sub("${A}${B}${A}", A="1", B="2") == "121"


def test_empty_value_is_allowed():
    assert _sub("'${A}'", A="") == "''"


def test_value_is_not_rescanned():
    # A value that looks like a placeholder is inserted as is.
    assert _sub("${A}", A="${B}") == "${B}"


def test_value_is_not_escaped():
    assert _sub("'${A}'", A="it's") == "'it's'"


def test_escape_gives_literal_placeholder():
    assert _sub("$${NAME}") == "${NAME}"


def test_escape_next_to_placeholder():
    assert _sub("$${A}=${A}", A="1") == "${A}=1"


def test_escape_does_not_validate_the_name():
    assert _sub("$${PG-HOST}") == "${PG-HOST}"


def test_escape_with_placeholder_inside():
    assert _sub("$${${A}}", A="x") == "${x}"


def test_dollar_alone_is_literal():
    assert _sub("SELECT '$', '$5', 'a$b' $") == "SELECT '$', '$5', 'a$b' $"


def test_double_dollar_not_followed_by_brace_is_literal():
    assert _sub("$$ $$a $$$") == "$$ $$a $$$"


def test_dollar_before_escape():
    # "$$${X}": the "$${" escape wins, the first "$" stays.
    assert _sub("$$${X}") == "$${X}"


def test_dollar_before_placeholder():
    assert _sub("$${A} $${A}", A="1") == "${A} ${A}"
    assert _sub("x$${A}", A="1") == "x${A}"


def test_bare_braces_are_untouched():
    assert _sub("{A} {{A}} $ {A}") == "{A} {{A}} $ {A}"


def test_unicode_text_and_value():
    assert _sub("-- привет ${A} ☃", A="значение ✓") == "-- привет значение ✓ ☃"


def test_multi_line():
    text = "CREATE TABLE t ON CLUSTER ${C}\n(\n  x String DEFAULT '${D}'\n);\n"
    assert _sub(text, C="c1", D="d") == (
        "CREATE TABLE t ON CLUSTER c1\n(\n  x String DEFAULT 'd'\n);\n"
    )


def test_underscore_and_digits_in_name():
    assert _sub("${_A1} ${a_b_2}", _A1="x", a_b_2="y") == "x y"


def test_unset_variable_fails():
    with pytest.raises(MigrationException) as err:
        _sub("SELECT 1;\nON CLUSTER ${CLUSTER_NAME}")

    message = str(err.value)
    assert "001_init.sql" in message
    assert "CLUSTER_NAME (line 2)" in message
    assert "--var" in message


def test_all_unset_variables_reported_once():
    with pytest.raises(MigrationException) as err:
        _sub("${A}\n${B}\n${A}\n${C}", C="set")

    assert "A (line 1), B (line 2)." in str(err.value)
    assert "C (" not in str(err.value)


def test_unset_error_does_not_contain_values():
    with pytest.raises(MigrationException) as err:
        _sub("${PASSWORD} ${MISSING}", PASSWORD=SECRET)

    assert SECRET not in str(err.value)
    assert "MISSING" in str(err.value)


@pytest.mark.parametrize(
    "text, placeholder",
    [
        ("${PG-HOST}", "${PG-HOST}"),
        ("${}", "${}"),
        ("${1A}", "${1A}"),
        ("${A B}", "${A B}"),
        ("${ A}", "${ A}"),
        ("${ÄÖ}", "${ÄÖ}"),
        ("${A${B}}", "${A${B}"),
        ("${" + "X" * 50 + "-}", "${" + "X" * 38 + "..."),
    ],
)
def test_malformed_placeholder_fails(text, placeholder):
    with pytest.raises(MigrationException) as err:
        _sub("SELECT 1;\n" + text, A="a", B="b")

    message = str(err.value)
    assert message.startswith("001_init.sql:2: malformed placeholder")
    assert repr(placeholder) in message


@pytest.mark.parametrize(
    "text, placeholder",
    [
        ("${PG_HOST", "${PG_HOST"),
        ("${PG_HOST\n}", "${PG_HOST"),
        ("${", "${"),
        ("x ${A trailing", "${A trailing"),
    ],
)
def test_unterminated_placeholder_fails(text, placeholder):
    with pytest.raises(MigrationException) as err:
        _sub(text)

    message = str(err.value)
    assert message.startswith("001_init.sql:1: unterminated placeholder")
    assert repr(placeholder) in message


def test_syntax_error_wins_over_unset():
    with pytest.raises(MigrationException, match="malformed"):
        _sub("${UNSET} ${BAD-NAME}")


def test_is_valid_name():
    assert is_valid_name("A")
    assert is_valid_name("_a_1")
    assert not is_valid_name("")
    assert not is_valid_name("1A")
    assert not is_valid_name("A-B")
    assert not is_valid_name("A\n")


def test_parse_assignment():
    assert parse_assignment("A=1") == ("A", "1")
    assert parse_assignment("A=") == ("A", "")
    assert parse_assignment("A=b=c") == ("A", "b=c")
    assert parse_assignment("A=x,y") == ("A", "x,y")


def test_parse_assignment_without_equals_hides_value():
    with pytest.raises(ValueError) as err:
        parse_assignment(SECRET)

    assert "NAME=VALUE" in str(err.value)
    assert SECRET not in str(err.value)


def test_parse_assignment_bad_name_hides_value():
    with pytest.raises(ValueError) as err:
        parse_assignment(f"PG-HOST={SECRET}")

    assert "'PG-HOST'" in str(err.value)
    assert SECRET not in str(err.value)


def test_resolve_disabled_by_default():
    assert resolve_variables() is None
    assert resolve_variables(None, False, {"A": "1"}) is None


def test_resolve_vars_alone_ignore_environment():
    assert resolve_variables({"A": "1"}, environ={"B": "2"}) == {"A": "1"}


def test_resolve_empty_vars_still_enable_substitution():
    assert resolve_variables({}) == {}


def test_resolve_environment():
    assert resolve_variables(None, True, {"A": "env"}) == {"A": "env"}


def test_resolve_vars_win_over_environment():
    assert resolve_variables({"A": "var"}, True, {"A": "env", "B": "env"}) == {
        "A": "var",
        "B": "env",
    }


def test_resolve_uses_process_environment(monkeypatch):
    monkeypatch.setenv("CHM_TEST_SUBSTITUTION_VAR", "from-env")

    resolved = resolve_variables(substitute_env=True)

    assert resolved["CHM_TEST_SUBSTITUTION_VAR"] == "from-env"


def test_resolve_rejects_invalid_name():
    with pytest.raises(MigrationException, match="Invalid variable name 'A-B'"):
        resolve_variables({"A-B": SECRET})


def test_resolve_rejects_non_string_value():
    with pytest.raises(MigrationException) as err:
        resolve_variables({"PORT": 5432})

    assert "PORT must be a string, got int" in str(err.value)
    assert "5432" not in str(err.value)


def test_storage_file_names_by_version(tmp_path):
    for name in ("001_a.sql", "001_a.down.sql", "02_b.sql", "notes.txt"):
        (tmp_path / name).write_text("SELECT 1;", encoding="utf8")

    storage = MigrationStorage(tmp_path)

    assert storage.migration_filenames() == {1: "001_a.sql", 2: "02_b.sql"}
    assert storage.down_filenames() == {1: "001_a.down.sql"}
