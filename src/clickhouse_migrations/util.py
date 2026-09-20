from typing import Optional, Tuple

from clickhouse_migrations.exceptions import MigrationException


def quote_identifier(identifier: str) -> str:
    """Quote a ClickHouse identifier (database, cluster name, ...) safely.

    Double quotes delimit the identifier and any embedded double quote is
    escaped by doubling it, so a name can never break out of the quoting.
    """
    return '"' + identifier.replace('"', '""') + '"'


def quote_string(value: str) -> str:
    """Quote a string literal for use in ClickHouse SQL.

    Backslashes and single quotes are escaped so the value cannot break out
    of the surrounding quotes.
    """
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _last_unquoted_dot(reference: str) -> Optional[int]:
    """Index of the last dot of ``reference`` that is not inside quotes.

    Both double-quoted and backtick-quoted identifiers are recognised; a quote
    character doubled inside such an identifier is an escaped quote and does
    not end it.
    """
    last: Optional[int] = None
    quote: Optional[str] = None
    index = 0
    while index < len(reference):
        char = reference[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(reference) and reference[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif char in ('"', "`"):
            quote = char
        elif char == ".":
            last = index
        index += 1

    return last


def _unquote_identifier(part: str) -> str:
    """Strip the optional quoting of a single identifier written by the user."""
    if len(part) >= 2 and part[0] == part[-1] and part[0] in ('"', "`"):
        return part[1:-1].replace(part[0] * 2, part[0])

    return part


def split_table_reference(reference: str) -> Tuple[Optional[str], str]:
    """Split a ``table`` or ``database.table`` reference into its two parts.

    The split happens on the last dot that is not inside a quoted identifier,
    so ``"my.db".events`` is the table ``events`` of the database ``my.db``.
    A bare name yields ``None`` as the database, meaning "the migrated one".
    """
    index = _last_unquoted_dot(reference)
    if index is None:
        database, table = None, _unquote_identifier(reference)
    else:
        database = _unquote_identifier(reference[:index])
        table = _unquote_identifier(reference[index + 1 :])

    if not table or (index is not None and not database):
        raise MigrationException(
            f"Invalid table name: {reference!r}. "
            "Expected 'table' or 'database.table'."
        )

    return database, table


def format_table_reference(database: Optional[str], table: str) -> str:
    """Render a (database, table) pair as a quoted, fully qualified name."""
    if database is None:
        return quote_identifier(table)

    return f"{quote_identifier(database)}.{quote_identifier(table)}"
