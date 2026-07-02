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
