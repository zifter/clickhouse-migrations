def quote_identifier(identifier: str) -> str:
    """Quote a ClickHouse identifier (database, cluster name, ...) safely.

    Double quotes delimit the identifier and any embedded double quote is
    escaped by doubling it, so a name can never break out of the quoting.
    """
    return '"' + identifier.replace('"', '""') + '"'
