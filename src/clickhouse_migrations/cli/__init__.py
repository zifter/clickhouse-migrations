"""The ``clickhouse-migrations`` command line.

One module per subcommand (its arguments and its handler), plus the pieces
they share: ``argtypes`` (argument value types, env-var booleans), ``common``
(connection and transport options, ``create_cluster``), ``options`` (lock,
substitution and output-format options), ``render`` (status table and JSON)
and ``app`` (the parser, ``get_context()`` and ``main()``).

``clickhouse_migrations.command_line`` re-exports all of it, so existing
imports and the console script keep working.
"""
