"""Pure helpers behind ``clickhouse-migrations diff``.

``diff`` never parses ClickHouse DDL itself beyond the statement header: the
target schema file is replayed into a throwaway scratch database and the
server's own view of both databases (``system.tables``, ``system.columns``,
``system.data_skipping_indices`` and ``SHOW CREATE``) is compared. This module
holds everything that does not talk to ClickHouse:

* ``prepare_target`` checks the schema file and rewrites it for the scratch
  database (``ON CLUSTER`` removed, ``Replicated*MergeTree`` made local);
* ``build_object`` turns the rows of one object into a comparable model;
* ``diff_schemas`` compares two models and returns a ``SchemaDiff`` with the
  generated statements, the refused changes and notes;
* ``SchemaDiff.render`` produces the migration text.

The database-facing part lives in ``ClickhouseCluster.diff``.
"""

import re
from pathlib import Path
from typing import Dict, Iterable, List, NamedTuple, Optional, Sequence, Set, Tuple

from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import MigrationStorage
from clickhouse_migrations.migrator import Migrator
from clickhouse_migrations.schema_dump import (
    KIND_QUOTED,
    KIND_SPACE,
    KIND_WORD,
    Token,
    _find_engine,
    _is_punct,
    _is_word,
    _join,
    _parse_call,
    _rewrite_engine,
    _strip_database,
    identifier_value,
    normalize_statement,
    tokenize,
)
from clickhouse_migrations.util import quote_string

KIND_TABLE = "table"
KIND_VIEW = "view"
KIND_MATERIALIZED_VIEW = "materialized view"
KIND_DICTIONARY = "dictionary"

# system.tables engine -> object kind; everything else is a table.
_ENGINE_KINDS = {
    "View": KIND_VIEW,
    "MaterializedView": KIND_MATERIALIZED_VIEW,
    "Dictionary": KIND_DICTIONARY,
}
_DROP_KEYWORD = {
    KIND_TABLE: "TABLE",
    KIND_VIEW: "VIEW",
    KIND_MATERIALIZED_VIEW: "VIEW",
    KIND_DICTIONARY: "DICTIONARY",
}
_REPLICATED_PREFIX_RE = re.compile(r"^(Replicated|Shared)(\w*MergeTree)$")

# Clauses of system.tables.engine_full, in the order the server prints them.
_KEY_CLAUSES = (
    ("ORDER BY", "sorting_key"),
    ("PARTITION BY", "partition_key"),
    ("PRIMARY KEY", "primary_key"),
    ("SAMPLE BY", "sampling_key"),
)

DESTRUCTIVE_HINT = (
    "Destructive statements are commented out: review them and uncomment, "
    "or rerun with --allow-destructive."
)
REBUILD_HINT = (
    "ClickHouse cannot change this in place: create a new table with the "
    "target definition, copy the data (INSERT ... SELECT) and swap the tables "
    "(EXCHANGE TABLES / RENAME), by hand."
)


class TargetStatement(NamedTuple):
    """One ``CREATE`` of the schema file, ready for the scratch database."""

    name: str
    kind: str
    sql: str
    engine_prefix: str  # "Replicated" / "Shared" when the file said so, else ""


class Column(NamedTuple):
    name: str
    type: str
    default_kind: str = ""
    default_expression: str = ""
    codec: str = ""
    comment: str = ""
    definition: str = ""  # the column as SHOW CREATE prints it


class Index(NamedTuple):
    name: str
    expr: str
    type_full: str
    granularity: int

    def definition(self) -> str:
        return (
            f"INDEX {quote_name(self.name)} {self.expr} TYPE {self.type_full} "
            f"GRANULARITY {self.granularity}"
        )


class DbObject(NamedTuple):  # pylint: disable=too-many-instance-attributes
    """The comparable model of one table / view / dictionary."""

    name: str
    kind: str
    text: str  # normalised SHOW CREATE (dump style)
    engine: str = ""  # engine family with its arguments, e.g. ReplacingMergeTree(ver)
    engine_prefix: str = ""
    sorting_key: str = ""
    partition_key: str = ""
    primary_key: str = ""
    sampling_key: str = ""
    ttl: str = ""
    settings: str = ""
    comment: str = ""
    columns: Tuple[Column, ...] = ()
    indices: Tuple[Index, ...] = ()
    other_elements: Tuple[str, ...] = ()  # projections, constraints, ...
    references: frozenset = frozenset()


class Statement(NamedTuple):
    sql: str
    destructive: bool = False


class Change(NamedTuple):
    """What happens to one object: a summary line and its statements."""

    name: str
    summary: str
    statements: Tuple[Statement, ...]


class SchemaDiff(NamedTuple):
    changes: Tuple[Change, ...] = ()
    refusals: Tuple[str, ...] = ()
    notes: Tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.changes and not self.refusals

    @property
    def has_destructive(self) -> bool:
        return any(s.destructive for c in self.changes for s in c.statements)

    def render(self, allow_destructive: bool = False) -> str:
        """The migration text: a comment header, then ``;`` terminated statements.

        Empty when there is nothing to change. Destructive statements are
        commented out unless ``allow_destructive``. Refused changes appear in
        the header only, never as SQL.
        """
        if self.is_empty:
            return ""

        header = [
            "Generated by `clickhouse-migrations diff`: brings the database to the "
            "target schema.",
            "Review (and edit) it before applying: this is a proposal, not a "
            "verified plan.",
        ]
        if self.changes:
            header += ["", "Changes:"]
            header += [f"  * {change.summary}" for change in self.changes]
        if self.has_destructive:
            header += [
                "",
                (
                    "Destructive statements are included (--allow-destructive)."
                    if allow_destructive
                    else DESTRUCTIVE_HINT
                ),
            ]
        if self.refusals:
            header += ["", "Refused (no SQL generated for these, write them by hand):"]
            header += [f"  * {refusal}" for refusal in self.refusals]
        if self.notes:
            header += ["", "Notes:"]
            header += [f"  * {note}" for note in self.notes]

        parts = ["\n".join(_comment(line) for line in header)]
        for change in self.changes:
            for statement in change.statements:
                text = statement.sql + ";"
                if statement.destructive and not allow_destructive:
                    text = "\n".join(_comment(line) for line in text.splitlines())
                parts.append(text)
        return "\n\n".join(parts) + "\n"


def _comment(line: str) -> str:
    line = " ".join(line.splitlines())
    return f"-- {line}".rstrip()


def quote_name(name: str) -> str:
    """A backtick quoted identifier (the style SHOW CREATE uses for columns)."""
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def write_diff_migration(
    migrations_dir, sql: str, name: str = "diff", version: Optional[int] = None
) -> Path:
    """Write ``sql`` as the next numbered migration (``NNN_<name>.sql``)."""
    return MigrationStorage(migrations_dir).create(name, version=version, body=sql)[0]


# --- the schema file -------------------------------------------------------


def _header_error(number: int, statement: str, problem: str) -> MigrationException:
    first_line = next(
        (
            line.strip()
            for line in statement.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ),
        "",
    )
    return MigrationException(
        f"Statement {number} of the schema file {problem}: {first_line[:120]}"
    )


_HEADER_KINDS = (
    (("TABLE",), KIND_TABLE),
    (("VIEW",), KIND_VIEW),
    (("MATERIALIZED", "VIEW"), KIND_MATERIALIZED_VIEW),
    (("DICTIONARY",), KIND_DICTIONARY),
)


def _parse_header(tokens: List[Token]):
    """``CREATE [OR REPLACE] <kind> [IF NOT EXISTS] <name> [ON CLUSTER c]``.

    Returns ``(kind, name_index, cluster_span)`` or ``None`` for anything else.
    ``name_index`` is ``-1`` for a qualified ``db.name``; ``cluster_span`` is
    the token range of `` ON CLUSTER c`` (or ``None``).
    """
    significant = [i for i, token in enumerate(tokens) if token.kind != KIND_SPACE]
    words = [
        tokens[i].text.upper() if tokens[i].kind == KIND_WORD else None
        for i in significant
    ]
    if words[:1] != ["CREATE"]:
        return None
    position = 1
    if words[1:3] == ["OR", "REPLACE"]:
        position = 3
    for keywords, kind in _HEADER_KINDS:
        if tuple(words[position : position + len(keywords)]) == keywords:
            position += len(keywords)
            break
    else:
        return None
    if words[position : position + 3] == ["IF", "NOT", "EXISTS"]:
        position += 3
    if position >= len(significant) or (
        identifier_value(tokens[significant[position]]) is None
    ):
        return None

    name_index = significant[position]
    rest = significant[position + 1 :]
    if rest and _is_punct(tokens[rest[0]], "."):
        return kind, -1, None
    cluster = None
    if words[position + 1 : position + 3] == ["ON", "CLUSTER"] and len(rest) > 2:
        cluster = (name_index + 1, rest[2] + 1)
    return kind, name_index, cluster


def _local_engine(tokens: List[Token]) -> Tuple[List[Token], str]:
    """``Replicated*MergeTree(...)`` -> ``*MergeTree(...)`` for the scratch copy.

    The ZooKeeper path / replica arguments go first (as in ``dump``), then the
    prefix. A local table of the same family has the same columns, keys and
    settings, which is all ``diff`` compares; the engine family itself is
    compared separately.
    """
    engine = _find_engine(tokens)
    if engine is None:
        return tokens, ""
    match = _REPLICATED_PREFIX_RE.match(engine.name)
    if not match:
        return tokens, ""
    tokens = _rewrite_engine(tokens, "", keep_replicated_paths=False)
    engine = _find_engine(tokens)
    tokens = list(tokens)
    tokens[engine.name_index] = Token(KIND_WORD, match.group(2))
    return tokens, match.group(1)


def prepare_target(schema_sql: str) -> List[TargetStatement]:
    """Split and check the schema file, rewritten for the scratch database.

    Only ``CREATE TABLE / VIEW / MATERIALIZED VIEW / DICTIONARY`` statements
    with database-less names are accepted (what ``dump`` writes), so replaying
    the file can never touch another database. ``ON CLUSTER`` is removed: the
    scratch database exists on the connected server only.
    """
    prepared: List[TargetStatement] = []
    seen: Set[str] = set()
    for number, statement in enumerate(
        Migrator.script_to_statements(schema_sql, True), start=1
    ):
        tokens = tokenize(statement.rstrip().rstrip(";").rstrip())
        header = _parse_header(tokens)
        if header is None:
            raise _header_error(
                number,
                statement,
                "is not supported (only CREATE TABLE, VIEW, MATERIALIZED VIEW "
                "and DICTIONARY statements are allowed)",
            )
        kind, name_index, cluster = header
        if name_index < 0:
            raise _header_error(
                number,
                statement,
                "uses a database-qualified name (write names without a "
                "database, as `dump` does)",
            )
        name = identifier_value(tokens[name_index])
        if name in seen:
            raise _header_error(number, statement, f"defines {name!r} twice")
        seen.add(name)
        if cluster is not None:
            tokens = tokens[: cluster[0]] + tokens[cluster[1] :]
        tokens, prefix = _local_engine(tokens)
        prepared.append(TargetStatement(name, kind, _join(tokens), prefix))
    return prepared


# --- the model ------------------------------------------------------------


def object_kind(engine: str) -> str:
    return _ENGINE_KINDS.get(engine, KIND_TABLE)


def strip_database(text: str, db_name: str, known: Set[str]) -> str:
    """``db.object`` -> ``object`` for the objects of ``db_name`` (token based)."""
    tokens, _ = _strip_database(tokenize(text), db_name, known)
    return _join(tokens)


def _elements(tokens: List[Token]) -> List[str]:
    """The comma separated items of the first top-level ``( ... )`` of a CREATE."""
    for index, token in enumerate(tokens):
        if _is_word(token, "AS") or _is_word(token, "ENGINE"):
            return []
        if _is_punct(token, "("):
            call = _parse_call(tokens, index - 1)
            return [_join(tokens[span]) for span in call.args if span.stop > span.start]
    return []


def split_elements(text: str) -> Tuple[Dict[str, str], List[str], List[str]]:
    """Columns (by name), index and other element texts of a CREATE TABLE."""
    tokens = tokenize(text)
    columns: Dict[str, str] = {}
    indices: List[str] = []
    others: List[str] = []
    for element in _elements(tokens):
        first = tokenize(element)[0]
        if first.kind == KIND_QUOTED:
            columns[identifier_value(first)] = element
        elif _is_word(first, "INDEX"):
            indices.append(element)
        else:
            others.append(element)
    return columns, indices, others


def split_engine_prefix(text: str) -> Tuple[str, str]:
    """``(prefix, text)``: the ``Replicated`` / ``Shared`` engine prefix removed.

    ``diff`` compares engines by family (the scratch copy of the target is
    never replicated); the prefix is compared on its own.
    """
    tokens = tokenize(text)
    engine = _find_engine(tokens)
    match = engine and _REPLICATED_PREFIX_RE.match(engine.name)
    if not match:
        return "", text
    tokens[engine.name_index] = Token(KIND_WORD, match.group(2))
    return match.group(1), _join(tokens)


def _engine_clause(text: str) -> str:
    """The ENGINE with its arguments, e.g. ``ReplacingMergeTree(ver)``."""
    tokens = tokenize(text)
    engine = _find_engine(tokens)
    if engine is None:
        return ""
    return _join(tokens[engine.name_index : engine.close_index + 1])


def engine_full_clauses(engine_full: str) -> Dict[str, str]:
    """The ``TTL`` and ``SETTINGS`` clauses of ``system.tables.engine_full``."""
    tokens = tokenize(engine_full or "")
    starts = []
    depth = 0
    for index, token in enumerate(tokens):
        if _is_punct(token, "("):
            depth += 1
        elif _is_punct(token, ")"):
            depth -= 1
        elif depth == 0 and (_is_word(token, "TTL") or _is_word(token, "SETTINGS")):
            starts.append(index)
    clauses: Dict[str, str] = {}
    for position, start in enumerate(starts):
        stop = starts[position + 1] if position + 1 < len(starts) else len(tokens)
        clauses[tokens[start].text.upper()] = _join(tokens[start + 1 : stop]).strip()
    return clauses


def _columns(rows, definitions, db_name, known) -> Tuple[Column, ...]:
    return tuple(
        Column(
            name=row["name"],
            type=row["type"],
            default_kind=row.get("default_kind") or "",
            default_expression=strip_database(
                row.get("default_expression") or "", db_name, known
            ),
            codec=row.get("compression_codec") or "",
            comment=row.get("comment") or "",
            definition=definitions.get(row["name"], ""),
        )
        for row in sorted(rows, key=lambda r: int(r["position"]))
    )


def _indices(rows, db_name, known) -> Tuple[Index, ...]:
    return tuple(
        Index(
            row["name"],
            strip_database(row["expr"], db_name, known),
            row["type_full"],
            int(row["granularity"]),
        )
        for row in rows
    )


def build_object(
    table_row: Dict,
    create_text: str,
    column_rows: Sequence[Dict],
    index_rows: Sequence[Dict],
    db_name: str,
    known: Set[str],
) -> DbObject:
    """The model of one object from its ``system.*`` rows and ``SHOW CREATE``.

    Every text (the statement, defaults, index expressions) loses the
    qualifier of ``db_name`` exactly as ``dump`` does, so the models of two
    databases compare equal when their dumps do.
    """
    normalized = normalize_statement(create_text, db_name, known)
    prefix, text = split_engine_prefix(normalized.text)
    kind = object_kind(table_row["engine"])
    base = {
        "name": table_row["name"],
        "kind": kind,
        "text": text,
        "engine_prefix": prefix,
        "references": frozenset(normalized.references),
    }
    if kind != KIND_TABLE:
        return DbObject(**base)

    clauses = engine_full_clauses(table_row.get("engine_full", ""))
    definitions, _, others = split_elements(text)
    return DbObject(
        **base,
        engine=_engine_clause(text),
        sorting_key=table_row.get("sorting_key") or "",
        partition_key=table_row.get("partition_key") or "",
        primary_key=table_row.get("primary_key") or "",
        sampling_key=table_row.get("sampling_key") or "",
        ttl=strip_database(clauses.get("TTL", ""), db_name, known),
        settings=clauses.get("SETTINGS", ""),
        comment=table_row.get("comment") or "",
        columns=_columns(column_rows, definitions, db_name, known),
        indices=_indices(index_rows, db_name, known),
        other_elements=tuple(others),
    )


# --- the diff -------------------------------------------------------------


def _label(obj: DbObject) -> str:
    return f"{obj.kind} {quote_name(obj.name)}"


def _restore_prefix(text: str, prefix: str) -> str:
    """Put the ``Replicated`` / ``Shared`` prefix the file had back on the engine."""
    if not prefix:
        return text
    tokens = tokenize(text)
    engine = _find_engine(tokens)
    if engine is None:
        return text
    tokens[engine.name_index] = Token(KIND_WORD, prefix + engine.name)
    return _join(tokens)


def _replace_statement(text: str) -> str:
    """``CREATE VIEW ...`` -> ``CREATE OR REPLACE VIEW ...``."""
    return re.sub(r"^CREATE ", "CREATE OR REPLACE ", text, count=1)


def _split_select(text: str) -> Tuple[str, str]:
    """``(header, select)`` of a normalised CREATE [MATERIALIZED] VIEW."""
    tokens = tokenize(text)
    depth = 0
    for index, token in enumerate(tokens):
        if _is_punct(token, "("):
            depth += 1
        elif _is_punct(token, ")"):
            depth -= 1
        elif depth == 0 and _is_word(token, "AS"):
            return _join(tokens[:index]).rstrip(), _join(tokens[index + 1 :]).strip()
    return text, ""


def _without_column_list(header: str) -> str:
    """A view header without its top-level ``( columns )`` list."""
    tokens = tokenize(header)
    for index, token in enumerate(tokens):
        if _is_punct(token, "("):
            call = _parse_call(tokens, index - 1)
            if call is not None:
                return _join(tokens[:index] + tokens[call.close_index + 1 :])
    return header


def _is_to_view(header: str) -> bool:
    tokens = tokenize(_without_column_list(header))
    return any(_is_word(token, "TO") for token in tokens)


def _diff_materialized_view(live: DbObject, target: DbObject, diff) -> None:
    live_header, _ = _split_select(live.text)
    target_header, target_select = _split_select(target.text)
    if live.engine_prefix != target.engine_prefix:
        same = False
    elif _is_to_view(target_header):
        # MODIFY QUERY re-derives the column list of a TO view from the new
        # SELECT; everything else (the TO table first of all) must stay.
        same = _without_column_list(live_header) == _without_column_list(target_header)
    else:
        # The inner table is not touched by MODIFY QUERY: its columns and
        # engine must already match.
        same = live_header == target_header
    if same and target_select:
        diff.change(
            target.name,
            f"{_label(target)}: modify query",
            [
                Statement(
                    f"ALTER TABLE {quote_name(target.name)} MODIFY QUERY {target_select}"
                )
            ],
        )
        return
    diff.refuse(
        f"{_label(target)}: its definition changes beyond the SELECT (target table, "
        "engine, columns or other clauses), which ALTER TABLE ... MODIFY QUERY cannot "
        "do. Recreate it by hand (DROP VIEW + CREATE MATERIALIZED VIEW); a view "
        "without TO loses the data it stores, and rows inserted in between are "
        "not processed."
    )


def _table_refusals(live: DbObject, target: DbObject) -> List[Tuple[str, bool]]:
    """``(reason, needs a rebuild)`` for every change diff will not generate."""
    reasons: List[Tuple[str, bool]] = []
    if live.engine_prefix != target.engine_prefix:
        reasons.append(
            (
                f"the engine changes from {live.engine_prefix}{live.engine} to "
                f"{target.engine_prefix}{target.engine}",
                True,
            )
        )
    elif live.engine != target.engine:
        reasons.append(
            (f"the engine changes from {live.engine} to {target.engine}", True)
        )
    for clause, field in _KEY_CLAUSES:
        before, after = getattr(live, field), getattr(target, field)
        if before != after:
            reasons.append(
                (
                    f"{clause} changes from {before or '(none)'} to "
                    f"{after or '(none)'}",
                    True,
                )
            )
    if live.settings != target.settings:
        reasons.append(
            (
                f"SETTINGS change from {live.settings or '(none)'} to "
                f"{target.settings or '(none)'} (most can be changed with "
                "ALTER TABLE ... MODIFY SETTING / RESET SETTING, by hand)",
                False,
            )
        )
    if live.other_elements != target.other_elements:
        reasons.append(
            (
                "its projections or constraints change (ADD/DROP PROJECTION or "
                "CONSTRAINT by hand; a new projection needs MATERIALIZE PROJECTION)",
                False,
            )
        )
    live_columns = {c.name: c for c in live.columns}
    for column in target.columns:
        before = live_columns.get(column.name)
        if (
            before is not None
            and before.definition != column.definition
            and before[:6] == column[:6]
        ):
            reasons.append(
                (
                    f"column {quote_name(column.name)} changes in a way diff does "
                    f"not handle (e.g. a column TTL): {before.definition} -> "
                    f"{column.definition}",
                    False,
                )
            )
    kept_live = [i.name for i in live.indices if i in target.indices]
    kept_target = [i.name for i in target.indices if i in live.indices]
    if kept_live != kept_target:
        reasons.append(
            (
                "the order of its data skipping indices changes (drop and re-add "
                "them by hand in the new order)",
                False,
            )
        )
    return reasons


def _place(names: List[str], position: int) -> str:
    return "FIRST" if position == 0 else f"AFTER {quote_name(names[position - 1])}"


def _default_clause(column: Column) -> str:
    if column.default_kind == "EPHEMERAL" and (
        column.default_expression
        == f"defaultValueOfTypeName({quote_string(column.type)})"
    ):
        return "EPHEMERAL"
    return f"{column.default_kind} {column.default_expression}"


def _column_statements(
    table: str, before: Column, after: Column
) -> Tuple[List[Statement], List[str]]:
    modify = f"ALTER TABLE {table} MODIFY COLUMN {quote_name(after.name)}"
    statements: List[Statement] = []
    what: List[str] = []
    if before.codec and not after.codec:
        statements.append(Statement(f"{modify} REMOVE CODEC"))
        what.append("codec")
    if before.default_kind and not after.default_kind:
        statements.append(Statement(f"{modify} REMOVE {before.default_kind}"))
        what.append("default")
    if after.default_kind and (before.default_kind, before.default_expression) != (
        after.default_kind,
        after.default_expression,
    ):
        statements.append(Statement(f"{modify} {after.type} {_default_clause(after)}"))
        what.append("default")
    if before.type != after.type:
        if not what or what[-1] != "default" or not after.default_kind:
            statements.append(Statement(f"{modify} {after.type}"))
        what.append("type (rewrites the column data)")
    if after.codec and after.codec != before.codec:
        statements.append(Statement(f"{modify} {after.type} {after.codec}"))
        what.append("codec")
    if before.comment != after.comment:
        statements.append(
            Statement(
                f"ALTER TABLE {table} COMMENT COLUMN {quote_name(after.name)} "
                f"{quote_string(after.comment)}"
            )
        )
        what.append("comment")
    return statements, what


def _diff_columns(table: str, live: DbObject, target: DbObject, statements, summary):
    target_names = [c.name for c in target.columns]
    live_columns = {c.name: c for c in live.columns}
    for column in live.columns:
        if column.name not in target_names:
            statements.append(
                Statement(
                    f"ALTER TABLE {table} DROP COLUMN {quote_name(column.name)}",
                    destructive=True,
                )
            )
            summary.append(f"drop column {quote_name(column.name)}")

    # Where the kept columns are once the dropped ones are gone; every target
    # column is then put right after its predecessor, in target order.
    current = [c.name for c in live.columns if c.name in target_names]
    for position, column in enumerate(target.columns):
        before = live_columns.get(column.name)
        if before is None:
            definition = column.definition or f"{quote_name(column.name)} {column.type}"
            statements.append(
                Statement(
                    f"ALTER TABLE {table} ADD COLUMN {definition} "
                    f"{_place(target_names, position)}"
                )
            )
            summary.append(f"add column {quote_name(column.name)}")
            current.insert(position, column.name)
            continue
        changed, what = _column_statements(table, before, column)
        statements.extend(changed)
        if what:
            summary.append(
                f"modify column {quote_name(column.name)} "
                f"({', '.join(dict.fromkeys(what))})"
            )
        if current.index(column.name) != position:
            statements.append(
                Statement(
                    f"ALTER TABLE {table} MODIFY COLUMN {quote_name(column.name)} "
                    f"{column.type} {_place(target_names, position)}"
                )
            )
            summary.append(f"move column {quote_name(column.name)}")
            current.remove(column.name)
            current.insert(position, column.name)


def _diff_indices(table: str, live: DbObject, target: DbObject):
    """``(drops, adds, summary)`` for the data skipping indices."""
    live_indices = {i.name: i for i in live.indices}
    target_names = [i.name for i in target.indices]
    replaced = {
        index.name
        for index in target.indices
        if index.name in live_indices and live_indices[index.name] != index
    }
    drops: List[Statement] = []
    adds: List[Statement] = []
    summary: List[str] = []
    for index in live.indices:
        if index.name not in target_names or index.name in replaced:
            drops.append(
                Statement(
                    f"ALTER TABLE {table} DROP INDEX {quote_name(index.name)}",
                    destructive=True,
                )
            )
            if index.name not in replaced:
                summary.append(f"drop index {quote_name(index.name)}")
    for position, index in enumerate(target.indices):
        if index.name in live_indices and index.name not in replaced:
            continue
        # Re-adding a changed index needs its DROP first, so both are
        # commented out together without --allow-destructive.
        adds.append(
            Statement(
                f"ALTER TABLE {table} ADD {index.definition()} "
                f"{_place(target_names, position)}",
                destructive=index.name in replaced,
            )
        )
        verb = "replace" if index.name in replaced else "add"
        summary.append(
            f"{verb} index {quote_name(index.name)} (only new data is indexed until "
            "ALTER TABLE ... MATERIALIZE INDEX)"
        )
    return drops, adds, summary


def _diff_table(live: DbObject, target: DbObject, diff) -> None:
    reasons = _table_refusals(live, target)
    for reason, rebuild in reasons:
        diff.refuse(
            f"{_label(target)}: {reason}." + (f" {REBUILD_HINT}" if rebuild else "")
        )
    if reasons:
        # A table that needs a rebuild gets no partial ALTERs: the rebuild
        # replaces them.
        return

    table = quote_name(target.name)
    summary: List[str] = []
    # Indices go first so none refers to a column that is dropped.
    statements, index_adds, index_summary = _diff_indices(table, live, target)
    _diff_columns(table, live, target, statements, summary)
    statements.extend(index_adds)
    summary.extend(index_summary)

    if live.ttl != target.ttl:
        if target.ttl:
            statements.append(Statement(f"ALTER TABLE {table} MODIFY TTL {target.ttl}"))
            summary.append("modify TTL (expired rows are deleted when it is applied)")
        else:
            statements.append(Statement(f"ALTER TABLE {table} REMOVE TTL"))
            summary.append("remove TTL")
    if live.comment != target.comment:
        statements.append(
            Statement(
                f"ALTER TABLE {table} MODIFY COMMENT {quote_string(target.comment)}"
            )
        )
        summary.append("modify comment")

    if not statements:
        return
    diff.change(target.name, f"{_label(target)}: {'; '.join(summary)}", statements)
    added = any(s.startswith("add column") for s in summary)
    dropped = any(s.startswith("drop column") for s in summary)
    if added and dropped:
        diff.note(
            f"{_label(target)} drops and adds columns: if a column was renamed, "
            "replace the pair with ALTER TABLE ... RENAME COLUMN (diff cannot tell "
            "a rename from a drop + add, and the DROP loses the data)."
        )


class _Builder:
    def __init__(self):
        self.changes: List[Change] = []
        self.refusals: List[str] = []
        self.notes: List[str] = []

    def change(self, name: str, summary: str, statements: Iterable[Statement]):
        self.changes.append(Change(name, summary, tuple(statements)))

    def refuse(self, reason: str):
        self.refusals.append(reason)

    def note(self, note: str):
        self.notes.append(note)


def diff_schemas(
    live: Dict[str, DbObject],
    target: Dict[str, DbObject],
    live_order: Sequence[str],
    target_order: Sequence[str],
) -> SchemaDiff:
    """What turns ``live`` into ``target``.

    ``*_order`` are the object names in dependency order (what an object
    reads from comes first). Objects missing from the target are dropped
    first, dependents first; then every target object is created or changed
    in target dependency order, so a new view comes after the new column it
    reads.
    """
    diff = _Builder()
    dropped = []
    for name in reversed(live_order):
        if name in target:
            continue
        obj = live[name]
        dropped.append(obj)
        diff.change(
            name,
            f"drop {_label(obj)}",
            [
                Statement(
                    f"DROP {_DROP_KEYWORD[obj.kind]} {quote_name(name)}",
                    destructive=True,
                )
            ],
        )

    created = []
    for name in target_order:
        after = target[name]
        before = live.get(name)
        if before is None:
            created.append(after)
            diff.change(
                name,
                f"create {_label(after)}",
                [Statement(_restore_prefix(after.text, after.engine_prefix))],
            )
        elif before.kind != after.kind:
            diff.refuse(
                f"{quote_name(name)} is a {before.kind} in the database but a "
                f"{after.kind} in the schema file: drop and recreate it by hand."
            )
        elif after.kind == KIND_TABLE:
            _diff_table(before, after, diff)
        elif (before.text, before.engine_prefix) == (after.text, after.engine_prefix):
            continue
        elif after.kind == KIND_MATERIALIZED_VIEW:
            _diff_materialized_view(before, after, diff)
        else:
            diff.change(
                name,
                f"replace {_label(after)}",
                [Statement(_replace_statement(after.text))],
            )

    if any(o.kind == KIND_TABLE for o in dropped) and any(
        o.kind == KIND_TABLE for o in created
    ):
        diff.note(
            "Tables are dropped and created: if a table was renamed, use RENAME "
            "TABLE instead (diff cannot tell a rename from a drop + create, and "
            "the DROP loses the data)."
        )
    return SchemaDiff(tuple(diff.changes), tuple(diff.refusals), tuple(diff.notes))
