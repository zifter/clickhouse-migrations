"""Pure helpers behind ``clickhouse-migrations dump``.

Nothing in this module talks to ClickHouse: it takes the ``SHOW CREATE``
output the server produced and turns it into portable, diffable statements,
orders them by dependency and compares dumps. The database-facing part lives
in ``ClickhouseCluster.dump``.

The statement rewriting is token based (string literals, quoted identifiers
and comments are recognised), never a blind text replace, so a database name
that happens to appear inside a string literal or a column comment is left
alone. Anything that cannot be rewritten safely is left exactly as the server
printed it.
"""

import difflib
import heapq
import os
import re
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, NamedTuple, Optional, Set

from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.util import quote_string

KIND_SPACE = "space"
KIND_STRING = "string"
KIND_QUOTED = "quoted"
KIND_WORD = "word"
KIND_PUNCT = "punct"

_TOKEN_RE = re.compile(
    r"""
      (?P<space>\s+|--[^\n]*|/\*.*?\*/)
    | (?P<string>'(?:[^'\\]|\\.|'')*'|\$\$.*?\$\$)
    | (?P<quoted>`(?:[^`\\]|\\.|``)*`|"(?:[^"\\]|\\.|"")*")
    | (?P<word>[A-Za-z_][A-Za-z0-9_]*)
    | (?P<punct>.)
    """,
    re.VERBOSE | re.DOTALL,
)
_UUID_STRING_RE = re.compile(r"^'[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}'$")
_REPLICATED_ENGINE_RE = re.compile(r"^(Replicated|Shared)\w*MergeTree$")

# Engines whose first / second argument is a database name.
_DATABASE_ARGUMENT = {"Merge": 0, "Buffer": 0, "Distributed": 1}
CURRENT_DATABASE = "currentDatabase()"

# Names ClickHouse gives to the storage of a materialized view without TO.
INNER_TABLE_PREFIXES = (".inner.", ".inner_id.")

# The migration lock table (besides "<migrations table>_lock") is bookkeeping too.
LOCK_TABLE = "schema_lock"


class Token(NamedTuple):
    kind: str
    text: str


def tokenize(sql: str) -> List[Token]:
    """Split SQL into tokens. Joining ``text`` of all tokens gives ``sql`` back."""
    tokens = []
    for match in _TOKEN_RE.finditer(sql):
        tokens.append(Token(match.lastgroup, match.group()))
    return tokens


def _join(tokens: Iterable[Token]) -> str:
    return "".join(token.text for token in tokens)


def _is_punct(token: Token, text: str) -> bool:
    return token.kind == KIND_PUNCT and token.text == text


def _is_word(token: Token, text: str) -> bool:
    return token.kind == KIND_WORD and token.text.upper() == text


def identifier_value(token: Token) -> Optional[str]:
    """The name an identifier token stands for, ``None`` for other tokens."""
    if token.kind == KIND_WORD:
        return token.text
    if token.kind == KIND_QUOTED:
        quote = token.text[0]
        body = token.text[1:-1].replace(quote * 2, quote)
        return re.sub(r"\\(.)", r"\1", body, flags=re.DOTALL)
    return None


def _next_significant(tokens: List[Token], index: int) -> Optional[int]:
    index += 1
    while index < len(tokens) and tokens[index].kind == KIND_SPACE:
        index += 1
    return index if index < len(tokens) else None


def _pop_spaces(tokens: List[Token]) -> None:
    while tokens and tokens[-1].kind == KIND_SPACE:
        tokens.pop()


def _strip_uuid(tokens: List[Token]) -> List[Token]:
    """Drop ``UUID '...'`` (and ``TO INNER UUID '...'``) from the statement header.

    The UUID differs per server, so it would make every dump differ. It can
    only appear before the column list / ``AS``, so the body is never touched.
    """
    out: List[Token] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if _is_punct(token, "(") or _is_word(token, "AS"):
            out.extend(tokens[index:])
            break
        following = _next_significant(tokens, index)
        if (
            _is_word(token, "UUID")
            and following is not None
            and _UUID_STRING_RE.match(tokens[following].text)
        ):
            _pop_spaces(out)
            if out and _is_word(out[-1], "INNER"):
                out.pop()
                _pop_spaces(out)
                if out and _is_word(out[-1], "TO"):
                    out.pop()
                    _pop_spaces(out)
            index = following + 1
            continue
        out.append(token)
        index += 1
    return out


def _is_reference(tokens, index, out, db_name, known) -> bool:
    """Whether ``tokens[index:]`` starts with ``<db_name>.<known object>``."""
    if identifier_value(tokens[index]) != db_name or index + 2 >= len(tokens):
        return False
    if (
        not _is_punct(tokens[index + 1], ".")
        or identifier_value(tokens[index + 2]) not in known
    ):
        return False
    # Not the tail of a longer path (x.db.name) and not a function call.
    follows_call = index + 3 < len(tokens) and _is_punct(tokens[index + 3], "(")
    return not (out and _is_punct(out[-1], ".")) and not follows_call


def _strip_database(tokens: List[Token], db_name: str, known: Set[str]):
    """Remove the ``db.`` qualifier from references to objects of ``db_name``.

    A reference is three adjacent tokens ``<db> . <object>`` where ``<db>`` is
    the dumped database and ``<object>`` is a name that exists in it. Anything
    else (other databases, string literals, ``x.db.y`` paths, function calls)
    is kept. Returns the new tokens and the referenced object names.
    """
    out: List[Token] = []
    referenced: Set[str] = set()
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if _is_reference(tokens, index, out, db_name, known):
            referenced.add(identifier_value(tokens[index + 2]))
            index += 2
            continue
        out.append(token)
        index += 1
    return out, referenced


class _Call(NamedTuple):
    """A ``NAME(arg, arg, ...)`` construct: token indexes and argument slices."""

    name: str
    name_index: int
    close_index: int
    args: List[slice]


def _parse_call(tokens: List[Token], name_index: int) -> Optional[_Call]:
    """Parse ``NAME(...)`` starting at ``name_index``; ``None`` without a ``(``."""
    open_index = _next_significant(tokens, name_index)
    if open_index is None or not _is_punct(tokens[open_index], "("):
        return None

    args: List[slice] = []
    depth = 0
    start = open_index + 1
    for index in range(open_index, len(tokens)):
        token = tokens[index]
        if _is_punct(token, "("):
            depth += 1
        elif _is_punct(token, ")"):
            depth -= 1
            if depth == 0:
                args.append(_trim(tokens, start, index))
                return _Call(tokens[name_index].text, name_index, index, args)
        elif _is_punct(token, ",") and depth == 1:
            args.append(_trim(tokens, start, index))
            start = index + 1
    return None


def _trim(tokens: List[Token], start: int, stop: int) -> slice:
    while start < stop and tokens[start].kind == KIND_SPACE:
        start += 1
    while stop > start and tokens[stop - 1].kind == KIND_SPACE:
        stop -= 1
    return slice(start, stop)


def _find_engine(tokens: List[Token]) -> Optional[_Call]:
    """The ``ENGINE = Name(...)`` clause, looked up before the ``AS SELECT`` body."""
    depth = 0
    for index, token in enumerate(tokens):
        if _is_punct(token, "("):
            depth += 1
        elif _is_punct(token, ")"):
            depth -= 1
        elif depth == 0 and _is_word(token, "AS"):
            return None
        elif depth == 0 and _is_word(token, "ENGINE"):
            equals = _next_significant(tokens, index)
            name = equals and _next_significant(tokens, equals)
            if (
                name
                and _is_punct(tokens[equals], "=")
                and tokens[name].kind == KIND_WORD
            ):
                return _parse_call(tokens, name) or _Call(
                    tokens[name].text, name, name, []
                )
    return None


def _single_string(tokens: List[Token], span: slice) -> Optional[str]:
    chunk = tokens[span]
    if len(chunk) == 1 and chunk[0].kind == KIND_STRING:
        return chunk[0].text
    return None


def _rewrite_engine(
    tokens: List[Token], db_name: str, keep_replicated_paths: bool
) -> List[Token]:
    engine = _find_engine(tokens)
    if engine is None:
        return tokens

    if not keep_replicated_paths and _REPLICATED_ENGINE_RE.match(engine.name):
        # ReplicatedXxxMergeTree('<zk path>', '<replica>', <engine args>...):
        # the first two arguments are per-cluster, everything after them is
        # part of the table definition and stays.
        if (
            len(engine.args) >= 2
            and _single_string(tokens, engine.args[0])
            and _single_string(tokens, engine.args[1])
        ):
            rest = engine.args[2:]
            if not rest:
                return (
                    tokens[: engine.name_index + 1] + tokens[engine.close_index + 1 :]
                )
            inner: List[Token] = []
            for position, span in enumerate(rest):
                if position:
                    inner.append(Token(KIND_PUNCT, ", "))
                inner.extend(tokens[span])
            return (
                tokens[: engine.name_index + 1]
                + [Token(KIND_PUNCT, "(")]
                + inner
                + tokens[engine.close_index :]
            )
        return tokens

    position = _DATABASE_ARGUMENT.get(engine.name)
    if (
        position is not None
        and len(engine.args) > position
        and _single_string(tokens, engine.args[position]) == quote_string(db_name)
    ):
        # The database argument is evaluated when the table is created, so
        # currentDatabase() makes the dump replayable into any database.
        span = engine.args[position]
        return (
            tokens[: span.start]
            + [Token(KIND_WORD, CURRENT_DATABASE)]
            + tokens[span.stop :]
        )
    return tokens


def _source_options(tokens: List[Token], call: _Call):
    """Words, the TABLE literal and the DB clause ``(index, literal index, text)``."""
    body = range(call.name_index + 2, call.close_index)
    words = {tokens[i].text.upper() for i in body if tokens[i].kind == KIND_WORD}
    table = None
    db_span = None
    for i in body:
        following = _next_significant(tokens, i)
        value = tokens[following].text if following is not None else ""
        if _is_word(tokens[i], "TABLE") and value.startswith("'"):
            table = value
        if _is_word(tokens[i], "DB") and value.startswith("'"):
            db_span = (i, following, value)
    return words, table, db_span


def _dictionary_source(tokens: List[Token], db_name: str, known: Set[str]):
    """Handle ``SOURCE(CLICKHOUSE(... DB '<db>' TABLE '<table>' ...))``.

    Returns the tokens with a redundant ``DB '<db>'`` removed and the table the
    dictionary reads from (when it lives in the dumped database). The DB
    option is only dropped for a source without HOST/PORT: that one resolves
    to the dictionary's own database, so the dump stays replayable elsewhere.
    A source with a HOST is a remote connection and is left as is.
    """
    for index, token in enumerate(tokens):
        if not _is_word(token, "CLICKHOUSE"):
            continue
        call = _parse_call(tokens, index)
        if call is None:
            continue

        words, table, db_span = _source_options(tokens, call)
        if db_span is not None and db_span[2] != quote_string(db_name):
            return tokens, None
        referenced = None
        if table is not None:
            name = table[1:-1]
            referenced = name if name in known else None
        if db_span is not None and not words & {"HOST", "PORT"}:
            start, stop, _ = db_span
            while start > 0 and tokens[start - 1].kind == KIND_SPACE:
                start -= 1
            end = stop + 1
            if _is_punct(tokens[start - 1], "(") and tokens[end].kind == KIND_SPACE:
                end += 1
            tokens = tokens[:start] + tokens[end:]
        return tokens, referenced
    return tokens, None


class NormalizedStatement(NamedTuple):
    text: str
    references: Set[str]


def normalize_statement(
    statement: str,
    db_name: str,
    known: Set[str],
    keep_replicated_paths: bool = False,
) -> NormalizedStatement:
    """Make one ``SHOW CREATE`` statement portable and stable.

    * ``UUID '...'`` is removed;
    * references to ``db_name`` objects (``db.table``, ``TO db.table``, the
      object being created, ...) lose their database qualifier when the
      object is one of ``known``;
    * ``Replicated*MergeTree('<path>', '<replica>', ...)`` loses its first two
      arguments unless ``keep_replicated_paths``;
    * a ``Distributed`` / ``Merge`` / ``Buffer`` database argument equal to
      ``db_name`` becomes ``currentDatabase()``;
    * ``DB '<db_name>'`` is dropped from a local ``CLICKHOUSE`` dictionary
      source;
    * trailing whitespace is removed.

    Also returns the names of the ``known`` objects the statement refers to,
    used to order the dump by dependency.
    """
    tokens = _strip_uuid(tokenize(statement.strip()))
    is_dictionary = len(tokens) > 4 and any(
        _is_word(token, "DICTIONARY") for token in tokens[:6]
    )
    references: Set[str] = set()
    if is_dictionary:
        tokens, table = _dictionary_source(tokens, db_name, known)
        if table:
            references.add(table)

    tokens, referenced = _strip_database(tokens, db_name, known)
    references |= referenced
    tokens = _rewrite_engine(tokens, db_name, keep_replicated_paths)

    lines = _join(tokens).splitlines()
    return NormalizedStatement("\n".join(line.rstrip() for line in lines), references)


def server_dependencies(
    rows: List[Dict], db_name: str, names: Set[str]
) -> Dict[str, Set[str]]:
    """What each of ``names`` needs, from the ``system.tables`` dependency columns.

    ``loading_dependencies_*`` lists what a table needs (dictionaries, ``dictGet``
    defaults, ...) and ``dependencies_*`` the views that read from it; both are
    absent on old servers, where the references parsed from the definitions
    still order most objects.
    """
    dependencies: Dict[str, Set[str]] = {name: set() for name in names}
    for row in rows:
        for database, table in zip(
            row.get("dependencies_database", []), row.get("dependencies_table", [])
        ):
            if database == db_name and table in names:
                dependencies[table].add(row["name"])
        for database, table in zip(
            row.get("loading_dependencies_database", []),
            row.get("loading_dependencies_table", []),
        ):
            if database == db_name and row["name"] in names:
                dependencies[row["name"]].add(table)
    return dependencies


def sort_by_dependency(dependencies: Dict[str, Set[str]]) -> List[str]:
    """Order names so every object comes after the ones it depends on.

    ``dependencies`` maps a name to the names it needs; unknown names and
    self references are ignored. Ties are broken by name, so the result is
    deterministic. Raises ``MigrationException`` on a cycle.
    """
    names = set(dependencies)
    pending = {
        name: {dep for dep in deps if dep in names and dep != name}
        for name, deps in dependencies.items()
    }
    dependents: Dict[str, Set[str]] = {name: set() for name in names}
    for name, deps in pending.items():
        for dep in deps:
            dependents[dep].add(name)

    ready = [name for name, deps in pending.items() if not deps]
    heapq.heapify(ready)
    ordered: List[str] = []
    while ready:
        name = heapq.heappop(ready)
        ordered.append(name)
        for dependent in dependents[name]:
            pending[dependent].discard(name)
            if not pending[dependent]:
                heapq.heappush(ready, dependent)

    if len(ordered) != len(names):
        raise MigrationException(
            "Circular dependency between database objects: "
            + " -> ".join(_find_cycle({n: d for n, d in pending.items() if d}))
        )
    return ordered


def _find_cycle(blocked: Dict[str, Set[str]]) -> List[str]:
    """A cycle among ``blocked`` objects (each one waits for another blocked one)."""
    path = [min(blocked)]
    while True:
        following = min(dep for dep in blocked[path[-1]] if dep in blocked)
        if following in path:
            return path[path.index(following) :] + [following]
        path.append(following)


def render_dump(statements: List[str]) -> str:
    """Join statements: ``;`` terminated, blank line separated, final newline."""
    if not statements:
        return ""
    return "\n\n".join(statement + ";" for statement in statements) + "\n"


def normalize_dump_text(text: str) -> str:
    """Line endings and trailing whitespace only, for comparing dumps."""
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").split("\n")]
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines) + "\n" if lines else ""


def diff_dumps(expected: str, actual: str, expected_name: str, actual_name: str) -> str:
    """Unified diff (``expected`` -> ``actual``), empty when they are equal."""
    diff = difflib.unified_diff(
        normalize_dump_text(expected).splitlines(),
        normalize_dump_text(actual).splitlines(),
        fromfile=expected_name,
        tofile=actual_name,
        lineterm="",
    )
    return "\n".join(diff)


def write_text_atomic(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` through a temp file and a rename.

    A reader never sees a half written file, and the mode of an existing file
    is kept (a new file gets 0644, not the 0600 of a temp file).
    """
    path = Path(path)
    mode = path.stat().st_mode & 0o777 if path.exists() else 0o644
    handle, temp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(handle, "w", encoding="utf8", newline="\n") as stream:
            stream.write(text)
        os.chmod(temp_name, mode)
        os.replace(temp_name, path)
    except BaseException:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
        raise
