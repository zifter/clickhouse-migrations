"""Offline checks of a migrations directory (the ``validate`` subcommand).

Nothing here connects to ClickHouse: every check works on file names and on
the statements exactly as ``migrate`` would split them (the same tokenizer).
"""

import json
import os
import re
from collections import defaultdict, namedtuple
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import DOWN_SUFFIX
from clickhouse_migrations.migrator import (
    StatementToken,
    find_unterminated_token,
    split_statement_tokens,
)

LEVEL_ERROR = "error"
LEVEL_WARNING = "warning"

CHECK_BAD_FILENAME = "bad-filename"
CHECK_DUPLICATE_VERSION = "duplicate-version"
CHECK_ORPHAN_DOWN = "orphan-down"
CHECK_EMPTY_FILE = "empty-file"
CHECK_UNTERMINATED = "unterminated"
CHECK_ENCODING = "encoding"
CHECK_VERSION_GAP = "version-gap"
CHECK_DESTRUCTIVE = "destructive"
CHECK_MISSING_DOWN = "missing-down"
CHECK_STANDALONE_SET = "standalone-set"
CHECK_ON_CLUSTER = "on-cluster-mismatch"

# One problem found by validate. file is the file name inside the directory;
# line is 1-based, or None when the finding is about the file as a whole.
Finding = namedtuple("Finding", ["file", "line", "level", "check", "message"])

# files is the number of *.sql files that were checked.
ValidationReport = namedtuple(
    "ValidationReport", ["migrations_dir", "files", "findings"]
)

_UP_NAME_RE = re.compile(r"^([0-9]+)_(.+)\.sql$", re.DOTALL)
_DOWN_NAME_RE = re.compile(r"^([0-9]+)_(.+)\.down\.sql$", re.DOTALL)

# The patterns below run on "code": the statement with comments removed and
# string literals / quoted identifiers replaced by placeholders, so a keyword
# inside a string, a quoted name or a comment never matches.
_FLAGS = re.IGNORECASE | re.DOTALL
_DROP_RE = re.compile(
    r"^\s*DROP\s+(?:TEMPORARY\s+)?(TABLE|DATABASE|DICTIONARY|VIEW)\b", _FLAGS
)
_TRUNCATE_RE = re.compile(r"^\s*TRUNCATE\b", _FLAGS)
_DELETE_FROM_RE = re.compile(r"^\s*DELETE\s+FROM\b", _FLAGS)
_ALTER_RE = re.compile(r"^\s*ALTER\s+TABLE\b", _FLAGS)
# A mutation is an ALTER command, so it follows the table name (and the
# optional ON CLUSTER clause) or a ",". This keeps the DELETE of a TTL rule
# ("MODIFY TTL d + INTERVAL 1 DAY DELETE") from counting.
_ALTER_MUTATION_RE = re.compile(
    r"(?:^\s*ALTER\s+TABLE\s+\S+(?:\s+ON\s+CLUSTER\s+\S+)?|,)\s*(DELETE|UPDATE)\b",
    _FLAGS,
)
_DROP_COLUMN_RE = re.compile(r"\bDROP\s+COLUMN\b", _FLAGS)
_SET_RE = re.compile(r"^\s*SET\s", _FLAGS)
_ON_CLUSTER_RE = re.compile(r"\bON\s+CLUSTER\b", _FLAGS)
_DDL_RE = re.compile(
    r"^\s*(?:CREATE|ALTER|DROP|RENAME|TRUNCATE|EXCHANGE|ATTACH|DETACH|OPTIMIZE)\b",
    _FLAGS,
)

_PLACEHOLDERS = {
    "line_comment": " ",
    "block_comment": " ",
    "single": " '' ",
    "double": " _ ",
    "backtick": " _ ",
}

_UNTERMINATED_WHAT = {
    "'": "string literal",
    '"': "quoted identifier",
    "`": "quoted identifier",
    "/": "block comment",
}


class _File:  # pylint: disable=too-few-public-methods
    def __init__(self, name: str, is_down: bool, version: Optional[int]):
        self.name = name
        self.is_down = is_down
        self.version = version
        # None until the content is known to be usable (readable, terminated).
        self.uses_on_cluster: Optional[bool] = None


def _line(script: str, offset: int) -> int:
    return script.count("\n", 0, offset) + 1


def _code(tokens: List[StatementToken]) -> str:
    return "".join(_PLACEHOLDERS.get(token.kind, token.text) for token in tokens)


def _statement_line(script: str, tokens: List[StatementToken]) -> int:
    first = next(token for token in tokens if token.text.strip())
    return _line(script, first.offset + len(first.text) - len(first.text.lstrip()))


def destructive_operations(code: str) -> List[str]:
    """Names of the destructive operations in one statement's code."""
    found: List[str] = []
    drop = _DROP_RE.match(code)
    if drop:
        found.append(f"DROP {drop.group(1).upper()}")
    if _TRUNCATE_RE.match(code):
        found.append("TRUNCATE")
    if _DELETE_FROM_RE.match(code):
        found.append("DELETE FROM")
    if _ALTER_RE.match(code):
        for mutation in sorted(
            {m.group(1).upper() for m in _ALTER_MUTATION_RE.finditer(code)}
        ):
            found.append(f"ALTER ... {mutation}")
        if _DROP_COLUMN_RE.search(code):
            found.append("DROP COLUMN")

    return found


def _parse_name(name: str) -> Tuple[bool, Optional[int]]:
    is_down = name.endswith(DOWN_SUFFIX)
    match = (_DOWN_NAME_RE if is_down else _UP_NAME_RE).match(name)
    return is_down, int(match.group(1)) if match else None


def _check_script(migration: _File, script: str) -> List[Finding]:
    name = migration.name
    unterminated = find_unterminated_token(script)
    if unterminated is not None:
        if unterminated.kind == "other":
            what = _UNTERMINATED_WHAT[unterminated.text]
            detail = "is never closed"
        else:
            what = _UNTERMINATED_WHAT[unterminated.text[0]]
            detail = "ends with an escaped quote, so it is never closed"
        return [
            Finding(
                name,
                _line(script, unterminated.offset),
                LEVEL_ERROR,
                CHECK_UNTERMINATED,
                f"{what} {detail}: the statement splitter reaches the end "
                "of the file inside it",
            )
        ]

    statements = split_statement_tokens(script)
    codes = [_code(tokens) for tokens in statements]
    if not any(code.strip() for code in codes):
        return [
            Finding(
                name,
                None,
                LEVEL_ERROR,
                CHECK_EMPTY_FILE,
                "file has no SQL statements (only whitespace and comments)",
            )
        ]

    findings: List[Finding] = []
    for tokens, code in zip(statements, codes):
        line = _statement_line(script, tokens)
        destructive = [] if migration.is_down else destructive_operations(code)
        if destructive:
            findings.append(
                Finding(
                    name,
                    line,
                    LEVEL_WARNING,
                    CHECK_DESTRUCTIVE,
                    f"destructive statement ({', '.join(destructive)})",
                )
            )

        if len(statements) > 1 and _SET_RE.match(code):
            findings.append(
                Finding(
                    name,
                    line,
                    LEVEL_WARNING,
                    CHECK_STANDALONE_SET,
                    "standalone SET in a multi-statement file: it does not carry "
                    "over to the next statement with clickhouse-connect; "
                    "use a SETTINGS clause instead",
                )
            )

    if not migration.is_down and any(_DDL_RE.match(code) for code in codes):
        migration.uses_on_cluster = any(_ON_CLUSTER_RE.search(code) for code in codes)

    return findings


def _read(path: Path, migration: _File) -> Tuple[Optional[str], List[Finding]]:
    try:
        return path.read_text(encoding="utf8"), []
    except UnicodeDecodeError as exc:
        return None, [
            Finding(
                migration.name,
                None,
                LEVEL_ERROR,
                CHECK_ENCODING,
                f"file is not valid UTF-8: {exc.reason} at byte {exc.start}",
            )
        ]


def _check_versions(files: List[_File], require_down: bool) -> List[Finding]:
    findings: List[Finding] = []
    by_version: Dict[Tuple[bool, int], List[_File]] = defaultdict(list)
    for migration in files:
        if migration.version is not None:
            by_version[(migration.is_down, migration.version)].append(migration)

    for (_, version), group in by_version.items():
        for migration in group if len(group) > 1 else []:
            others = ", ".join(m.name for m in group if m is not migration)
            findings.append(
                Finding(
                    migration.name,
                    None,
                    LEVEL_ERROR,
                    CHECK_DUPLICATE_VERSION,
                    f"version {version} is also used by {others}",
                )
            )

    up = {v: group[0] for (is_down, v), group in by_version.items() if not is_down}
    down = {v: group[0] for (is_down, v), group in by_version.items() if is_down}

    for version, migration in sorted(down.items()):
        if version not in up:
            findings.append(
                Finding(
                    migration.name,
                    None,
                    LEVEL_ERROR,
                    CHECK_ORPHAN_DOWN,
                    f"no up-migration with version {version}",
                )
            )

    # Only a directory that uses down files at all is expected to pair every
    # migration, unless --require-down asks for it explicitly.
    if require_down or down:
        level = LEVEL_ERROR if require_down else LEVEL_WARNING
        for version, migration in sorted(up.items()):
            if version not in down:
                findings.append(
                    Finding(
                        migration.name,
                        None,
                        level,
                        CHECK_MISSING_DOWN,
                        f"no paired {Path(migration.name).stem}{DOWN_SUFFIX}",
                    )
                )

    versions = sorted(up)
    for previous, version in zip(versions, versions[1:]):
        if version - previous > 1:
            missing = (
                f"version {previous + 1} is"
                if version - previous == 2
                else f"versions {previous + 1}-{version - 1} are"
            )
            findings.append(
                Finding(
                    up[version].name,
                    None,
                    LEVEL_WARNING,
                    CHECK_VERSION_GAP,
                    f"{missing} missing before this file",
                )
            )

    return findings


def _check_on_cluster(files: List[_File]) -> List[Finding]:
    ddl_files = [m for m in files if m.uses_on_cluster is not None]
    with_clause = [m for m in ddl_files if m.uses_on_cluster]
    without_clause = [m for m in ddl_files if not m.uses_on_cluster]
    if not with_clause or not without_clause:
        return []

    total = len(ddl_files)
    # Flag the minority, it is the likelier mistake (ties: the files without it).
    if len(with_clause) < len(without_clause):
        return [
            Finding(
                m.name,
                None,
                LEVEL_WARNING,
                CHECK_ON_CLUSTER,
                f"uses ON CLUSTER, while {len(without_clause)} of {total} "
                "DDL migrations do not",
            )
            for m in with_clause
        ]
    return [
        Finding(
            m.name,
            None,
            LEVEL_WARNING,
            CHECK_ON_CLUSTER,
            f"has no ON CLUSTER, while {len(with_clause)} of {total} "
            "DDL migrations use it",
        )
        for m in without_clause
    ]


def validate_migrations(
    migrations_dir: Union[Path, str], require_down: bool = False
) -> ValidationReport:
    """Check a migrations directory offline; never connects to ClickHouse.

    Only *.sql files are looked at (README.md and friends are ignored).
    Raises MigrationException if the directory does not exist.
    """
    migrations_dir = Path(migrations_dir)
    if not migrations_dir.is_dir():
        raise MigrationException(
            f"Migrations directory does not exist: {migrations_dir}"
        )

    files: List[_File] = []
    findings: List[Finding] = []
    for entry in os.scandir(migrations_dir):
        if not entry.name.endswith(".sql") or not entry.is_file():
            continue

        is_down, version = _parse_name(entry.name)
        migration = _File(entry.name, is_down, version)
        files.append(migration)
        if version is None:
            expected = f"{{VERSION}}_{{name}}{DOWN_SUFFIX if is_down else '.sql'}"
            findings.append(
                Finding(
                    entry.name,
                    None,
                    LEVEL_ERROR,
                    CHECK_BAD_FILENAME,
                    f"file name does not match {expected} "
                    "(VERSION is an integer, name is not empty)",
                )
            )

        script, read_findings = _read(Path(entry.path), migration)
        findings.extend(read_findings)
        if script is not None:
            findings.extend(_check_script(migration, script))

    findings.extend(_check_versions(files, require_down))
    findings.extend(_check_on_cluster(files))

    files.sort(key=lambda m: (m.version is None, m.version or 0, m.is_down, m.name))
    order = {m.name: index for index, m in enumerate(files)}
    findings.sort(key=lambda f: (order[f.file], f.line or 0))

    return ValidationReport(str(migrations_dir), len(files), findings)


def _counts(report: ValidationReport) -> Tuple[int, int]:
    errors = sum(1 for f in report.findings if f.level == LEVEL_ERROR)
    return errors, len(report.findings) - errors


def validate_exit_code(report: ValidationReport, strict: bool = False) -> int:
    errors, warnings = _counts(report)
    return 1 if errors or (strict and warnings) else 0


def format_validate(report: ValidationReport) -> str:
    errors, warnings = _counts(report)
    if not report.findings:
        return (
            f"No problems found in {report.files} migration file(s) "
            f"in {report.migrations_dir}."
        )

    table = [("FILE", "LEVEL", "CHECK", "MESSAGE")]
    for finding in report.findings:
        location = (
            finding.file if finding.line is None else f"{finding.file}:{finding.line}"
        )
        table.append((location, finding.level, finding.check, finding.message))

    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    lines = [
        "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)).rstrip()
        for row in table
    ]
    lines.append("")
    lines.append(
        f"{errors} error(s), {warnings} warning(s) in {report.files} "
        f"migration file(s) in {report.migrations_dir}."
    )
    return "\n".join(lines)


def format_validate_json(report: ValidationReport) -> str:
    errors, warnings = _counts(report)
    return json.dumps(
        {
            "migrations_dir": report.migrations_dir,
            "files": report.files,
            "errors": errors,
            "warnings": warnings,
            "findings": [
                {
                    "file": f.file,
                    "line": f.line,
                    "level": f.level,
                    "check": f.check,
                    "message": f.message,
                }
                for f in report.findings
            ],
        },
        indent=2,
    )
