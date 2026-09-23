"""``${NAME}`` placeholder substitution in migration files (opt-in).

The rules follow ``envsubst`` and the Node ``clickhouse-migrations``:

* ``${NAME}`` is replaced with the value of ``NAME``;
* an unset ``NAME`` fails - a placeholder is never left in the SQL and never
  silently becomes empty;
* ``$${NAME}`` is an escape and becomes a literal ``${NAME}``;
* a malformed (``${PG-HOST}``, ``${}``) or unterminated (``${PG_HOST``)
  placeholder fails;
* any other ``$`` is left alone.

Substitution is purely textual: SQL is not parsed and values are not escaped.

Variable values may be secrets (passwords of dictionary sources, ...), so no
error raised here ever contains a value - only file names, line numbers,
variable names and the placeholder text as written in the file.
"""

import os
import re
from typing import Dict, List, Mapping, Optional, Tuple

from clickhouse_migrations.exceptions import MigrationException

VARIABLE_NAME_PATTERN = "[A-Za-z_][A-Za-z0-9_]*"

_VARIABLE_NAME_RE = re.compile(VARIABLE_NAME_PATTERN)
# "$${" (escape) is tried first, so "$${X}" is never read as "$" + "${X}".
_OPENING_RE = re.compile(r"\$\$\{|\$\{")
# A placeholder never spans lines: no closing brace on the same line means it
# is unterminated.
_PLACEHOLDER_RE = re.compile(r"\$\{([^}\n]*)\}")

# How much of a broken placeholder is quoted back in an error message.
_SNIPPET_LENGTH = 40


def is_valid_name(name: str) -> bool:
    return _VARIABLE_NAME_RE.fullmatch(name) is not None


def _line_of(text: str, index: int) -> int:
    return text.count("\n", 0, index) + 1


def _snippet(text: str) -> str:
    if len(text) > _SNIPPET_LENGTH:
        return text[:_SNIPPET_LENGTH] + "..."
    return text


def substitute(text: str, variables: Mapping[str, str], source: str) -> str:
    """Return ``text`` with every ``${NAME}`` replaced from ``variables``.

    ``source`` names the file in error messages. Every unset variable of the
    file is reported at once; a syntax error stops at the first one.
    """
    parts: List[str] = []
    missing: Dict[str, int] = {}
    pos = 0

    while True:
        opening = _OPENING_RE.search(text, pos)
        if opening is None:
            parts.append(text[pos:])
            break

        parts.append(text[pos : opening.start()])
        if opening.group() == "$${":
            parts.append("${")
            pos = opening.end()
            continue

        placeholder = _PLACEHOLDER_RE.match(text, opening.start())
        if placeholder is None:
            written = text[opening.start() :].split("\n", 1)[0]
            raise MigrationException(
                f"{source}:{_line_of(text, opening.start())}: unterminated "
                f"placeholder {_snippet(written)!r} (no closing '}}' on the same "
                "line; write $${ for a literal ${)"
            )

        name = placeholder.group(1)
        if not is_valid_name(name):
            raise MigrationException(
                f"{source}:{_line_of(text, opening.start())}: malformed "
                f"placeholder {_snippet(placeholder.group())!r}: the name must "
                f"match {VARIABLE_NAME_PATTERN} (write $${{ for a literal ${{)"
            )

        if name in variables:
            parts.append(variables[name])
        else:
            missing.setdefault(name, _line_of(text, opening.start()))
        pos = placeholder.end()

    if missing:
        listed = ", ".join(f"{name} (line {line})" for name, line in missing.items())
        raise MigrationException(
            f"{source}: variable(s) not set: {listed}. Pass them with "
            "--var NAME=VALUE, or allow the process environment with "
            "--substitute-env."
        )

    return "".join(parts)


def parse_assignment(value: str) -> Tuple[str, str]:
    """Parse a ``NAME=VALUE`` pair; the value may be empty or contain ``=``.

    The error never echoes the value: it may be a secret.
    """
    name, separator, variable_value = value.partition("=")
    if not separator:
        raise ValueError("expected NAME=VALUE, but there is no '='")
    if not is_valid_name(name):
        raise ValueError(
            f"invalid variable name {name!r} in NAME=VALUE: "
            f"it must match {VARIABLE_NAME_PATTERN}"
        )
    return name, variable_value


def resolve_variables(
    variables: Optional[Mapping[str, str]] = None,
    substitute_env: bool = False,
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[Dict[str, str]]:
    """Build the mapping used for substitution, or None when it is disabled.

    Substitution is enabled by explicit ``variables`` (even an empty mapping)
    and/or ``substitute_env``. With ``substitute_env`` the process environment
    is used too, and explicit ``variables`` win over it.
    """
    if variables is None and not substitute_env:
        return None

    resolved: Dict[str, str] = {}
    if substitute_env:
        resolved.update(os.environ if environ is None else environ)

    for name, value in (variables or {}).items():
        if not is_valid_name(name):
            raise MigrationException(
                f"Invalid variable name {name!r}: it must match {VARIABLE_NAME_PATTERN}"
            )
        if not isinstance(value, str):
            raise MigrationException(
                f"The value of variable {name} must be a string, "
                f"got {type(value).__name__}"
            )
        resolved[name] = value

    return resolved
