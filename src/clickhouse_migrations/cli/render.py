"""The status table and its JSON form (status, baseline, repair)."""

import json
from typing import List, Optional

from clickhouse_migrations.migrator import StatusRow

STATUS_FORMAT_TABLE = "table"
STATUS_FORMAT_JSON = "json"
STATUS_FORMATS = (STATUS_FORMAT_TABLE, STATUS_FORMAT_JSON)


def format_status(rows: List[StatusRow]) -> str:
    if not rows:
        return "No migrations found."

    table = [("VERSION", "STATUS", "MD5", "APPLIED AT", "HAS DOWN")]
    for row in rows:
        table.append(
            (
                str(row.version),
                row.state,
                row.md5 or "",
                str(row.applied_at) if row.applied_at is not None else "",
                "yes" if row.has_down else "no",
            )
        )

    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    return "\n".join(
        "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)) for row in table
    )


def _isoformat(value) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def format_status_json(db_name: str, rows: List[StatusRow]) -> str:
    return json.dumps(
        {
            "database": db_name,
            "migrations": [
                {
                    "version": row.version,
                    "state": row.state,
                    "md5": row.md5,
                    "applied_at": _isoformat(row.applied_at),
                    "has_down": bool(row.has_down),
                }
                for row in rows
            ],
        },
        indent=2,
    )


def print_rows(ctx, cluster, rows: List[StatusRow], empty_message: str) -> None:
    if ctx.format == STATUS_FORMAT_JSON:
        db_name = ctx.db_name if ctx.db_name is not None else cluster.default_db_name
        print(format_status_json(db_name, rows))
    else:
        print(format_status(rows) if rows else empty_message)
