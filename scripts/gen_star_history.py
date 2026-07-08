#!/usr/bin/env python3
"""Generate a self-contained star-history SVG from the GitHub API.

Committed as assets/star-history.svg and refreshed by the star-history workflow,
so the README chart never depends on a third-party embed service at render time.

Usage:
    GITHUB_TOKEN=$(gh auth token) python scripts/gen_star_history.py \
        --repo zifter/clickhouse-migrations --out assets/star-history.svg
"""
import argparse
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

API = "https://api.github.com"

# Colours chosen to stay legible on both light and dark README backgrounds.
ACCENT = "#539bf5"
TEXT = "#7d8590"
GRID = "#7d8590"


def _get(url: str, token: str, accept: str) -> urllib.request.addinfourl:
    req = urllib.request.Request(url)
    req.add_header("Accept", accept)
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    return urllib.request.urlopen(req)


def fetch_starred_at(repo: str, token: str) -> list:
    """Return sorted list of datetime objects, one per stargazer."""
    stars = []
    page = 1
    while True:
        url = f"{API}/repos/{repo}/stargazers?per_page=100&page={page}"
        with _get(url, token, "application/vnd.github.star+json") as resp:
            batch = json.load(resp)
        if not batch:
            break
        for item in batch:
            stars.append(
                datetime.strptime(item["starred_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
            )
        if len(batch) < 100:
            break
        page += 1
    stars.sort()
    return stars


def _nice_ceil(value: int) -> int:
    if value <= 5:
        return 5
    step = 10 ** (len(str(value)) - 1)
    return ((value // step) + 1) * step


def _fmt_date(dt: datetime) -> str:
    return dt.strftime("%b %Y")


def render_svg(stars: list, repo: str) -> str:
    width, height = 800, 400
    pad_l, pad_r, pad_t, pad_b = 60, 30, 45, 45
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    now = datetime.now(timezone.utc)
    total = len(stars)

    # Build cumulative (datetime, count) points. Start the line at the baseline of
    # the first star so the growth is visible, and extend it flat to "now".
    if stars:
        t_min = stars[0]
        points = [(stars[0], 0)]
        points += [(dt, i + 1) for i, dt in enumerate(stars)]
        points.append((now, total))
    else:
        t_min = now
        points = [(now, 0)]

    t_max = now
    span = (t_max - t_min).total_seconds() or 1.0
    y_max = _nice_ceil(total)

    def px(dt: datetime) -> float:
        return pad_l + plot_w * ((dt - t_min).total_seconds() / span)

    def py(count: int) -> float:
        return pad_t + plot_h * (1 - count / y_max)

    coords = [(px(dt), py(c)) for dt, c in points]
    line = " ".join(f"{x:.1f},{y:.1f}" for x, y in coords)
    area = (
        f"{pad_l:.1f},{pad_t + plot_h:.1f} "
        + line
        + f" {coords[-1][0]:.1f},{pad_t + plot_h:.1f}"
    )

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" '
        f'font-family="-apple-system,Segoe UI,Helvetica,Arial,sans-serif">',
        f'<text x="{pad_l}" y="24" fill="{TEXT}" font-size="15" font-weight="600">'
        f"Star history — {repo}</text>",
    ]

    # Y gridlines + labels.
    y_ticks = 5
    for i in range(y_ticks + 1):
        val = round(y_max * i / y_ticks)
        y = py(val)
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{pad_l + plot_w}" y2="{y:.1f}" '
            f'stroke="{GRID}" stroke-opacity="0.2" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{pad_l - 8}" y="{y + 4:.1f}" fill="{TEXT}" font-size="11" '
            f'text-anchor="end">{val}</text>'
        )

    # X labels (start, middle, end).
    for frac in (0.0, 0.5, 1.0):
        dt = t_min + (t_max - t_min) * frac
        x = pad_l + plot_w * frac
        anchor = "start" if frac == 0 else "end" if frac == 1 else "middle"
        parts.append(
            f'<text x="{x:.1f}" y="{height - 16}" fill="{TEXT}" font-size="11" '
            f'text-anchor="{anchor}">{_fmt_date(dt)}</text>'
        )

    parts.append(f'<polygon points="{area}" fill="{ACCENT}" fill-opacity="0.12"/>')
    parts.append(
        f'<polyline points="{line}" fill="none" stroke="{ACCENT}" '
        f'stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>'
    )
    parts.append(
        f'<text x="{pad_l + plot_w}" y="{pad_t - 6}" fill="{ACCENT}" font-size="12" '
        f'font-weight="600" text-anchor="end">{total} ★</text>'
    )
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default="zifter/clickhouse-migrations")
    parser.add_argument("--out", default="assets/star-history.svg")
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN", "")
    try:
        stars = fetch_starred_at(args.repo, token)
    except urllib.error.HTTPError as exc:  # pragma: no cover - network guard
        print(f"GitHub API error: {exc}")
        return 1

    svg = render_svg(stars, args.repo)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf8") as handle:
        handle.write(svg)
    print(f"Wrote {args.out} ({len(stars)} stars)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
