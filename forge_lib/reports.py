from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

from .status import SPRT_DB_PATH


SPRT_DEFAULT_REFERENCE_NET = "nn-1a298aa575a0.nnue"
SPRT_DEFAULT_ENGINE_FILTER = "*"
SPRT_DEFAULT_REFERENCE_ENGINE = "stockfish"


def sprt_report_db_path() -> Path:
    override = os.environ.get("FORGE_SPRT_DB")
    return Path(override).expanduser() if override else SPRT_DB_PATH


def candidate_filter(value: str) -> tuple[str, str]:
    """Return the displayed candidate filter and its SQL LIKE pattern."""
    displayed = value.strip() or "*"
    if "*" in displayed or "?" in displayed:
        pattern = displayed.replace("*", "%").replace("?", "_")
    elif "%" in displayed or "_" in displayed:
        pattern = displayed
    else:
        pattern = displayed + "%"
    return displayed, pattern


def candidate_pattern(value: str) -> str:
    """Return the SQL LIKE pattern for a candidate filter."""
    return candidate_filter(value)[1]


def engine_filter(value: str) -> tuple[str, str]:
    """Return the displayed engine filter and its SQL LIKE pattern."""
    displayed = Path(value.strip()).name if value.strip() else "*"
    pattern = displayed.replace("*", "%").replace("?", "_")
    return displayed, pattern


def reference_pattern(value: str) -> tuple[str, re.Pattern[str]]:
    """Return a basename filter and its compiled regular expression.

    Reference filters are regular expressions, with shell-style wildcards
    accepted as a convenience (for example, ``nn-*.nnue``). Matching is
    performed against the complete basename stored in the SPRT database.
    """
    value = value.strip()
    pattern = Path(value).name if value else ".*"
    try:
        compiled = re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"invalid reference net regexp {pattern!r}: {exc}") from exc
    return pattern, compiled


def _reference_net_matches(
    compiled: re.Pattern[str], shell_pattern: str, value: Any,
) -> int:
    if value is None:
        return 0
    name = str(value)
    return int(bool(compiled.search(name) or fnmatch.fnmatchcase(name, shell_pattern)))


def sprt_score_report(
    *,
    reference_net: str = SPRT_DEFAULT_REFERENCE_NET,
    candidate_net: str = "*",
    engine: str = SPRT_DEFAULT_ENGINE_FILTER,
    reference_engine: str = SPRT_DEFAULT_REFERENCE_ENGINE,
    min_games: int = 1500,
    limit: int = 20,
    show_run: bool = False,
    db_path: Path | None = None,
) -> dict[str, Any]:
    if min_games < 0:
        raise ValueError("min_games must be non-negative")
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    path = db_path or sprt_report_db_path()
    if not path.is_file():
        raise FileNotFoundError(f"SPRT database not found: {path}")

    reference_filter, reference_re = reference_pattern(reference_net)
    candidate_filter_value, candidate_like = candidate_filter(candidate_net)
    where = ["reference_net REGEXP ?", "candidate_net LIKE ?"]
    params: list[Any] = [reference_filter, candidate_like]
    engine_filter_value, engine_like = engine_filter(engine)
    where.append("engine LIKE ?")
    params.append(engine_like)
    reference_engine_value, reference_engine_like = engine_filter(reference_engine)
    if reference_engine_value == "*":
        # '*' also selects results recorded before the reference engine was.
        where.append("(reference_engine LIKE ? OR reference_engine IS NULL)")
    else:
        where.append("reference_engine LIKE ?")
    params.append(reference_engine_like)
    if min_games:
        where.append("games >= ?")
        params.append(min_games)
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    # Fixed-depth/node matches measure move quality, not playing strength at
    # a clock, so they never replace a candidate's ranked result.
    if "tc" in {row[1] for row in connection.execute("PRAGMA table_info(sprt)")}:
        where.append("(tc IS NULL OR (tc NOT LIKE 'depth=%' AND tc NOT LIKE 'nodes=%'))")
    predicate = " AND ".join(where)

    columns = ["rank", "elo", "candidate", "candidate_net", "reference", "reference_net", "games", "date"]
    if show_run:
        columns.append("run")
    # The latest result per candidate (engine + net) and reference engine.
    query = f"""
        SELECT ROW_NUMBER() OVER (ORDER BY elo DESC) AS rank,
               elo, engine AS candidate, candidate_net, reference_engine AS reference, reference_net,
               games, date, run
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY engine, candidate_net, reference_engine
                ORDER BY date DESC, id DESC
            ) AS candidate_rank
            FROM sprt
            WHERE {predicate}
        )
        WHERE candidate_rank = 1
        ORDER BY elo DESC
        LIMIT ?
    """
    params.append(limit)

    connection.row_factory = sqlite3.Row
    connection.create_function(
        "REGEXP",
        2,
        lambda pattern, value: _reference_net_matches(reference_re, str(pattern), value),
    )
    try:
        rows = [dict(row) for row in connection.execute(query, params)]
    finally:
        connection.close()
    return {
        "template": "sprt",
        "report": "score",
        "columns": columns,
        "rows": rows,
        "filters": {
            "reference_net": reference_filter,
            "candidate_net": candidate_filter_value,
            "engine": engine_filter_value,
            "reference_engine": reference_engine_value,
            "min_games": min_games,
            "limit": limit,
        },
    }


def report_text(payload: dict[str, Any]) -> str:
    columns = [str(column) for column in payload.get("columns", [])]
    rows = payload.get("rows", [])
    if not columns or not isinstance(rows, list):
        return ""

    def text_value(value: Any) -> str:
        if value is None:
            return "—"
        if isinstance(value, float):
            return f"{value:.1f}"
        return str(value)

    values = [[text_value(row.get(column)) for column in columns] for row in rows if isinstance(row, dict)]
    widths = [max([len(column), *(len(row[index]) for row in values)]) for index, column in enumerate(columns)]
    line = "  ".join(column.ljust(widths[index]) for index, column in enumerate(columns))
    rule = "  ".join("-" * width for width in widths)
    body = ["  ".join(value.ljust(widths[index]) for index, value in enumerate(row)) for row in values]
    return "\n".join([line, rule, *body])


def cmd_report(args: argparse.Namespace) -> int:
    if args.template != "sprt" or args.report != "score":
        raise SystemExit(f"unknown report: {args.template} {args.report}")
    payload = sprt_score_report(
        show_run=bool(args.verbose),
        reference_net=args.reference_net,
        candidate_net=args.candidate_net,
        engine=args.engine or "",
        reference_engine=args.reference_engine or "",
        min_games=args.min_games,
        limit=args.limit,
    )
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(report_text(payload))
    return 0
