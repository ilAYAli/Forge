from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from .status import SPRT_DB_PATH


SPRT_REPORT_VIEWS = ("ranking", "runs")


def sprt_report_db_path() -> Path:
    override = os.environ.get("FORGE_SPRT_DB")
    return Path(override).expanduser() if override else SPRT_DB_PATH


def candidate_pattern(value: str) -> str:
    value = value.strip() or "%"
    return value if "%" in value or "_" in value else value + "%"


def sprt_score_report(
    *,
    view: str = "ranking",
    reference_net: str = "nn-1a298aa575a0.nnue",
    candidate_net: str = "enyo-%",
    engine: str = "",
    min_games: int = 1500,
    limit: int = 20,
    db_path: Path | None = None,
) -> dict[str, Any]:
    if view not in SPRT_REPORT_VIEWS:
        raise ValueError(f"unknown SPRT score view: {view}")
    if min_games < 0:
        raise ValueError("min_games must be non-negative")
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    path = db_path or sprt_report_db_path()
    if not path.is_file():
        raise FileNotFoundError(f"SPRT database not found: {path}")

    where = ["reference_net = ?", "candidate_net LIKE ?"]
    params: list[Any] = [Path(reference_net).name, candidate_pattern(candidate_net)]
    if engine:
        where.append("engine = ?")
        params.append(Path(engine).name)
    if min_games:
        where.append("games >= ?")
        params.append(min_games)
    predicate = " AND ".join(where)

    if view == "ranking":
        columns = ["rank", "candidate_net", "elo", "ci", "engine", "games", "date"]
        query = f"""
            SELECT ROW_NUMBER() OVER (ORDER BY elo DESC) AS rank,
                   candidate_net, elo, ci, engine, games, date
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY candidate_net
                    ORDER BY date DESC, id DESC
                ) AS candidate_rank
                FROM sprt
                WHERE {predicate}
            )
            WHERE candidate_rank = 1
            ORDER BY elo DESC
            LIMIT ?
        """
    else:
        columns = ["date", "candidate_net", "reference_net", "elo", "ci", "llr", "engine", "games", "run"]
        query = f"""
            SELECT date, candidate_net, reference_net, elo, ci, llr, engine, games, run
            FROM sprt
            WHERE {predicate}
            ORDER BY date DESC, id DESC
            LIMIT ?
        """
    params.append(limit)

    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = [dict(row) for row in connection.execute(query, params)]
    finally:
        connection.close()
    return {
        "template": "sprt",
        "report": "score",
        "view": view,
        "columns": columns,
        "rows": rows,
        "filters": {
            "reference_net": Path(reference_net).name,
            "candidate_net": candidate_net,
            "engine": Path(engine).name if engine else "",
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
    widths = [max(len(column), *(len(row[index]) for row in values)) for index, column in enumerate(columns)]
    line = "  ".join(column.ljust(widths[index]) for index, column in enumerate(columns))
    rule = "  ".join("-" * width for width in widths)
    body = ["  ".join(value.ljust(widths[index]) for index, value in enumerate(row)) for row in values]
    return "\n".join([line, rule, *body])


def cmd_report(args: argparse.Namespace) -> int:
    if args.template != "sprt" or args.report != "score":
        raise SystemExit(f"unknown report: {args.template} {args.report}")
    payload = sprt_score_report(
        view=args.view,
        reference_net=args.reference_net,
        candidate_net=args.candidate_net,
        engine=args.engine or "",
        min_games=args.min_games,
        limit=args.limit,
    )
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(report_text(payload))
    return 0
