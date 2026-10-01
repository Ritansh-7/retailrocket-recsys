"""Fail fast on schema and value problems in the provided Retailrocket extracts."""

import json
from pathlib import Path

import duckdb

from recsys.config import settings

EXPECTED = {
    "events.csv": {"timestamp", "visitorid", "event", "itemid", "transactionid"},
    "category_tree.csv": {"categoryid", "parentid"},
    "item_properties_part1.csv": {"timestamp", "itemid", "property", "value"},
    "item_properties_part2.csv": {"timestamp", "itemid", "property", "value"},
}


def validate(data_dir: Path) -> dict[str, int]:
    for name, expected in EXPECTED.items():
        path = data_dir / name
        if not path.is_file():
            raise FileNotFoundError(f"Required dataset file is missing: {path}")
        columns = set(
            duckdb.sql("SELECT * FROM read_csv_auto(?) LIMIT 0", params=[str(path)]).columns
        )
        missing = expected - columns
        if missing:
            raise ValueError(f"{name} is missing columns: {sorted(missing)}")
    events_path = (data_dir / "events.csv").resolve().as_posix()
    row = duckdb.sql(
        """SELECT count(*) AS rows, count_if(visitorid IS NULL OR itemid IS NULL) AS bad_ids,
          count_if(event NOT IN ('view', 'addtocart', 'transaction')) AS unknown_events
        FROM read_csv_auto(?)""",
        params=[events_path],
    ).fetchone()
    summary = {
        "event_rows": row[0],
        "events_with_missing_ids": row[1],
        "unknown_event_types": row[2],
    }
    if not summary["event_rows"] or summary["events_with_missing_ids"]:
        raise ValueError(f"Invalid event data: {summary}")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    validate(settings.data_dir)
