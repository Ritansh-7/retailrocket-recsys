"""Train item co-visitation recommendations and run a temporal offline evaluation."""

import json
import os
from pathlib import Path

import duckdb
import mlflow
import pandas as pd

from recsys.config import settings
from recsys.model import Recommender


def train(data_dir: Path, artifact_dir: Path) -> dict[str, float | int]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    events = (data_dir / "events.csv").resolve().as_posix()
    sql_events = events.replace("'", "''")

    def sql_path(path: Path) -> str:
        return path.resolve().as_posix().replace("'", "''")

    con = duckdb.connect()
    con.execute("SET threads TO 4")
    con.execute("SET memory_limit = '6GB'")
    con.execute(
        f"""CREATE TEMP TABLE events AS
        SELECT CAST(timestamp AS BIGINT) AS ts, CAST(visitorid AS BIGINT) AS visitor_id,
               CAST(itemid AS BIGINT) AS item_id, event
        FROM read_csv_auto('{sql_events}') WHERE event IN ('view', 'addtocart', 'transaction')
        LIMIT 300000"""
    )
    cutoff = con.execute("SELECT quantile_cont(ts, 0.8) FROM events").fetchone()[0]
    con.execute(
        """CREATE TEMP TABLE baskets AS
        SELECT visitor_id, list(item_id ORDER BY last_seen DESC) AS items
        FROM (SELECT visitor_id, item_id, max(ts) AS last_seen FROM events WHERE ts <= ?
              AND visitor_id IN (SELECT visitor_id FROM events WHERE ts <= ?
                GROUP BY visitor_id ORDER BY hash(visitor_id) LIMIT 1000)
              GROUP BY visitor_id, item_id) GROUP BY visitor_id""",
        [cutoff, cutoff],
    )
    con.execute(
        """CREATE TEMP TABLE bounded_baskets AS
        SELECT visitor_id, list_slice(items, 1, 10) AS items FROM baskets"""
    )
    con.execute(
        """CREATE TEMP TABLE item_pairs AS
        SELECT src.item_id AS source, dst.item_id AS target, count(*) AS co_visits
        FROM bounded_baskets b
        CROSS JOIN UNNEST(b.items) AS src(item_id)
        CROSS JOIN UNNEST(b.items) AS dst(item_id)
        WHERE src.item_id <> dst.item_id
        GROUP BY source, target"""
    )
    con.execute(
        f"""COPY (
          SELECT source, target FROM (
            SELECT source, target, row_number() OVER (PARTITION BY source
              ORDER BY co_visits DESC, target) AS rank
            FROM item_pairs
          ) WHERE rank <= 100 ORDER BY source, rank
        ) TO '{sql_path(artifact_dir / "item_neighbors.parquet")}' (FORMAT PARQUET, COMPRESSION ZSTD)"""  # noqa: E501
    )
    con.execute(
        f"""COPY (
          SELECT item_id FROM (SELECT item_id, count(*) AS popularity FROM events
            WHERE ts <= ? GROUP BY item_id ORDER BY popularity DESC, item_id LIMIT 10000)
          ORDER BY popularity DESC, item_id
        ) TO '{sql_path(artifact_dir / "popular_items.parquet")}' (FORMAT PARQUET, COMPRESSION ZSTD)""",  # noqa: E501
        [cutoff],
    )
    con.execute(
        f"""COPY (SELECT visitor_id, epoch_ms(max(ts)) AS event_timestamp,
          count(*)::BIGINT AS event_count, count(DISTINCT item_id)::BIGINT AS unique_items
          FROM events WHERE ts <= ? GROUP BY visitor_id)
          TO '{sql_path(artifact_dir / "visitor_features.parquet")}' (FORMAT PARQUET, COMPRESSION ZSTD)""",  # noqa: E501
        [cutoff],
    )
    con.execute(
        f"""COPY (SELECT item_id, epoch_ms(max(ts)) AS event_timestamp,
          count(*)::BIGINT AS interaction_count, count(DISTINCT visitor_id)::BIGINT AS unique_visitors,
          count(*)::DOUBLE AS popularity_score FROM events WHERE ts <= ? GROUP BY item_id)
          TO '{sql_path(artifact_dir / "item_features.parquet")}' (FORMAT PARQUET, COMPRESSION ZSTD)""",  # noqa: E501
        [cutoff],
    )
    neighbors_frame = pd.read_parquet(artifact_dir / "item_neighbors.parquet")
    neighbors = neighbors_frame.groupby("source", sort=False)["target"].apply(list).to_dict()
    popular = pd.read_parquet(artifact_dir / "popular_items.parquet")["item_id"].tolist()
    (artifact_dir / "item_neighbors.json").write_text(
        json.dumps({str(k): v for k, v in neighbors.items()}, separators=(",", ":")),
        encoding="utf-8",
    )
    (artifact_dir / "popular_items.json").write_text(json.dumps(popular), encoding="utf-8")

    test_users = con.execute(
        """SELECT visitor_id, list(item_id ORDER BY ts) AS heldout
        FROM events WHERE ts > ? GROUP BY visitor_id HAVING count(*) >= 1
        ORDER BY hash(visitor_id) LIMIT 2000""",
        [cutoff],
    ).fetchall()
    histories = con.execute(
        """SELECT visitor_id, list(item_id ORDER BY last_seen) AS history FROM (
          SELECT visitor_id, item_id, max(ts) last_seen FROM events WHERE ts <= ?
          AND visitor_id IN (SELECT visitor_id FROM (SELECT visitor_id FROM events WHERE ts > ?
            GROUP BY visitor_id ORDER BY hash(visitor_id) LIMIT 2000))
          GROUP BY visitor_id, item_id) GROUP BY visitor_id""",
        [cutoff, cutoff],
    ).fetchall()
    history_map = {visitor: items for visitor, items in histories}
    recommender = Recommender(artifact_dir)
    totals = {"control_hits": 0, "treatment_hits": 0, "control_ndcg": 0.0, "treatment_ndcg": 0.0}
    users_evaluated = 0
    for visitor, heldout in test_users:
        history = history_map.get(visitor, [])
        if not history or not heldout:
            continue
        targets = set(heldout)
        for variant in ("control", "treatment"):
            recs = recommender.recommend(history, variant, limit=10)
            ranks = [rank for rank, item in enumerate(recs, start=1) if item in targets]
            if ranks:
                totals[f"{variant}_hits"] += 1
                totals[f"{variant}_ndcg"] += 1.0 / __import__("math").log2(ranks[0] + 1)
        users_evaluated += 1
    denominator = max(users_evaluated, 1)
    metrics: dict[str, float | int] = {
        "temporal_cutoff_ms": int(cutoff),
        "evaluation_users": users_evaluated,
        "control_hit_rate_at_10": totals["control_hits"] / denominator,
        "treatment_hit_rate_at_10": totals["treatment_hits"] / denominator,
        "control_ndcg_at_10": totals["control_ndcg"] / denominator,
        "treatment_ndcg_at_10": totals["treatment_ndcg"] / denominator,
    }
    (artifact_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", settings.tracking_uri)
    try:
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(settings.experiment_name)
        with mlflow.start_run(run_name="temporal-item-covisitation"):
            mlflow.log_param("train_cutoff_timestamp_ms", int(cutoff))
            mlflow.log_param("recommendation_count", 10)
            mlflow.log_metrics({k: float(v) for k, v in metrics.items() if k != "evaluation_users"})
            mlflow.log_artifacts(str(artifact_dir))
    except Exception as exc:  # MLflow is optional when running the local DVC pipeline.
        print(f"MLflow logging skipped: {exc}")
    con.close()
    return metrics


if __name__ == "__main__":
    result = train(settings.data_dir, settings.artifact_dir)
    print(json.dumps(result, indent=2))
