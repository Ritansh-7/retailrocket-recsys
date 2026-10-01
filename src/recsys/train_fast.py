"""Bounded local training path for the large Retailrocket CSV export."""

import json
import math
import os
from collections import Counter, defaultdict
from pathlib import Path

import mlflow
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn import config_context
from sklearn.neighbors import NearestNeighbors

from recsys.config import settings


def train(data_dir: Path, artifact_dir: Path) -> dict:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.read_csv(data_dir / "events.csv", nrows=300_000)
    frame = frame[frame.event.isin(["view", "addtocart", "transaction"])].copy()
    frame["timestamp"] = pd.to_numeric(frame["timestamp"])
    cutoff = frame.timestamp.quantile(0.8)
    train = frame[frame.timestamp <= cutoff]
    test = frame[frame.timestamp > cutoff]
    histories = train.groupby("visitorid").apply(
        lambda group: list(dict.fromkeys(group.sort_values("timestamp").itemid.tolist()))[-10:],
        include_groups=False,
    )
    popular = train.itemid.value_counts().head(10_000).index.astype(int).tolist()
    pairs: dict[int, Counter] = defaultdict(Counter)
    for items in histories:
        for source in items:
            for target in items:
                if source != target:
                    pairs[int(source)][int(target)] += 1
    neighbors = {
        str(source): [item for item, _ in counts.most_common(100)]
        for source, counts in pairs.items()
    }
    # Fit a scikit-learn item-item collaborative filtering model. Each item is
    # represented by the visitors who interacted with it, and cosine distance
    # finds items with similar visitor profiles.
    visitor_codes, _ = pd.factorize(train.visitorid)
    item_codes, item_ids = pd.factorize(train.itemid)
    interactions = csr_matrix(
        (
            [1.0] * len(train),
            (visitor_codes, item_codes),
        ),
        shape=(int(visitor_codes.max()) + 1, len(item_ids)),
    )
    item_matrix = interactions.T.tocsr()
    neighbor_count = min(101, item_matrix.shape[0])
    sklearn_model = NearestNeighbors(
        n_neighbors=neighbor_count, metric="cosine", algorithm="brute", n_jobs=1
    )
    sklearn_model.fit(item_matrix)
    with config_context(working_memory=16):
        distances, indices = sklearn_model.kneighbors(item_matrix)
    sklearn_neighbors = {
        str(int(item_ids[item_index])): [
            int(item_ids[index])
            for distance, index in zip(row_distances[1:], row_indices[1:], strict=False)
            if distance < 1.0
        ]
        for item_index, (row_distances, row_indices) in enumerate(
            zip(distances, indices, strict=False)
        )
    }
    (artifact_dir / "item_neighbors.json").write_text(json.dumps(neighbors), encoding="utf-8")
    (artifact_dir / "sklearn_neighbors.json").write_text(
        json.dumps(sklearn_neighbors), encoding="utf-8"
    )
    (artifact_dir / "popular_items.json").write_text(json.dumps(popular), encoding="utf-8")
    pd.DataFrame(
        {
            "source": [int(k) for k in neighbors for _ in neighbors[k]],
            "target": [item for k in neighbors for item in neighbors[k]],
        }
    ).to_parquet(artifact_dir / "item_neighbors.parquet")
    pd.DataFrame({"item_id": popular}).to_parquet(artifact_dir / "popular_items.parquet")
    visitor_features = (
        train.groupby("visitorid")
        .agg(event_count=("itemid", "size"), unique_items=("itemid", "nunique"))
        .reset_index()
        .rename(columns={"visitorid": "visitor_id"})
    )
    visitor_features["event_timestamp"] = pd.to_datetime(int(cutoff), unit="ms", utc=True)
    visitor_features.to_parquet(artifact_dir / "visitor_features.parquet", index=False)
    item_features = (
        train.groupby("itemid")
        .agg(interaction_count=("itemid", "size"), unique_visitors=("visitorid", "nunique"))
        .reset_index()
        .rename(columns={"itemid": "item_id"})
    )
    item_features["popularity_score"] = item_features.interaction_count.astype(float)
    item_features["event_timestamp"] = pd.to_datetime(int(cutoff), unit="ms", utc=True)
    item_features.to_parquet(artifact_dir / "item_features.parquet", index=False)
    control_hits = treatment_hits = control_ndcg = treatment_ndcg = users = 0
    heldout = test.groupby("visitorid").itemid.apply(set)
    for visitor, targets in heldout.items():
        history = histories.get(visitor, [])
        if not history:
            continue
        users += 1
        treatment = []
        for source in reversed(history):
            treatment.extend(sklearn_neighbors.get(str(source), []))
        treatment = list(dict.fromkeys([x for x in treatment if x not in history]))[:10]
        treatment += [x for x in popular if x not in history and x not in treatment][
            : 10 - len(treatment)
        ]
        control = [x for x in popular if x not in history][:10]
        for recs, kind in ((control, "control"), (treatment, "treatment")):
            ranks = [i for i, item in enumerate(recs, 1) if item in targets]
            if ranks:
                if kind == "control":
                    control_hits += 1
                    control_ndcg += 1 / math.log2(ranks[0] + 1)
                else:
                    treatment_hits += 1
                    treatment_ndcg += 1 / math.log2(ranks[0] + 1)
    denominator = max(users, 1)
    metrics = {
        "temporal_cutoff_ms": int(cutoff),
        "evaluation_users": users,
        "control_hit_rate_at_10": control_hits / denominator,
        "treatment_hit_rate_at_10": treatment_hits / denominator,
        "control_ndcg_at_10": control_ndcg / denominator,
        "treatment_ndcg_at_10": treatment_ndcg / denominator,
        "sklearn_model": "NearestNeighbors cosine item-item collaborative filtering",
    }
    (artifact_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    try:
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", settings.tracking_uri))
        mlflow.set_experiment(settings.experiment_name)
        with mlflow.start_run(run_name="sklearn-nearest-neighbors"):
            mlflow.log_params(
                {
                    "algorithm": "NearestNeighbors",
                    "metric": "cosine",
                    "n_neighbors": neighbor_count - 1,
                    "training_rows": len(train),
                }
            )
            mlflow.log_metrics(
                {
                    key: float(value)
                    for key, value in metrics.items()
                    if isinstance(value, int | float)
                }
            )
            # Metrics are logged directly so the run remains portable when the
            # training command runs outside the MLflow container.
    except Exception as exc:
        print(f"MLflow logging skipped: {exc}")
        if mlflow.active_run() is not None:
            try:
                mlflow.end_run(status="FINISHED")
            except Exception:
                pass
    return metrics


if __name__ == "__main__":
    print(json.dumps(train(settings.data_dir, settings.artifact_dir), indent=2))
