"""Log the serving recommender to MLflow, register it, and keep only the latest run."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import mlflow
import mlflow.pyfunc
import pandas as pd

from recsys.config import settings
from recsys.model import Recommender

REGISTERED_MODEL_NAME = "retailrocket-item-neighbors"


class RecsysPyfunc(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        self.recommender = Recommender(Path(context.artifacts["artifact_dir"]))

    def predict(self, context, model_input: pd.DataFrame):
        recommendations = []
        for _, row in model_input.iterrows():
            raw = row.get("recent_item_ids", "")
            if isinstance(raw, list):
                history = [int(item) for item in raw]
            else:
                history = [int(item) for item in str(raw).split(",") if str(item).strip().isdigit()]
            variant = str(row.get("variant", "treatment"))
            count = int(row.get("count", 10))
            recommendations.append(self.recommender.recommend(history, variant, count))
        return recommendations


def _numeric_metrics(metrics: dict) -> dict[str, float]:
    return {key: float(value) for key, value in metrics.items() if isinstance(value, int | float)}


def _staging_dir(artifact_dir: Path) -> Path:
    staged = Path(tempfile.mkdtemp(prefix="recsys-mlflow-"))
    for name in (
        "metrics.json",
        "popular_items.json",
        "item_neighbors.json",
        "sklearn_neighbors.json",
    ):
        source = artifact_dir / name
        if source.is_file():
            shutil.copy2(source, staged / name)
    return staged


def publish_run(
    artifact_dir: Path,
    metrics: dict,
    params: dict | None = None,
    run_name: str = "sklearn-nearest-neighbors",
) -> str | None:
    tracking_uri = settings.tracking_uri
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(settings.experiment_name)
    staged = _staging_dir(artifact_dir)
    try:
        with mlflow.start_run(run_name=run_name) as run:
            mlflow.log_params(params or {})
            mlflow.log_metrics(_numeric_metrics(metrics))
            mlflow.log_dict(metrics, "metrics.json")
            mlflow.pyfunc.log_model(
                artifact_path="model",
                python_model=RecsysPyfunc(),
                artifacts={"artifact_dir": str(staged)},
            )
            run_id = run.info.run_id
            model_uri = f"runs:/{run_id}/model"
            result = mlflow.register_model(model_uri, REGISTERED_MODEL_NAME)
            client = mlflow.MlflowClient()
            client.set_registered_model_alias(
                REGISTERED_MODEL_NAME, "champion", result.version
            )
            return run_id
    finally:
        shutil.rmtree(staged, ignore_errors=True)


def publish_from_artifacts() -> dict:
    artifact_dir = settings.artifact_dir
    metrics_path = artifact_dir / "metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.is_file() else {}
    run_id = publish_run(
        artifact_dir,
        metrics,
        params={
            "algorithm": "NearestNeighbors",
            "metric": "cosine",
            "serving_model": "Recommender",
            "training_source": "artifacts",
        },
    )
    return {"run_id": run_id, "metrics": metrics, "registered_model": REGISTERED_MODEL_NAME}


if __name__ == "__main__":
    print(json.dumps(publish_from_artifacts(), indent=2, default=str))
