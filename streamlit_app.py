"""Streamlit dashboard for recommendation demos and experiment metrics."""

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

from recsys.assignment import assign_variant
from recsys.model import Recommender

ROOT = Path(__file__).resolve().parent
ARTIFACT_DIR = Path(os.getenv("RECSYS_ARTIFACT_DIR", ROOT / "artifacts"))
GITHUB_REPO = "https://github.com/Ritansh-7/retailrocket-recsys"
GRAFANA_URL = os.getenv("RECSYS_GRAFANA_URL", "http://localhost:3000")
PROMETHEUS_URL = os.getenv("RECSYS_PROMETHEUS_URL", "http://localhost:9090")
MLFLOW_URL = os.getenv("RECSYS_MLFLOW_URL", "http://localhost:5000")
MLFLOW_MODELS_URL = os.getenv("RECSYS_MLFLOW_MODELS_URL", f"{MLFLOW_URL}/#/models")
KAFKA_UI_URL = os.getenv("RECSYS_KAFKA_UI_URL", "http://localhost:8080")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("RECSYS_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

st.set_page_config(
    page_title="RetailRocket recommender", page_icon=":material/analytics:", layout="wide"
)


@st.cache_resource
def load_recommender(artifact_dir: str) -> Recommender:
    return Recommender(Path(artifact_dir))


@st.cache_data
def load_metrics(path: str) -> dict:
    metrics_path = Path(path)
    if not metrics_path.is_file():
        return {}
    return json.loads(metrics_path.read_text(encoding="utf-8"))


def format_rate(value: float | int) -> str:
    return f"{float(value):.2%}"


def render_overview(metrics: dict) -> None:
    st.subheader("Offline evaluation")
    st.caption(
        "Temporal holdout metrics from the trained RetailRocket interaction model. "
        "Treatment uses scikit-learn NearestNeighbors with cosine similarity."
    )
    control_rate = metrics.get("control_hit_rate_at_10", 0.0)
    treatment_rate = metrics.get("treatment_hit_rate_at_10", 0.0)
    lift = treatment_rate - control_rate
    with st.container(horizontal=True):
        st.metric("Control hit rate@10", format_rate(control_rate), border=True)
        st.metric("Treatment hit rate@10", format_rate(treatment_rate), border=True)
        st.metric("Absolute lift", format_rate(lift), border=True)
        st.metric("Evaluation visitors", f"{metrics.get('evaluation_users', 0):,}", border=True)
    with st.container(border=True):
        st.subheader("Policy comparison")
        st.progress(control_rate, text=f"Control · popular · {format_rate(control_rate)}")
        st.progress(
            treatment_rate,
            text=f"Treatment · sklearn item-item · {format_rate(treatment_rate)}",
        )


def render_recommender() -> None:
    st.subheader("Recommendation playground")
    st.caption("Assignments are stable for the same visitor, experiment, and assignment salt.")
    with st.form("recommendation_form", border=True):
        visitor_id = st.number_input("Visitor ID", min_value=1, value=257597, step=1)
        experiment_id = st.text_input("Experiment ID", value="recsys-v1")
        recent_items = st.text_input("Recent item IDs", value="355908, 248676")
        count = st.slider("Recommendations", min_value=3, max_value=20, value=10)
        submitted = st.form_submit_button("Generate recommendations", type="primary")
    if not submitted:
        return
    try:
        history = [int(item.strip()) for item in recent_items.split(",") if item.strip()]
    except ValueError:
        st.error("Enter comma-separated numeric item IDs.", icon=":material/error:")
        return
    recommender = load_recommender(str(ARTIFACT_DIR))
    salt = os.getenv("RECSYS_ASSIGNMENT_SALT", "replace-this-before-production")
    variant = assign_variant(int(visitor_id), experiment_id, salt)
    recommendations = recommender.recommend(history, variant, count)
    st.success(f"Assigned variant: {variant}", icon=":material/check_circle:")
    table = pd.DataFrame({"rank": range(1, len(recommendations) + 1), "item_id": recommendations})
    st.dataframe(table, hide_index=True, width="stretch")


def main() -> None:
    st.title("RetailRocket recommender")
    st.caption("Recommendation system experiment console")
    with st.sidebar:
        st.markdown("### Navigation")
        page = st.radio(
            "View", ["Overview", "Recommendation playground"], label_visibility="collapsed"
        )
        st.caption(f"Artifacts: {ARTIFACT_DIR}")
        st.markdown("### Platform links")
        st.link_button("Open Kafka UI", KAFKA_UI_URL, icon=":material/hub:", width="stretch")
        st.caption(f"Kafka broker: `{KAFKA_BOOTSTRAP_SERVERS}`")
        if GRAFANA_URL:
            st.link_button(
                "Open Grafana", GRAFANA_URL, icon=":material/dashboard:", width="stretch"
            )
        if PROMETHEUS_URL:
            st.link_button(
                "Open Prometheus",
                PROMETHEUS_URL,
                icon=":material/query_stats:",
                width="stretch",
            )
        st.link_button("Open MLflow", MLFLOW_URL, icon=":material/monitoring:", width="stretch")
        st.link_button(
            "Open MLflow Model Registry",
            MLFLOW_MODELS_URL,
            icon=":material/model_training:",
            width="stretch",
        )
        st.link_button(
            "Open DVC pipeline definition",
            f"{GITHUB_REPO}/blob/main/dvc.yaml",
            icon=":material/account_tree:",
            width="stretch",
        )
        st.caption("DVC is managed from the terminal: `dvc status` or `dvc repro`.")
    metrics = load_metrics(str(ARTIFACT_DIR / "metrics.json"))
    if not metrics:
        st.warning(
            "No metrics found. Run the DVC training pipeline first.", icon=":material/warning:"
        )
    if page == "Overview":
        render_overview(metrics)
    else:
        render_recommender()


if __name__ == "__main__":
    main()
