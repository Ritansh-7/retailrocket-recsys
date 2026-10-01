# Retailrocket recommender and live experiment service

This repository builds a product recommender from Retailrocket interaction events and serves a stable control/treatment experiment through FastAPI. It is an end-to-end project scaffold for a production-style deployment; the public historical dataset itself is observational and does not establish causal A/B results.

## What the provided files support

- `events.csv`: 2.76 million views, add-to-cart events, and transactions, with visitor, item, and millisecond timestamp fields. Used for model training and temporal offline evaluation.
- `category_tree.csv` and both item-property extracts: schema checked and retained as DVC inputs for later item metadata features.
- `customer_acquisition_data.csv`: excluded from this recommender pipeline because it is an unrelated, aggregated acquisition table.

The offline baseline recommends popular products. The treatment recommender uses item co-visitation from each visitor's recent history. Training splits the event timeline, evaluates both policies on later events, and writes the hit rate and NDCG metrics. Those are offline ranking metrics, not a randomized experiment result.

## Local setup

1. Create a Python 3.12 or 3.13 virtual environment and run `pip install -e .` (this installs the app and base dependencies). Feast is optional and isolated in `requirements-feast.txt` because its current pinned release requires an older PyArrow version.
2. Copy `.env.example` to `.env` and replace both secrets with unique random values.
3. Keep the supplied source files in `data/raw/` (already copied here). Track them with `dvc add -f data/raw`, then configure and push to a shared DVC remote before expecting another machine or GitHub runner to retrieve them.
4. On Windows, run `dvc repro` from the included `.venv`; the DVC stages use `.venv/Scripts/python.exe` explicitly. Artifacts are written under `artifacts/` and the run is recorded in MLflow when the tracking server is available.
5. Run `docker compose up --build -d`. The API, MLflow, Prometheus, and Grafana bind to localhost; Kafka stays on the internal Docker network. Grafana is at `http://localhost:3000`, MLflow at `http://localhost:5000`, Prometheus at `http://localhost:9090`, and the API at `http://localhost:8000`.

Recommendation request example:

```json
{
  "visitor_id": 12345,
  "experiment_id": "recsys-v1",
  "recent_item_ids": [355908, 248676],
  "count": 10
}
```

Send it to `POST /recommendations` with the `X-API-Key` header set to `RECSYS_API_KEY`. Assignment is deterministic per visitor, experiment, and secret salt. Recommendation assignments and impressions are published to Kafka; the event sink persists append-only daily JSONL files. Send click, add-to-cart, and transaction outcomes to `POST /events` using the returned variant. Do not send direct personal identifiers.

To analyze live outcomes, run `python -m recsys.analyze "data/events/events-*.jsonl"`. It reports visitor-level click and purchase rate tests and Welch's test for revenue per visitor. Run it only after a real randomized serving period, with both assignment groups and outcomes collected.

## Project components

- DVC tracks the pipeline and generated model artifacts. A shared DVC remote still needs to be configured for team and GitHub runner access.
- DuckDB scans CSV inputs and builds Parquet artifacts without a database server.
- MLflow stores training parameters, metrics, and artifacts in its file-backed local store.
- Feast definitions describe offline visitor/item features in `feature_repo/`. The serving path takes recent item IDs in the request and does not use an online feature database.
- Kafka transports assignments, impressions, and outcomes; the consumer writes them to append-only JSONL.
- FastAPI serves recommendations and event ingestion; Prometheus and Grafana monitor API load and latency.
- Streamlit provides the local dashboard at `http://localhost:8501`, with offline policy metrics and a recommendation playground. GitHub Actions runs linting, Python compilation, Streamlit checks, and a container build. The manually triggered release workflow publishes to GHCR and can restart API, event sink, MLflow, and Streamlit services over SSH.

## Release prerequisites

Configure GitHub Actions secrets `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_PATH`, and `DEPLOY_SSH_KEY`; provision the host with Docker Compose, this repository, `.env`, model artifacts, and GHCR image pull access. Configure a DVC remote with CI credentials before moving model training into hosted workflows. Put the API behind a TLS reverse proxy and access control before exposing it beyond localhost. The single-node Kafka Compose setup and local file event sink are suitable for a small demonstration; scale-out production needs durable shared object storage and a multi-broker Kafka deployment.
