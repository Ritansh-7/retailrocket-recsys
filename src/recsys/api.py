import json
import logging
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Response

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except (ImportError, ModuleNotFoundError):
    KafkaProducer = None

    class KafkaError(Exception):
        pass


from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field

from recsys.assignment import assign_variant
from recsys.config import settings
from recsys.model import Recommender

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
REQUESTS = Counter("recsys_requests_total", "Recommendation requests", ["variant", "status"])
LATENCY = Histogram("recsys_request_duration_seconds", "Recommendation latency in seconds")
EVENTS = Counter("recsys_events_published_total", "Events published to Kafka", ["event_type"])
MODEL: Recommender | None = None
PRODUCER: Any = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global MODEL, PRODUCER
    try:
        MODEL = Recommender(settings.artifact_dir)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.error("Recommendation artifacts are missing or invalid: %s", exc)
    try:
        if KafkaProducer is None:
            raise KafkaError("Kafka client is unavailable")
        PRODUCER = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode(),
            acks="all",
            retries=5,
            max_in_flight_requests_per_connection=1,
        )
    except KafkaError:
        logger.exception("Kafka is unavailable at startup; event ingestion will return 503")
    yield
    if PRODUCER is not None:
        PRODUCER.flush(timeout=5)
        PRODUCER.close(timeout=5)


app = FastAPI(
    title="Retailrocket Recommendation Experiment API", version="0.1.0", lifespan=lifespan
)


class RecommendationRequest(BaseModel):
    visitor_id: int = Field(gt=0)
    experiment_id: str = Field(min_length=1, max_length=100)
    recent_item_ids: list[int] = Field(default_factory=list, max_length=100)
    count: int = Field(default=10, ge=1, le=50)


class EventRequest(BaseModel):
    visitor_id: int = Field(gt=0)
    experiment_id: str = Field(min_length=1, max_length=100)
    variant: Literal["control", "treatment"]
    event_type: Literal["assignment", "impression", "click", "add_to_cart", "transaction"]
    item_id: int | None = None
    purchase_amount: float | None = Field(default=None, ge=0)
    timestamp_ms: int | None = None


def publish(payload: dict) -> None:
    if PRODUCER is None:
        raise HTTPException(status_code=503, detail="Event broker is not available")
    payload.setdefault("event_id", uuid.uuid4().hex)
    try:
        PRODUCER.send(
            settings.kafka_topic, value=payload, key=str(payload["visitor_id"]).encode()
        ).get(timeout=5)
    except KafkaError as exc:
        logger.exception("Kafka publish failed")
        raise HTTPException(status_code=503, detail="Could not publish event") from exc
    EVENTS.labels(payload["event_type"]).inc()


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if settings.api_key and (
        x_api_key is None or not secrets.compare_digest(x_api_key, settings.api_key)
    ):
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    if MODEL is None:
        raise HTTPException(status_code=503, detail="Recommendation artifacts have not been loaded")
    return {"status": "ready"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/recommendations", dependencies=[Depends(require_api_key)])
def recommendations(request: RecommendationRequest):
    started = time.perf_counter()
    if MODEL is None:
        raise HTTPException(status_code=503, detail="Recommendation artifacts are not available")
    variant = assign_variant(request.visitor_id, request.experiment_id, settings.assignment_salt)
    try:
        items = MODEL.recommend(request.recent_item_ids, variant, request.count)
        timestamp_ms = int(time.time() * 1000)
        publish(
            {
                "visitor_id": request.visitor_id,
                "experiment_id": request.experiment_id,
                "variant": variant,
                "event_type": "assignment",
                "timestamp_ms": timestamp_ms,
            }
        )
        for item in items:
            publish(
                {
                    "visitor_id": request.visitor_id,
                    "experiment_id": request.experiment_id,
                    "variant": variant,
                    "event_type": "impression",
                    "item_id": item,
                    "timestamp_ms": timestamp_ms,
                }
            )
        REQUESTS.labels(variant, "success").inc()
        return {
            "visitor_id": request.visitor_id,
            "experiment_id": request.experiment_id,
            "variant": variant,
            "item_ids": items,
            "timestamp_ms": timestamp_ms,
        }
    except HTTPException:
        REQUESTS.labels(variant, "error").inc()
        raise
    finally:
        LATENCY.observe(time.perf_counter() - started)


@app.post("/events", status_code=202, dependencies=[Depends(require_api_key)])
def ingest_event(request: EventRequest):
    expected = assign_variant(request.visitor_id, request.experiment_id, settings.assignment_salt)
    if request.variant != expected:
        raise HTTPException(status_code=409, detail="Variant does not match the stable assignment")
    payload = request.model_dump(exclude_none=True)
    payload.setdefault("timestamp_ms", int(time.time() * 1000))
    publish(payload)
    return {"accepted": True}
