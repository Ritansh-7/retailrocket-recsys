"""Persist Kafka experiment events as append-only JSONL files; no database required."""

import json
import logging
import os
from datetime import UTC, datetime

from kafka import KafkaConsumer

from recsys.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run() -> None:
    settings.event_log_dir.mkdir(parents=True, exist_ok=True)
    consumer = KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        group_id="experiment-event-sink-v1",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    while True:
        for messages in consumer.poll(timeout_ms=1000).values():
            by_day: dict[str, list[dict]] = {}
            for message in messages:
                event = message.value
                day = datetime.fromtimestamp(event["timestamp_ms"] / 1000, UTC).date().isoformat()
                by_day.setdefault(day, []).append(event)
            for day, events in by_day.items():
                path = settings.event_log_dir / f"events-{day}.jsonl"
                with path.open("a", encoding="utf-8") as output:
                    for event in events:
                        output.write(json.dumps(event, separators=(",", ":")) + "\n")
                    output.flush()
                    os.fsync(output.fileno())
            consumer.commit()
            logger.info("Persisted %s Kafka events", sum(map(len, by_day.values())))


if __name__ == "__main__":
    run()
