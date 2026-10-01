"""Send a steady recommendation load so Prometheus and Grafana have live series."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

API_URL = os.getenv("RECSYS_API_URL", "http://127.0.0.1:8000").rstrip("/")
API_KEY = os.getenv("RECSYS_API_KEY", "")
INTERVAL_SEC = float(os.getenv("RECSYS_LOADGEN_INTERVAL_SEC", "5"))


def post(path: str, body: dict) -> dict:
    request = urllib.request.Request(
        f"{API_URL}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "X-API-Key": API_KEY},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode())


def wait_for_ready() -> None:
    while True:
        try:
            urllib.request.urlopen(f"{API_URL}/readyz", timeout=3)
            return
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(2)


def main() -> None:
    wait_for_ready()
    visitor = 10_000
    while True:
        visitor += 1
        try:
            rec = post(
                "/recommendations",
                {
                    "visitor_id": visitor,
                    "experiment_id": "recsys-v1",
                    "recent_item_ids": [355908, 248676],
                    "count": 5,
                },
            )
            items = rec.get("item_ids") or []
            if items:
                post(
                    "/events",
                    {
                        "visitor_id": visitor,
                        "experiment_id": "recsys-v1",
                        "variant": rec["variant"],
                        "event_type": "click",
                        "item_id": items[0],
                    },
                )
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            print(f"loadgen retry: {exc}", flush=True)
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
