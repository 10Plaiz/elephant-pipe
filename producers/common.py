"""Helpers shared by both producers: HTTP capture from CoinGecko and a Kafka producer."""

import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()   # optional free demo key


def now_utc_iso() -> str:
    """Current UTC time as ISO-8601 with milliseconds, e.g. 2026-10-07T08:15:30.123Z"""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def fetch_json(path: str, params: dict | None = None, retries: int = 4) -> dict | list:
    """GET a CoinGecko endpoint, backing off on rate limits (HTTP 429) and network errors."""
    headers = {"Accept": "application/json", "User-Agent": "crypto-pipeline-student-project/1.0"}
    if API_KEY:
        headers["x-cg-demo-api-key"] = API_KEY

    wait = 15
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(f"{COINGECKO_BASE}{path}", params=params, headers=headers, timeout=20)
            if resp.status_code == 429:
                print(f"  ! rate limited by CoinGecko, waiting {wait}s (attempt {attempt}/{retries})")
                time.sleep(wait)
                wait *= 2
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            print(f"  ! request failed: {exc} (attempt {attempt}/{retries})")
            time.sleep(wait)
            wait *= 2
    raise RuntimeError(f"CoinGecko {path} failed after {retries} attempts")


def make_producer(client_id: str) -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": client_id,
        "acks": "all",
        "linger.ms": 50,
    })


def delivery_report(err, msg):
    """Called once per message to confirm Kafka stored it (topic, partition, offset)."""
    if err is not None:
        print(f"  x delivery failed: {err}")
    else:
        print(f"  -> {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}  key={msg.key().decode()}")


def send_json(producer: Producer, topic: str, key: str, payload: dict) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)
