"""
Producer 2: Global market statistics
Captures the whole-market snapshot from CoinGecko /global every INTERVAL seconds
and sends one JSON message to the Kafka topic `crypto_global_stats`.

Run:  python producer_global_stats.py
Env:  KAFKA_BOOTSTRAP (default localhost:9092), GLOBAL_INTERVAL (default 30),
      COINGECKO_API_KEY (optional)
"""

import os
import signal
import sys
import time
from datetime import datetime, timezone

from common import fetch_json, make_producer, now_utc_iso, send_json

TOPIC = "crypto_global_stats"
INTERVAL = int(os.getenv("GLOBAL_INTERVAL", "30"))

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def capture() -> dict:
    """Capture and structure the global snapshot (10 fields)."""
    data = fetch_json("/global").get("data", {})
    updated = data.get("updated_at")
    return {
        "active_cryptocurrencies": data.get("active_cryptocurrencies"),
        "markets": data.get("markets"),
        "total_market_cap_usd": (data.get("total_market_cap") or {}).get("usd"),
        "total_volume_usd": (data.get("total_volume") or {}).get("usd"),
        "btc_dominance_pct": (data.get("market_cap_percentage") or {}).get("btc"),
        "eth_dominance_pct": (data.get("market_cap_percentage") or {}).get("eth"),
        "market_cap_change_pct_24h": data.get("market_cap_change_percentage_24h_usd"),
        # CoinGecko gives a unix timestamp here; convert to ISO so Spark parses both topics the same way
        "updated_at": (datetime.fromtimestamp(updated, timezone.utc).isoformat().replace("+00:00", "Z")
                       if updated else None),
        "captured_at": now_utc_iso(),
        "source": "coingecko/global",
    }


def main():
    producer = make_producer("producer-global-stats")
    print(f"[Producer 2] Global market stats -> topic '{TOPIC}' every {INTERVAL}s")
    cycle = 0
    while running:
        cycle += 1
        started = time.time()
        try:
            snap = capture()
            print(f"\n[Producer 2] cycle {cycle} at {snap['captured_at']}")
            print(f"  total market cap  ${snap['total_market_cap_usd'] or 0:,.0f}")
            print(f"  24h volume        ${snap['total_volume_usd'] or 0:,.0f}")
            print(f"  BTC dominance     {snap['btc_dominance_pct'] or 0:.2f}%")
            print(f"  24h change        {snap['market_cap_change_pct_24h'] or 0:+.2f}%")
            send_json(producer, TOPIC, "global", snap)
            producer.flush(10)
        except Exception as exc:
            print(f"[Producer 2] cycle {cycle} skipped: {exc}")

        while running and time.time() - started < INTERVAL:
            time.sleep(0.5)

    producer.flush(10)
    print("[Producer 2] stopped.")
    sys.exit(0)


if __name__ == "__main__":
    main()
