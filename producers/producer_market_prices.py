"""
Producer 1: Coin market prices
Captures the top N coins from CoinGecko /coins/markets every INTERVAL seconds
and sends one JSON message per coin to the Kafka topic `crypto_market_prices`.

Run:  python producer_market_prices.py
Env:  KAFKA_BOOTSTRAP (default localhost:9092), MARKET_INTERVAL (default 30),
      TOP_N_COINS (default 20), COINGECKO_API_KEY (optional)
"""

import os
import signal
import sys
import time

from common import fetch_json, make_producer, now_utc_iso, send_json

TOPIC = "crypto_market_prices"
INTERVAL = int(os.getenv("MARKET_INTERVAL", "30"))
TOP_N = int(os.getenv("TOP_N_COINS", "20"))

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def capture() -> list[dict]:
    """Capture and structure the coin rows (13 fields per coin)."""
    rows = fetch_json("/coins/markets", {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_N,
        "page": 1,
        "price_change_percentage": "24h",
    })
    captured_at = now_utc_iso()
    return [{
        "coin_id": c.get("id"),
        "symbol": (c.get("symbol") or "").upper(),
        "name": c.get("name"),
        "current_price": c.get("current_price"),
        "market_cap": c.get("market_cap"),
        "market_cap_rank": c.get("market_cap_rank"),
        "total_volume": c.get("total_volume"),
        "high_24h": c.get("high_24h"),
        "low_24h": c.get("low_24h"),
        "price_change_pct_24h": c.get("price_change_percentage_24h"),
        "last_updated": c.get("last_updated"),
        "captured_at": captured_at,
        "source": "coingecko/coins/markets",
    } for c in rows]


def main():
    producer = make_producer("producer-market-prices")
    print(f"[Producer 1] Coin market prices -> topic '{TOPIC}' every {INTERVAL}s (top {TOP_N} coins)")
    cycle = 0
    while running:
        cycle += 1
        started = time.time()
        try:
            coins = capture()
            print(f"\n[Producer 1] cycle {cycle} at {now_utc_iso()}: captured {len(coins)} coins")
            for coin in coins:
                print(f"  {coin['market_cap_rank'] or '-':>3}. {coin['symbol']:<6} "
                      f"${coin['current_price'] or 0:<14,} 24h {coin['price_change_pct_24h'] or 0:+.2f}%")
                send_json(producer, TOPIC, coin["coin_id"], coin)
            producer.flush(10)
        except Exception as exc:  # keep streaming even if one cycle fails
            print(f"[Producer 1] cycle {cycle} skipped: {exc}")

        # sleep in small steps so Ctrl+C stops quickly
        while running and time.time() - started < INTERVAL:
            time.sleep(0.5)

    producer.flush(10)
    print("[Producer 1] stopped.")
    sys.exit(0)


if __name__ == "__main__":
    main()
