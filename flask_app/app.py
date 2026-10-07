"""
Flask webpage: reads the rows Spark stored in Cassandra and displays them.
  GET /           -> dashboard page (refreshes itself every 10 s)
  GET /api/data   -> JSON with the latest stored rows from both tables

Run:  python app.py   then open http://localhost:5000
Env:  CASSANDRA_HOST (default 127.0.0.1), CASSANDRA_PORT (default 9042)
"""

import os
from datetime import datetime, timedelta, timezone

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from flask import Flask, jsonify, render_template

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = "crypto_pipeline"
RECORD_LIMIT = 200      # raw stored rows shown in the scrollable log
GLOBAL_LIMIT = 60       # global snapshots shown

app = Flask(__name__)
_session = None


def session():
    """Connect lazily so the page can show a clear error if Cassandra is not up yet."""
    global _session
    if _session is None:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, connect_timeout=5)
        s = cluster.connect(KEYSPACE)
        s.row_factory = dict_factory
        _session = s
    return _session


def day_partitions():
    """Today and yesterday (UTC): the partition keys used by both tables."""
    today = datetime.now(timezone.utc).date()
    return [today.isoformat(), (today - timedelta(days=1)).isoformat()]


def query_recent(table: str, limit: int) -> list[dict]:
    rows: list[dict] = []
    for day in day_partitions():
        if len(rows) >= limit:
            break
        rows += list(session().execute(
            f"SELECT * FROM {table} WHERE capture_date = %s LIMIT %s", (day, limit - len(rows))))
    return rows


def count_today(table: str) -> int:
    row = session().execute(f"SELECT COUNT(*) AS n FROM {table} WHERE capture_date = %s",
                            (day_partitions()[0],)).one()
    return row["n"] if row else 0


def to_json(row: dict) -> dict:
    """Cassandra returns naive UTC datetimes; send them as ISO strings ending in Z."""
    return {k: (v.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
                if isinstance(v, datetime) else v)
            for k, v in row.items()}


@app.route("/")
def index():
    return render_template("index.html", host=f"{CASSANDRA_HOST}:{CASSANDRA_PORT}")


@app.route("/api/data")
def api_data():
    global _session
    try:
        records = query_recent("market_prices", RECORD_LIMIT)
        history = query_recent("global_stats", GLOBAL_LIMIT)

        # newest stored row per coin, ordered by market-cap rank
        latest: dict[str, dict] = {}
        for r in records:                      # rows arrive newest first
            latest.setdefault(r["coin_id"], r)
        latest_sorted = sorted(latest.values(), key=lambda r: (r.get("market_cap_rank") or 9999))

        newest_write = max((r["processed_at"] for r in records + history if r.get("processed_at")), default=None)

        return jsonify({
            "ok": True,
            "server_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "last_write": newest_write.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            if newest_write else None,
            "counts": {"market_prices": count_today("market_prices"),
                       "global_stats": count_today("global_stats")},
            "latest": [to_json(r) for r in latest_sorted],
            "records": [to_json(r) for r in records],
            "global_latest": to_json(history[0]) if history else None,
            "global_history": [to_json(r) for r in history],
        })
    except Exception as exc:
        _session = None    # reconnect on the next request
        return jsonify({"ok": False,
                        "error": f"Can't read Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}: {exc}"}), 503


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
