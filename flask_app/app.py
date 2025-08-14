# flask_app/app.py
from __future__ import annotations
from celery import chain
from flask import Flask, Response, json, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient, DESCENDING
from datetime import datetime

# If you still need your blueprint, keep this:
try:
    from routes.pair_routes import pair_bp
except Exception:
    pair_bp = None

# === Import the new task names from your refactored tasks.py ===
from celery_worker.tasks import (
    trigger_chain,
    fetch_and_store_prices_task,
    model_backfill_task,
    evaluate_and_place_trade_task,
)

app = Flask(__name__)
CORS(app)

if pair_bp:
    app.register_blueprint(pair_bp)

client = MongoClient("mongodb://mongo:27017")
db = client["trading_db"]


# ------------------------------
# Basic health
# ------------------------------
@app.route("/health")
def health():
    return {"status": "ok"}


@app.route("/test-db")
def test_db():
    db.test_collection.insert_one({"status": "working"})
    count = db.test_collection.count_documents({})
    return {"message": f"Mongo test successful. Docs in test_collection: {count}"}


# ------------------------------
# Task triggers (match new tasks)
# ------------------------------
@app.route("/run-cycle", methods=["POST", "GET"])
def run_cycle():
    """
    Kick off one full cycle: fetch -> model backfill -> evaluate/trade -> (optional cleanup)
    Optional query params:
      ?model=zscore|eg_ci  (default zscore)
      &strategy=zscore|cointegration (default zscore)
    """
    model = request.args.get("model", "zscore")
    strategy = request.args.get("strategy", "zscore")
    res = trigger_chain.apply_async(kwargs={"model_name": model, "strategy_name": strategy})
    return jsonify({"task_id": res.id, "status": "submitted", "model": model, "strategy": strategy})


@app.route("/fetch-prices", methods=["POST", "GET"])
def fetch_prices():
    """Manually fetch latest prices (7-day lookback window policy is in the task)."""
    res = fetch_and_store_prices_task.apply_async()
    return jsonify({"task_id": res.id, "status": "submitted"})


@app.route("/model-backfill", methods=["POST", "GET"])
def model_backfill():
    """Recompute model outputs for the entire available window and upsert."""
    model = request.args.get("model", "zscore")
    res = model_backfill_task.apply_async(kwargs={"model_name": model})
    return jsonify({"task_id": res.id, "status": "submitted", "model": model})


@app.route("/evaluate", methods=["POST", "GET"])
def evaluate_once():
    """Run evaluate/execute step only."""
    model = request.args.get("model", "zscore")
    strategy = request.args.get("strategy", "zscore")
    res = evaluate_and_place_trade_task.apply_async(kwargs={"model_name": model, "strategy_name": strategy})
    return jsonify({"task_id": res.id, "status": "submitted", "model": model, "strategy": strategy})


# ------------------------------
# Data APIs used by the frontend
# ------------------------------
@app.route("/trade-history", methods=["GET"])
def trade_history():
    """
    Returns all trades, oldest->newest.
    NOTE: With the refactor, strategy stores the generic metric as 'z_like'
          (could be z-score or residual z). We surface it as 'z_like'.
    """
    trades = db.trades.find().sort("timestamp", 1)

    def serialize_trade(t):
        ts = t.get("timestamp")
        return {
            "id": str(t["_id"]),
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts),
            "action": t.get("action", ""),
            "aapl_price": t.get("aapl_price"),
            "msft_price": t.get("msft_price"),
            "spread": t.get("spread"),
            # prefer explicit z_score if present for backward compat, else z_like
            "z_score": t.get("z_score", t.get("z_like")),
            "z_like": t.get("z_like", t.get("z_score")),
            "pnl": t.get("pnl"),
            "status": t.get("status", ""),
            "shares": t.get("shares", 1),
            "model": t.get("model"),
        }

    return jsonify([serialize_trade(t) for t in trades])


@app.route("/stock-zscores", methods=["GET"])
def stock_zscores():
    """
    Returns the time series from spread_data (old name kept for frontend).
    It now includes:
      - z_score  (for zscore model)
      - resid_z  (for cointegration model)
      - model    ("zscore" or "eg_ci")
    """
    docs = db.spread_data.find().sort("timestamp", 1)

    def serialize(doc):
        ts = doc.get("timestamp")
        return {
            "id": str(doc["_id"]),
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts),
            "z_score": doc.get("z_score"),
            "resid_z": doc.get("resid_z"),
            "spread": doc.get("spread", doc.get("resid")),  # spread for zscore; resid for eg_ci
            "aapl_price": doc.get("aapl_price"),
            "msft_price": doc.get("msft_price"),
            "hedge_beta": doc.get("hedge_beta"),
            "adf_pval": doc.get("adf_pval"),
            "model": doc.get("model", "zscore"),
        }

    return jsonify([serialize(d) for d in docs])


@app.route("/prices", methods=["GET"])
def prices():
    """
    /prices?symbols=AAPL,MSFT&limit=300
    Returns per-symbol series: [{ timestamp, close }]
    """
    symbols = request.args.get("symbols", "AAPL,MSFT").split(",")
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    limit = int(request.args.get("limit", 300))

    out = {}
    for sym in symbols:
        docs = (
            db.symbol_price_data
            .find({"symbol": sym})
            .sort("Date", DESCENDING)
            .limit(limit)
        )

        series = []
        for d in docs:
            close = (
                d.get("Close")
                or d.get(f"{sym}_Close")
                or d.get("close")
                or d.get("Adj Close")
            )
            dt = d.get("Date")
            ts = dt.isoformat() if isinstance(dt, datetime) else str(dt)
            if close is not None:
                series.append({"timestamp": ts, "close": float(close)})
        out[sym] = list(reversed(series))
    return jsonify(out)


@app.route("/pnl-history", methods=["GET"])
def pnl_history():
    """
    Cumulative PnL time series.
    - Realized PnL at each closed trade.
    - If open positions exist, append latest unrealized point using last spread_data prices.
    """
    trades = list(db.trades.find().sort("timestamp", 1))
    if not trades:
        return jsonify([])

    points = []
    cum_realized = 0.0
    last_ts = None

    for t in trades:
        ts = t.get("timestamp")
        if not ts or t.get("status") != "closed":
            continue
        pnl = t.get("pnl")
        if pnl is None:
            continue
        cum_realized += float(pnl)
        points.append({
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts),
            "cumulative_pnl": cum_realized,
            "type": "realized",
            "trade_id": str(t["_id"]),
            "pnl": float(pnl),
            "shares": t.get("shares", 1),
        })
        last_ts = ts

    # Unrealized PnL snapshot
    open_trades = list(db.trades.find({"status": "open"}))
    if open_trades:
        latest = db.spread_data.find_one(sort=[("timestamp", -1)])
        if latest and "aapl_price" in latest and "msft_price" in latest:
            a_now = float(latest["aapl_price"]); m_now = float(latest["msft_price"])
            total_unreal = 0.0; total_open_shares = 0

            long_aapl = [t for t in open_trades if t["action"] == "long_aapl_short_msft"]
            short_aapl = [t for t in open_trades if t["action"] == "short_aapl_long_msft"]

            # Long AAPL / Short MSFT
            if long_aapl:
                sh = sum(t.get("shares", 1) for t in long_aapl)
                a_cost = sum(t["aapl_price"] * t.get("shares", 1) for t in long_aapl) / sh
                m_cost = sum(t["msft_price"] * t.get("shares", 1) for t in long_aapl) / sh
                unreal = ((a_now - a_cost) - (m_now - m_cost)) * sh
                total_unreal += unreal; total_open_shares += sh

            # Short AAPL / Long MSFT
            if short_aapl:
                sh = sum(t.get("shares", 1) for t in short_aapl)
                a_cost = sum(t["aapl_price"] * t.get("shares", 1) for t in short_aapl) / sh
                m_cost = sum(t["msft_price"] * t.get("shares", 1) for t in short_aapl) / sh
                unreal = ((m_now - m_cost) - (a_now - a_cost)) * sh
                total_unreal += unreal; total_open_shares += sh

            points.append({
                "timestamp": (latest["timestamp"].isoformat()
                              if isinstance(latest.get("timestamp"), datetime)
                              else str(latest.get("timestamp"))),
                "cumulative_pnl": cum_realized + total_unreal,
                "type": "unrealized",
                "unrealized_pnl": total_unreal,
                "total_open_shares": total_open_shares,
                "open_positions": len(open_trades),
            })

    return jsonify(points)


# ------------------------------
# Debug helpers (unchanged API)
# ------------------------------
@app.route("/trades/debug", methods=["GET"])
def debug_trades():
    trades = list(db.trades.find().sort("timestamp", -1))
    out = []
    for t in trades:
        ts = t.get("timestamp")
        out.append({
            "id": str(t["_id"]),
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts),
            "action": t.get("action"),
            "status": t.get("status"),
            "aapl_price": t.get("aapl_price"),
            "msft_price": t.get("msft_price"),
            "pnl": t.get("pnl"),
            "z_like": t.get("z_like", t.get("z_score")),
            "spread": t.get("spread"),
            "shares": t.get("shares", 1),
            "model": t.get("model"),
        })
    open_trades = [t for t in out if t["status"] == "open"]
    long_aapl = [t for t in open_trades if t["action"] == "long_aapl_short_msft"]
    short_aapl = [t for t in open_trades if t["action"] == "short_aapl_long_msft"]

    pos_summary = {}
    if long_aapl:
        sh = sum(t["shares"] for t in long_aapl)
        avg_a = sum(t["aapl_price"] * t["shares"] for t in long_aapl) / sh
        pos_summary["long_aapl_short_msft"] = {"count": len(long_aapl), "total_shares": sh, "avg_aapl_entry": round(avg_a, 4)}
    if short_aapl:
        sh = sum(t["shares"] for t in short_aapl)
        avg_a = sum(t["aapl_price"] * t["shares"] for t in short_aapl) / sh
        pos_summary["short_aapl_long_msft"] = {"count": len(short_aapl), "total_shares": sh, "avg_aapl_entry": round(avg_a, 4)}

    return jsonify({
        "total_trades": len(out),
        "open_trades": len(open_trades),
        "closed_trades": len([t for t in out if t["status"] == "closed"]),
        "position_summary": pos_summary,
        "trades": out
    })


@app.route("/trades/reset", methods=["POST"])
def reset_trades():
    db.trades.delete_many({})
    return jsonify({"message": "All trades reset"})


if __name__ == "__main__":
    # In Docker, your compose maps host:5050 -> container:5000
    app.run(debug=True, host="0.0.0.0", port=5000)
