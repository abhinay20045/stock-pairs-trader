from celery import chain
from flask import Flask, Response, json, jsonify, request
from pymongo import MongoClient, DESCENDING
from routes.pair_routes import pair_bp
from celery_worker.tasks import fetch_and_store_prices  # ✅ Import the task
from flask_cors import CORS
from datetime import datetime




app = Flask(__name__)
app.register_blueprint(pair_bp)

CORS(app)

client = MongoClient("mongodb://mongo:27017")
db = client["trading_db"]

@app.route('/health')
def health():
    return {"status": "ok"}

@app.route("/test-db")
def test_db():
    db.test_collection.insert_one({"status": "working"})
    count = db.test_collection.count_documents({})
    return {"message": f"Mongo test successful. Docs in test_collection: {count}"}

@app.route("/run-task")
def run_task():
    from celery_worker.tasks import align_and_extract_close_prices
    result = chain(fetch_and_store_prices.s("AAPL", "MSFT"), align_and_extract_close_prices.s()).apply_async()
    return Response(
    json.dumps({"task_id": result.id, "status": "submitted for alignment and extraction"}, default=str),
    mimetype='application/json'
)
@app.route("/trade-history", methods=["GET"])
def trade_history():
    client = MongoClient("mongodb://mongo:27017")
    db = client["trading_db"]
    trades = db.trades.find().sort("timestamp", 1)

    def serialize_trade(trade):
        return {
            "id": str(trade["_id"]),
            "timestamp": trade["timestamp"].isoformat(),
            "action": trade.get("action", ""),
            "aapl_price": trade.get("aapl_price", None),
            "msft_price": trade.get("msft_price", None),
            "spread": trade.get("spread", None),
            "z_score": trade.get("z_score", None),
            "pnl": trade.get("pnl", None),
            "status": trade.get("status", ""),
        }

    return jsonify([serialize_trade(t) for t in trades])

@app.route("/prices", methods=["GET"])
def prices():
    # /prices?symbols=AAPL,MSFT&limit=300
    symbols = request.args.get("symbols", "AAPL,MSFT").split(",")
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    limit = int(request.args.get("limit", 300))

    out = {}
    for sym in symbols:
        # latest N by Date, then reverse to ascending
        docs = (
            db.symbol_price_data
              .find({"symbol": sym})
              .sort("Date", DESCENDING)
              .limit(limit)
        )
        series = []
        for d in docs:
            # Try common field names
            close = (
                d.get("Close")
                or d.get(f"{sym}_Close")
                or d.get("close")
                or d.get("Adj Close")
            )
            # Convert date
            dt = d.get("Date")
            if isinstance(dt, datetime):
                ts = dt.isoformat()
            else:
                # handle string date
                ts = str(dt)
            if close is not None:
                series.append({"timestamp": ts, "close": float(close)})
        out[sym] = list(reversed(series))
    return jsonify(out)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
