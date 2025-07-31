from celery import chain
from flask import Flask, Response, json, jsonify
from pymongo import MongoClient
from routes.pair_routes import pair_bp
from celery_worker.tasks import fetch_and_store_prices  # ✅ Import the task
from flask_cors import CORS




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
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
