from flask import Flask, jsonify
from pymongo import MongoClient
from routes.pair_routes import pair_bp
from celery_worker.tasks import fetch_and_store_prices  # ✅ Import the task

app = Flask(__name__)
app.register_blueprint(pair_bp)

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
    # Step 1: Fetch and store prices (async)
    fetch_and_store_prices.delay("AAPL", "MSFT")
    # Step 2: Align and extract close prices (async)
    from celery_worker.tasks import align_and_extract_close_prices
    result = align_and_extract_close_prices.delay()
    return jsonify({"task_id": result.id, "status": "submitted for alignment and extraction"})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
