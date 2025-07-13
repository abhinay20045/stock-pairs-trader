from flask import Flask
from routes.pair_routes import pair_bp
from pymongo import MongoClient

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

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")