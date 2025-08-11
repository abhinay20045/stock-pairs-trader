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
            "shares": trade.get("shares", 1)
        }

    return jsonify([serialize_trade(t) for t in trades])

@app.route("/stock-zscores", methods=["GET"])
def stock_zscores():
    client = MongoClient("mongodb://mongo:27017")
    db = client["trading_db"]
    
    # Fetch all z-score data from spread_data collection, sorted by timestamp
    zscores = db.spread_data.find().sort("timestamp", 1)

    def serialize_stock_zscore(doc):
        return {
            "id": str(doc["_id"]),
            "timestamp": doc["timestamp"].isoformat(),
            "z_score": doc.get("z_score", None),
            "spread": doc.get("spread", None),
            "aapl_price": doc.get("aapl_price", None),
            "msft_price": doc.get("msft_price", None),
        }

    return jsonify([serialize_stock_zscore(z) for z in zscores])

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

@app.route("/pnl-history", methods=["GET"])
def pnl_history():
    """
    Returns a time series of cumulative PnL.
    - Realized PnL increases whenever a 'closed' trade doc appears (uses its stored 'pnl')
    - If there are open trades, append a final point with realized + unrealized PnL at 'now'
    - Handles multiple open trades with average costs
    """
    # Get all trades sorted by timestamp
    trades = list(db.trades.find().sort("timestamp", 1))
    
    if not trades:
        return jsonify([])

    points = []
    cum_realized = 0.0
    last_timestamp = None

    # Process all trades to build PnL history
    for t in trades:
        ts = t.get("timestamp")
        status = t.get("status")
        action = t.get("action")
        
        # Skip trade entries that don't have proper data
        if not ts or not status or not action:
            continue
            
        # For closed trades, use the stored PnL
        if status == "closed" and "pnl" in t and t["pnl"] is not None:
            cum_realized += float(t["pnl"])
            points.append({
                "timestamp": ts.isoformat(),
                "cumulative_pnl": cum_realized,
                "type": "realized",
                "trade_id": str(t["_id"]),
                "pnl": float(t["pnl"]),
                "shares": t.get("shares", 1)
            })
            last_timestamp = ts

    # Calculate unrealized PnL for all open trades
    open_trades = list(db.trades.find({"status": "open"}))
    if open_trades and last_timestamp:
        # Get latest price data
        latest = db.spread_data.find_one(sort=[("timestamp", -1)])
        if latest and "aapl_price" in latest and "msft_price" in latest:
            a_now = float(latest["aapl_price"])
            m_now = float(latest["msft_price"])
            
            # Group open trades by action type
            long_aapl_trades = [t for t in open_trades if t["action"] == "long_aapl_short_msft"]
            short_aapl_trades = [t for t in open_trades if t["action"] == "short_aapl_long_msft"]
            
            total_unrealized = 0.0
            total_open_shares = 0
            
            # Calculate unrealized PnL for long AAPL positions
            if long_aapl_trades:
                total_aapl_shares = sum(t.get("shares", 1) for t in long_aapl_trades)
                total_aapl_cost = sum(t["aapl_price"] * t.get("shares", 1) for t in long_aapl_trades)
                avg_aapl_entry = total_aapl_cost / total_aapl_shares
                
                total_msft_shares = sum(t.get("shares", 1) for t in long_aapl_trades)
                total_msft_cost = sum(t["msft_price"] * t.get("shares", 1) for t in long_aapl_trades)
                avg_msft_entry = total_msft_cost / total_msft_shares
                
                unreal = (a_now - avg_aapl_entry) - (m_now - avg_msft_entry)
                unreal *= total_aapl_shares
                total_unrealized += unreal
                total_open_shares += total_aapl_shares
            
            # Calculate unrealized PnL for short AAPL positions
            if short_aapl_trades:
                total_aapl_shares = sum(t.get("shares", 1) for t in short_aapl_trades)
                total_aapl_cost = sum(t["aapl_price"] * t.get("shares", 1) for t in short_aapl_trades)
                avg_aapl_entry = total_aapl_cost / total_aapl_shares
                
                total_msft_shares = sum(t.get("shares", 1) for t in short_aapl_trades)
                total_msft_cost = sum(t["msft_price"] * t.get("shares", 1) for t in short_aapl_trades)
                avg_msft_entry = total_msft_cost / total_msft_shares
                
                unreal = (avg_msft_entry - m_now) - (avg_aapl_entry - a_now)
                unreal *= total_aapl_shares
                total_unrealized += unreal
                total_open_shares += total_aapl_shares
            
            if total_open_shares > 0:
                points.append({
                    "timestamp": latest["timestamp"].isoformat(),
                    "cumulative_pnl": cum_realized + total_unrealized,
                    "type": "unrealized",
                    "unrealized_pnl": total_unrealized,
                    "total_open_shares": total_open_shares,
                    "open_positions": len(open_trades)
                })

    return jsonify(points)

@app.route("/trades/debug", methods=["GET"])
def debug_trades():
    """
    Debug endpoint to see all trades and their current status
    """
    trades = list(db.trades.find().sort("timestamp", -1))
    
    debug_info = []
    for t in trades:
        debug_info.append({
            "id": str(t["_id"]),
            "timestamp": t["timestamp"].isoformat() if t.get("timestamp") else None,
            "action": t.get("action"),
            "status": t.get("status"),
            "aapl_price": t.get("aapl_price"),
            "msft_price": t.get("msft_price"),
            "pnl": t.get("pnl"),
            "z_score": t.get("z_score"),
            "spread": t.get("spread"),
            "shares": t.get("shares", 1)
        })
    
    # Group open trades by action type
    open_trades = [t for t in debug_info if t["status"] == "open"]
    long_aapl_positions = [t for t in open_trades if t["action"] == "long_aapl_short_msft"]
    short_aapl_positions = [t for t in open_trades if t["action"] == "short_aapl_long_msft"]
    
    # Calculate position summaries
    position_summary = {}
    if long_aapl_positions:
        total_shares = sum(t["shares"] for t in long_aapl_positions)
        total_aapl_cost = sum(t["aapl_price"] * t["shares"] for t in long_aapl_positions)
        avg_aapl_entry = total_aapl_cost / total_shares
        position_summary["long_aapl_short_msft"] = {
            "count": len(long_aapl_positions),
            "total_shares": total_shares,
            "avg_aapl_entry": round(avg_aapl_entry, 2)
        }
    
    if short_aapl_positions:
        total_shares = sum(t["shares"] for t in short_aapl_positions)
        total_aapl_cost = sum(t["aapl_price"] * t["shares"] for t in short_aapl_positions)
        avg_aapl_entry = total_aapl_cost / total_shares
        position_summary["short_aapl_long_msft"] = {
            "count": len(short_aapl_positions),
            "total_shares": total_shares,
            "avg_aapl_entry": round(avg_aapl_entry, 2)
        }
    
    return jsonify({
        "total_trades": len(debug_info),
        "open_trades": len(open_trades),
        "closed_trades": len([t for t in debug_info if t["status"] == "closed"]),
        "position_summary": position_summary,
        "trades": debug_info
    })

@app.route("/trades/reset", methods=["POST"])
def reset_trades():
    """
    Reset all trades (for testing purposes)
    """
    db.trades.delete_many({})
    return jsonify({"message": "All trades reset"})
    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
