from celery import Celery, chain
import yfinance as yf
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta, timezone

app = Celery("tasks", broker="redis://redis:6379/0")

@app.task
def fetch_and_store_prices(stock1, stock2):
    print(f"Fetching prices for {stock1} and {stock2}")
    try:
        client = MongoClient("mongodb://mongo:27017")
        db = client["trading_db"]
        today = datetime.now(timezone.utc)

        for symbol in [stock1, stock2]:
            # Step 1: Get the last stored date for this symbol
            latest_doc = db.symbol_price_data.find_one(
                {"symbol": symbol},
                sort=[("Date", -1)]
            )
            if latest_doc:
                last_date = latest_doc["Date"]
                if isinstance(last_date, datetime):
                    start_datetime = (last_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
                else:
                    start_datetime = datetime.combine(last_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
            else:
                start_datetime = datetime(2022, 1, 1, tzinfo=timezone.utc)

            if start_datetime >= today:
                print(f"No new data to fetch for {symbol}. Latest date: {start_datetime}")
                continue

            print(f"Fetching {symbol} data from {start_datetime} to {today}")
            data = yf.download(symbol, start=start_datetime, end=today, auto_adjust=False)

            if data.empty:
                print(f"No new data for {symbol}")
                continue

            data = data.reset_index()
            data.columns = [
                "Date",
                f"{symbol}_Open",
                f"{symbol}_High",
                f"{symbol}_Low",
                f"{symbol}_Close",
                f"{symbol}_Adj Close",
                f"{symbol}_Volume"
            ]
            data["symbol"] = symbol

            records = data.to_dict("records")
            for row in records:
                row_date = row["Date"]
                # Ensure row_date is a datetime with tzinfo
                if isinstance(row_date, datetime):
                    row_date_dt = row_date if row_date.tzinfo else row_date.replace(tzinfo=timezone.utc)
                else:
                    row_date_dt = datetime.combine(row_date, datetime.min.time(), tzinfo=timezone.utc)
                if row_date_dt >= today:
                    continue  # Skip today to avoid incomplete records
                db.symbol_price_data.update_one(
                    {"symbol": symbol, "Date": row["Date"]},
                    {"$set": row},
                    upsert=True
                )

            print(f"Inserted {len(records)} records for {symbol} from {start_datetime} to {today}")

        return f"Fetched and stored prices for {stock1} and {stock2}"
    except Exception as e:
        print(f"Error in fetch_and_store_prices: {e}")
        return f"Error: {e}"


@app.task
def align_and_extract_close_prices(_):
    client = MongoClient("mongodb://mongo:27017")
    db = client["trading_db"]

    aapl_docs = list(db.symbol_price_data.find({"symbol": "AAPL"}))
    msft_docs = list(db.symbol_price_data.find({"symbol": "MSFT"}))

    if not aapl_docs or not msft_docs:
        print("AAPL or MSFT data not found in symbol_price_data.")
        return None

    aapl_df = pd.DataFrame(aapl_docs)
    msft_df = pd.DataFrame(msft_docs)

    if "Date" not in aapl_df.columns or "AAPL_Close" not in aapl_df.columns:
        print("Missing columns in AAPL data.")
        return None
    if "Date" not in msft_df.columns or "MSFT_Close" not in msft_df.columns:
        print("Missing columns in MSFT data.")
        return None

    aapl_df["Date"] = pd.to_datetime(aapl_df["Date"])
    msft_df["Date"] = pd.to_datetime(msft_df["Date"])
    aapl_df = aapl_df.sort_values("Date")
    msft_df = msft_df.sort_values("Date")

    merged = pd.merge(
        aapl_df[["Date", "AAPL_Close"]],
        msft_df[["Date", "MSFT_Close"]],
        on="Date",
        how="inner"
    )

    AAPL_prices = merged["AAPL_Close"].to_numpy()
    MSFT_prices = merged["MSFT_Close"].to_numpy()
    print("Sample AAPL:", AAPL_prices[:10])
    print("Sample MSFT:", MSFT_prices[:10])

    return {
        "dates": merged["Date"].dt.strftime("%Y-%m-%d").tolist(),
        "AAPL_prices": AAPL_prices.tolist(),
        "MSFT_prices": MSFT_prices.tolist()
    }


@app.task
def calculate_spread_and_zscore(prev = None, window: int = 30, _=None):
    """
    Recompute z-scores for the entire history (rolling window) and upsert into
    trading_db.spread_data. This backfills any missing days and also provides
    the most recent record as the task result.
    """
    client = MongoClient("mongodb://mongo:27017")
    db = client["trading_db"]

    # --- Load price data ---
    aapl_docs = list(db.symbol_price_data.find({"symbol": "AAPL"}))
    msft_docs = list(db.symbol_price_data.find({"symbol": "MSFT"}))

    if not aapl_docs or not msft_docs:
        print("[zscore] Missing price data (AAPL/MSFT).")
        return {"error": "Missing price data"}

    aapl_df = pd.DataFrame(aapl_docs)
    msft_df = pd.DataFrame(msft_docs)

    # Defensive column handling (supports either "<SYM>_Close" or "Close")
    def normalize_close(df, sym):
        df = df.copy()
        df["Date"] = pd.to_datetime(df["Date"])
        close_col = f"{sym}_Close"
        if close_col in df.columns:
            out = df[["Date", close_col]].rename(columns={close_col: "Close"})
        elif "Close" in df.columns:
            out = df[["Date", "Close"]]
        else:
            # try best-effort find a Close-like column
            guess = df.filter(like="Close").columns
            if len(guess) == 0:
                raise ValueError(f"[zscore] No close column found for {sym}")
            out = df[["Date", guess[0]]].rename(columns={guess[0]: "Close"})
        out = out.sort_values("Date")
        out["Symbol"] = sym
        return out

    a = normalize_close(aapl_df, "AAPL")  # Date, Close
    m = normalize_close(msft_df, "MSFT")  # Date, Close

    # --- Merge & compute spread and rolling stats ---
    merged = pd.merge(
        a.rename(columns={"Close": "AAPL_Close"}),
        m.rename(columns={"Close": "MSFT_Close"}),
        on="Date",
        how="inner"
    ).sort_values("Date").reset_index(drop=True)

    if merged.shape[0] < window:
        print(f"[zscore] Not enough data points (have {merged.shape[0]}, need {window}).")
        return {"error": "Not enough data points for Z-score"}

    merged["spread"] = merged["AAPL_Close"] - merged["MSFT_Close"]
    merged["spread_mean"] = merged["spread"].rolling(window).mean()
    merged["spread_std"] = merged["spread"].rolling(window).std()
    merged["z_score"] = (merged["spread"] - merged["spread_mean"]) / merged["spread_std"]

    # Drop the initial NaNs before the rolling window is filled
    merged = merged.dropna(subset=["z_score"]).copy()
    if merged.empty:
        print("[zscore] No rows after rolling window; aborting.")
        return {"error": "No z-scores computed"}

    # --- Ensure index for idempotent upserts ---
    try:
        db.spread_data.create_index("timestamp", unique=True)
    except Exception as e:
        print(f"[zscore] Index creation warning: {e}")

    # --- Upsert all rows (backfill + keep current updated) ---
    upserts = 0
    for _, r in merged.iterrows():
        # Use the candle's date as the timestamp (UTC midnight)
        ts = pd.Timestamp(r["Date"]).to_pydatetime().replace(tzinfo=timezone.utc)
        doc = {
            "timestamp": ts,
            "aapl_price": float(r["AAPL_Close"]),
            "msft_price": float(r["MSFT_Close"]),
            "spread": float(r["spread"]),
            "z_score": float(r["z_score"]),
        }
        res = db.spread_data.update_one(
            {"timestamp": ts},
            {"$set": doc},
            upsert=True
        )
        # Count only effective writes
        if res.upserted_id is not None or res.modified_count > 0:
            upserts += 1

    latest = merged.iloc[-1]
    latest_ts = pd.Timestamp(latest["Date"]).to_pydatetime().replace(tzinfo=timezone.utc)
    latest_record = {
        "timestamp": latest_ts,
        "aapl_price": float(latest["AAPL_Close"]),
        "msft_price": float(latest["MSFT_Close"]),
        "spread": float(latest["spread"]),
        "z_score": float(latest["z_score"]),
        "upserts": upserts
    }

    print(f"[zscore] Upserted {upserts} rows. Latest: {latest_record}")
    # Return a JSON-serializable dict (no ObjectId)
    return latest_record


@app.task
def evaluate_and_place_trade(_=None):
    from pymongo import MongoClient
    from bson.objectid import ObjectId

    client = MongoClient("mongodb://mongo:27017")
    db = client["trading_db"]

    print("🔍 [Trade Eval] Starting trade evaluation...")

    # Fetch latest z-score record
    latest = db.spread_data.find_one(sort=[("timestamp", -1)])
    if not latest:
        print("⚠️ [Trade Eval] No z-score data found in spread_data.")
        return None

    z_score = latest["z_score"]
    aapl_price = latest["aapl_price"]
    msft_price = latest["msft_price"]
    spread = latest["spread"]
    timestamp = latest["timestamp"]

    print(f"📊 [Trade Eval] Latest Data — Z-score: {z_score}, AAPL: {aapl_price}, MSFT: {msft_price}, Spread: {spread}, Time: {timestamp}")

    # Check for existing open trades of the same type
    if z_score > .1:
        trade_action = "short_aapl_long_msft"
        open_trades = list(db.trades.find({"status": "open", "action": trade_action}))
    elif z_score < -.1:
        trade_action = "long_aapl_short_msft"
        open_trades = list(db.trades.find({"status": "open", "action": trade_action}))
    else:
        trade_action = None
        open_trades = []

    if trade_action:
        print(f"🟡 [Trade Eval] Found {len(open_trades)} open trades of type: {trade_action}")
        
        # Check if we should close positions
        should_close = (
            (trade_action == "long_aapl_short_msft" and z_score >= 0) or
            (trade_action == "short_aapl_long_msft" and z_score <= 0)
        )
        
        if should_close and open_trades:
            # Close all open trades of this type
            total_pnl = 0.0
            for trade in open_trades:
                shares = trade.get("shares", 1)
                if trade_action == "long_aapl_short_msft":
                    pnl = (aapl_price - trade["aapl_price"]) - (msft_price - trade["msft_price"])
                else:
                    pnl = (msft_price - trade["msft_price"]) - (aapl_price - trade["aapl_price"])
                
                pnl *= shares
                total_pnl += pnl
                
                # Update the trade with PnL and closed status
                db.trades.update_one(
                    {"_id": trade["_id"]}, 
                    {"$set": {
                        "status": "closed",
                        "pnl": pnl,
                        "close_timestamp": timestamp,
                        "close_aapl_price": aapl_price,
                        "close_msft_price": msft_price,
                        "close_z_score": z_score,
                        "shares": shares
                    }}
                )
            
            print(f"🔴 [Trade Closed] Closed {len(open_trades)} trades | Total PnL: {total_pnl:.2f}")
            
        elif not should_close:
            # Add to existing position or create new one
            if open_trades:
                print(f"📈 [Trade Eval] Adding to existing position")
            
            # Create new trade entry
            new_trade = {
                "timestamp": timestamp,
                "action": trade_action,
                "aapl_price": aapl_price,
                "msft_price": msft_price,
                "spread": spread,
                "z_score": z_score,
                "status": "open",
                "shares": 1
            }
            db.trades.insert_one(new_trade)
            print(f"🟢 [Trade Placed] {trade_action} at Z-score: {z_score} | Shares: 1")
            
    else:
        print("ℹ️ [Trade Eval] Z-score does not exceed entry threshold. No trade placed.")

    return "Trade evaluation completed."



@app.task
def trigger_chain():
    result = chain(
        fetch_and_store_prices.s("AAPL", "MSFT"),
        align_and_extract_close_prices.s(), calculate_spread_and_zscore.s(), evaluate_and_place_trade.s()
    ).apply_async()
    return {"task_id": result.id, "status": "submitted for alignment and extraction"}

app.config_from_object("celery_worker.celeryconfig")
