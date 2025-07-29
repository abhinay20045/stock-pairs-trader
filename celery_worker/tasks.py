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
def calculate_spread_and_zscore(_=None):
    client = MongoClient("mongodb://mongo:27017")
    db = client["trading_db"]

    aapl_docs = list(db.symbol_price_data.find({"symbol": "AAPL"}))
    msft_docs = list(db.symbol_price_data.find({"symbol": "MSFT"}))

    if not aapl_docs or not msft_docs:
        print("Missing price data")
        return None

    aapl_df = pd.DataFrame(aapl_docs)
    msft_df = pd.DataFrame(msft_docs)
    aapl_df["Date"] = pd.to_datetime(aapl_df["Date"])
    msft_df["Date"] = pd.to_datetime(msft_df["Date"])

    merged = pd.merge(
        aapl_df[["Date", "AAPL_Close"]],
        msft_df[["Date", "MSFT_Close"]],
        on="Date",
        how="inner"
    ).sort_values("Date")

    merged["spread"] = merged["AAPL_Close"] - merged["MSFT_Close"]

    if merged.shape[0] < 30:
        print("Not enough data points for Z-score")
        return None

    recent_data = merged.iloc[-30:]
    spread_series = recent_data["spread"]
    mean = spread_series.mean()
    std = spread_series.std()

    if std == 0:
        print("Standard deviation is zero, skipping Z-score calculation")
        return None

    z_score = (spread_series.iloc[-1] - mean) / std

    record = {
        "timestamp": datetime.now(timezone.utc),
        "aapl_price": recent_data["AAPL_Close"].iloc[-1],
        "msft_price": recent_data["MSFT_Close"].iloc[-1],
        "spread": spread_series.iloc[-1],
        "z_score": z_score,
    }

    db.spread_data.insert_one(record)
    print(f"Inserted spread record: {record}")
    return record


@app.task
def trigger_chain():
    result = chain(
        fetch_and_store_prices.s("AAPL", "MSFT"),
        align_and_extract_close_prices.s()
    ).apply_async()
    return {"task_id": result.id, "status": "submitted for alignment and extraction"}


app.config_from_object("celery_worker.celeryconfig")
