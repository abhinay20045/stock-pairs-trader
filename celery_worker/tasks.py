from celery import Celery, chain
import yfinance as yf
from pymongo import MongoClient
import pandas as pd
from flask import Response, json


app = Celery(
    "tasks",
    broker="redis://redis:6379/0"
)

@app.task
def fetch_and_store_prices(stock1, stock2):
    print(f"Fetching prices for {stock1} and {stock2}")
    try:
        client = MongoClient("mongodb://mongo:27017")
        db = client["trading_db"]
        data = yf.download(f"{stock1} {stock2}", start="2022-01-01", end="2023-01-01")
        print(f"Downloaded data shape: {data.shape}")
        if data.empty:
            print("No data fetched from yfinance!")
            return "No data fetched from yfinance!"
        for symbol in [stock1, stock2]:
            # Get only that symbol's data (drops to single-level columns)
            symbol_data = data.xs(symbol, axis=1, level=1)
            symbol_data = symbol_data.reset_index()
            # Rename columns to be unique: e.g., AAPL_Close
            symbol_data.columns = ["Date"] + [f"{symbol}_{col}" for col in symbol_data.columns if col != "Date"]

            assert symbol_data.columns.is_unique, f"Non-unique columns for {symbol}!"
            print(f"Inserting data for {symbol}, rows: {symbol_data.shape[0]}")
            
            db.price_data.update_one(
                {"symbol": symbol, "start": "2022-01-01", "end": "2023-01-01"},
                {"$set": {"data": symbol_data.to_dict("records")}},
                upsert=True
            )
        print(f"Successfully stored prices for {stock1} and {stock2}")
        return f"Fetched and stored prices for {stock1} and {stock2}"
    except Exception as e:
        print(f"Error in fetch_and_store_prices: {e}")
        return f"Error: {e}"

@app.task
def align_and_extract_close_prices(_):
    client = MongoClient("mongodb://mongo:27017")
    db = client["trading_db"]
    # Get the price data for AAPL and MSFT
    aapl_doc = db.price_data.find_one({"symbol": "AAPL"})
    msft_doc = db.price_data.find_one({"symbol": "MSFT"})
    if not aapl_doc or not msft_doc:
        print("AAPL or MSFT data not found in MongoDB.")
        return None
    aapl_df = pd.DataFrame(aapl_doc["data"])
    msft_df = pd.DataFrame(msft_doc["data"])
    # Extract Date and Close columns
    aapl_df = aapl_df[["Date", "AAPL_Close"]]
    msft_df = msft_df[["Date", "MSFT_Close"]]
    aapl_df["Date"] = pd.to_datetime(aapl_df["Date"])
    msft_df["Date"] = pd.to_datetime(msft_df["Date"])
    aapl_df = aapl_df.sort_values("Date")
    msft_df = msft_df.sort_values("Date")
    # Merge on Date (inner join for intersection)
    merged = pd.merge(aapl_df, msft_df, on="Date", how="inner")
    # Create aligned numpy arrays
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
def trigger_chain():
    result = chain(fetch_and_store_prices.s("AAPL", "MSFT"), align_and_extract_close_prices.s()).apply_async()
    return Response(
    json.dumps({"task_id": result.id, "status": "submitted for alignment and extraction"}, default=str),
    mimetype='application/json'
)
app.config_from_object("celery_worker.celeryconfig") 
