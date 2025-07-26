from celery import Celery
import yfinance as yf
from pymongo import MongoClient

# Create Celery app
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
            if (symbol, 'Close') in data.columns:
                symbol_data = data.xs(symbol, axis=1, level=1, drop_level=False)
            else:
                symbol_data = data
            # Flatten columns to string keys for MongoDB
            symbol_data.columns = ['_'.join([str(i) for i in col if i]) for col in symbol_data.columns.values]
            print(f"Inserting data for {symbol}, rows: {symbol_data.shape[0]}")
            db.price_data.update_one(
                {"symbol": symbol, "start": "2022-01-01", "end": "2023-01-01"},
                {"$set": {"data": symbol_data.reset_index().to_dict("records")}},
                upsert=True
            )
        print(f"Successfully stored prices for {stock1} and {stock2}")
        return f"Fetched and stored prices for {stock1} and {stock2}"
    except Exception as e:
        print(f"Error in fetch_and_store_prices: {e}")
        return f"Error: {e}"
