from celery import Celery

app = Celery('tasks', broker='redis://redis:6379/0')

@app.task
def fetch_and_trade():
    print("Fetching stock data and executing strategy...")