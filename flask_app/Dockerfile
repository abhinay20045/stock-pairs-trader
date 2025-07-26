FROM python:3.11-slim

WORKDIR /app
COPY flask_app/ /app/
COPY celery_worker/ /app/celery_worker/
RUN pip install -r requirements.txt

CMD ["python", "app.py"]
