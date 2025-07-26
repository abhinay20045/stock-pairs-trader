FROM python:3.11-slim

WORKDIR /app
COPY celery_worker/ /app/celery_worker/
RUN pip install -r /app/celery_worker/requirements.txt

CMD ["celery", "-A", "celery_worker.tasks", "worker", "--loglevel=info"]
