from datetime import timedelta

beat_schedule = {
    "fetch-and-align-chain-every-10s": {
        "task": "celery_worker.tasks.trigger_chain",
        "schedule": timedelta(seconds=10),
    },
}
timezone = "UTC"