"""
Celery application entry point with full beat schedule.
File: nifty100/celery.py
"""

import os
from celery import Celery
from celery.schedules import crontab

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nifty100.settings")

app = Celery("nifty100")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

# ---------------------------------------------------------------------------
# Beat schedule — spec §7.3
# ---------------------------------------------------------------------------
app.conf.beat_schedule = {
    "run_etl_pipeline": {
        "task":     "intelligence.tasks.run_etl_pipeline",
        "schedule": crontab(hour=1, minute=0),
    },
    "score_all_companies": {
        "task":     "intelligence.tasks.score_all_companies",
        "schedule": crontab(hour=2, minute=0),
    },
    "generate_pros_cons": {
        "task":     "intelligence.tasks.generate_pros_cons",
        "schedule": crontab(hour=2, minute=30),
    },
    "detect_anomalies": {
        "task":     "intelligence.tasks.detect_anomalies",
        "schedule": crontab(hour=0, minute=0, day_of_week="sunday"),
    },
    "detect_trends": {
        "task":     "intelligence.tasks.detect_trends",
        "schedule": crontab(hour=0, minute=30, day_of_week="sunday"),
    },
    "invalidate_cache": {
        "task":     "intelligence.tasks.invalidate_cache",
        "schedule": crontab(hour=3, minute=0),
    },
}

app.conf.timezone = "Asia/Kolkata"


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    print(f"Request: {self.request!r}")