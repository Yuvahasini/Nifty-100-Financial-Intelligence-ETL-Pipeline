"""
Celery tasks for scheduled ETL refresh and ML scoring.
"""

import subprocess
import sys
from pathlib import Path
from celery import shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run_script(script_path: str) -> bool:
    result = subprocess.run(
        [sys.executable, script_path],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error(f"Script {script_path} failed:\n{result.stderr}")
        return False
    logger.info(result.stdout)
    return True


@shared_task(name="intelligence.tasks.run_ml_scoring", bind=True, max_retries=2)
def run_ml_scoring(self):
    """Run ETL Step 4 — ML health scoring."""
    logger.info("Starting daily ML scoring …")
    ok = _run_script("etl/04_ml_scores.py")
    if not ok:
        raise self.retry(countdown=300)
    logger.info("ML scoring complete.")
    return "done"


@shared_task(name="intelligence.tasks.run_full_etl", bind=True, max_retries=1)
def run_full_etl(self):
    """Run all 4 ETL steps in sequence (manual trigger)."""
    logger.info("Starting full ETL pipeline …")
    for step in ["etl/01_extract_from_excel.py",
                 "etl/02_clean_and_transform.py",
                 "etl/03_load_to_warehouse.py",
                 "etl/04_ml_scores.py"]:
        logger.info(f"  Running {step} …")
        if not _run_script(step):
            raise self.retry(countdown=60)
    logger.info("Full ETL pipeline complete.")
    return "done"