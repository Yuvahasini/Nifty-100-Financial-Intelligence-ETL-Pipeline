"""
Celery tasks for scheduled ETL refresh, ML scoring, anomaly detection,
trend analysis, and cache invalidation.
All task names match the beat schedule defined in nifty100/celery.py.
"""

import subprocess
import sys
import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date

import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nifty100.settings")

from celery import shared_task
from celery.utils.log import get_task_logger
from sqlalchemy import create_engine, text

logger = get_task_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_URL = os.getenv("NIFTY_DB_URL", "postgresql://postgres:postgres@localhost:5433/nifty100")


def _get_engine():
    return create_engine(DB_URL, echo=False)


def _run_script(script_path: str) -> bool:
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error(f"Script {script_path} failed:\n{result.stderr}")
        return False
    logger.info(result.stdout[-2000:])   # last 2000 chars to avoid log bloat
    return True


# ---------------------------------------------------------------------------
# Task 1 — ETL: clean + load (Steps 2 and 3 per spec)
# ---------------------------------------------------------------------------

@shared_task(name="intelligence.tasks.run_etl_pipeline", bind=True, max_retries=2)
def run_etl_pipeline(self):
    """Daily 1:00 AM — run ETL steps 2 (clean) and 3 (load)."""
    logger.info("▶ run_etl_pipeline starting …")
    for step in ["etl/02_clean_and_transform.py", "etl/03_load_to_warehouse.py"]:
        logger.info(f"  Running {step} …")
        if not _run_script(step):
            raise self.retry(countdown=300)
    logger.info("✔ run_etl_pipeline complete.")
    return "done"


# ---------------------------------------------------------------------------
# Task 2 — ML scoring
# ---------------------------------------------------------------------------

@shared_task(name="intelligence.tasks.score_all_companies", bind=True, max_retries=2)
def score_all_companies(self):
    """Daily 2:00 AM — recalculate health scores for all companies."""
    logger.info("▶ score_all_companies starting …")
    if not _run_script("etl/04_ml_scores.py"):
        raise self.retry(countdown=300)
    logger.info("✔ score_all_companies complete.")
    return "done"


# ---------------------------------------------------------------------------
# Task 3 — Pros / cons generation
# ---------------------------------------------------------------------------

@shared_task(name="intelligence.tasks.generate_pros_cons", bind=True, max_retries=2)
def generate_pros_cons(self):
    """Daily 2:30 AM — re-run pros/cons rule engine for all companies."""
    logger.info("▶ generate_pros_cons starting …")
    if not _run_script("etl/04_ml_scores.py"):
        raise self.retry(countdown=300)
    logger.info("✔ generate_pros_cons complete.")
    return "done"


# ---------------------------------------------------------------------------
# Task 4 — Anomaly detection (Z-score across all fact tables)
# ---------------------------------------------------------------------------

@shared_task(name="intelligence.tasks.detect_anomalies", bind=True, max_retries=1)
def detect_anomalies(self):
    """Weekly Sunday 00:00 — Z-score anomaly detection across all fact tables."""
    logger.info("▶ detect_anomalies starting …")
    engine = _get_engine()

    ZSCORE_THRESHOLD = 2.5
    records = []

    try:
        pl = pd.read_sql(
            "SELECT symbol, fiscal_year, sales, net_profit, operating_profit "
            "FROM fact_profit_loss fp JOIN dim_year dy ON fp.year_id = dy.year_id "
            "WHERE dy.is_ttm = false",
            engine,
        )
        bs = pd.read_sql(
            "SELECT symbol, fiscal_year, borrowings "
            "FROM fact_balance_sheet fb JOIN dim_year dy ON fb.year_id = dy.year_id "
            "WHERE dy.is_ttm = false",
            engine,
        )
    except Exception as exc:
        logger.error(f"DB read failed: {exc}")
        raise self.retry(countdown=600)

    for df, cols in [(pl, ["sales", "net_profit", "operating_profit"]),
                     (bs, ["borrowings"])]:
        for sym, grp in df.groupby("symbol"):
            for col in cols:
                vals = grp[col].dropna()
                if len(vals) < 3:
                    continue
                z = (vals - vals.mean()) / (vals.std() + 1e-9)
                flagged = grp.loc[z.abs() >= ZSCORE_THRESHOLD]
                for _, row in flagged.iterrows():
                    zv = float(z.loc[row.name])
                    records.append({
                        "symbol":      sym,
                        "fiscal_year": int(row.get("fiscal_year", 0) or 0),
                        "metric":      col,
                        "z_score":     round(zv, 3),
                        "direction":   "spike" if zv > 0 else "drop",
                        "method":      "zscore",
                        "flagged_at":  date.today(),
                    })

    if records:
        flags_df = pd.DataFrame(records)
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM fact_anomaly_flags WHERE method = 'zscore'"
            ))
        flags_df.to_sql(
            "fact_anomaly_flags", con=engine,
            if_exists="append", index=False, method="multi",
        )
        logger.info(f"✔ detect_anomalies: wrote {len(records)} zscore flags.")
    else:
        logger.info("✔ detect_anomalies: no anomalies found.")

    return f"{len(records)} flags written"


# ---------------------------------------------------------------------------
# Task 5 — Trend detection (linear regression on sales & profit)
# ---------------------------------------------------------------------------

@shared_task(name="intelligence.tasks.detect_trends", bind=True, max_retries=1)
def detect_trends(self):
    """Weekly Sunday 00:30 — linear regression trend analysis for all companies."""
    logger.info("▶ detect_trends starting …")
    engine = _get_engine()

    try:
        pl = pd.read_sql(
            "SELECT symbol, fiscal_year, sales, net_profit "
            "FROM fact_profit_loss fp JOIN dim_year dy ON fp.year_id = dy.year_id "
            "WHERE dy.is_ttm = false ORDER BY symbol, fiscal_year",
            engine,
        )
    except Exception as exc:
        logger.error(f"DB read failed: {exc}")
        raise self.retry(countdown=600)

    trend_records = []
    for sym, grp in pl.groupby("symbol"):
        recent = grp[grp["fiscal_year"] >= grp["fiscal_year"].max() - 4].sort_values("fiscal_year")
        if len(recent) < 2:
            continue
        x = np.arange(len(recent), dtype=float)
        for col, label in [("sales", "sales_trend"), ("net_profit", "profit_trend")]:
            y = recent[col].fillna(0).values
            slope = float(np.polyfit(x, y, 1)[0])
            avg   = float(y.mean())
            slope_pct = (slope / avg * 100) if avg != 0 else 0.0
            trend = "UP" if slope_pct > 5 else "DOWN" if slope_pct < -5 else "FLAT"
            trend_records.append({
                "symbol": sym, "metric": label,
                "slope": round(slope, 2), "slope_pct": round(slope_pct, 2),
                "trend": trend, "computed_at": date.today(),
            })

    if trend_records:
        logger.info(f"✔ detect_trends: computed {len(trend_records)} trend records.")
        # Persist to a lightweight table if it exists, otherwise just log
        try:
            pd.DataFrame(trend_records).to_sql(
                "fact_trend_labels", con=engine,
                if_exists="replace", index=False, method="multi",
            )
        except Exception:
            logger.warning("fact_trend_labels table not found — trends logged only.")

    return f"{len(trend_records)} trend records"


# ---------------------------------------------------------------------------
# Task 6 — Cache invalidation
# ---------------------------------------------------------------------------

@shared_task(name="intelligence.tasks.invalidate_cache")
def invalidate_cache():
    """Daily 3:00 AM — clear Redis cache for all changed company data."""
    try:
        from django.core.cache import cache
        cache.clear()
        logger.info("✔ invalidate_cache: Redis cache cleared.")
        return "cache cleared"
    except Exception as exc:
        logger.warning(f"Cache clear skipped: {exc}")
        return "skipped"