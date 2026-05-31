"""
ETL Script 6 — Load anomaly flags and peer mapping into PostgreSQL.
Run after notebooks/03 and notebooks/05 have generated their CSV exports.

  python etl/06_load_anomaly_peers.py

Creates tables if they don't exist, then upserts data.
"""

import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

CLEAN_DIR = Path("data/clean")
PEER_FILE  = Path("data/peer_mapping.csv")

DB_URL = os.getenv("NIFTY_DB_URL", "postgresql://postgres:postgres@localhost:5433/nifty100")


def get_engine():
    return create_engine(DB_URL, echo=False)


# ---------------------------------------------------------------------------
# DDL — create tables if missing
# ---------------------------------------------------------------------------

DDL_ANOMALY = """
CREATE TABLE IF NOT EXISTS fact_anomaly_flags (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(20) NOT NULL REFERENCES dim_company(symbol) ON DELETE CASCADE,
    fiscal_year INTEGER,
    metric      VARCHAR(50) NOT NULL,
    z_score     NUMERIC(8, 3),
    direction   VARCHAR(10),
    method      VARCHAR(30) DEFAULT 'zscore',
    flagged_at  DATE,
    UNIQUE (symbol, fiscal_year, metric, method)
);
CREATE INDEX IF NOT EXISTS idx_anomaly_symbol     ON fact_anomaly_flags(symbol);
CREATE INDEX IF NOT EXISTS idx_anomaly_method     ON fact_anomaly_flags(method);
CREATE INDEX IF NOT EXISTS idx_anomaly_flagged_at ON fact_anomaly_flags(flagged_at);
"""

DDL_PEERS = """
CREATE TABLE IF NOT EXISTS fact_peer_mapping (
    id               SERIAL PRIMARY KEY,
    symbol           VARCHAR(20) NOT NULL REFERENCES dim_company(symbol) ON DELETE CASCADE,
    peer_symbol      VARCHAR(20) NOT NULL REFERENCES dim_company(symbol) ON DELETE CASCADE,
    peer_rank        INTEGER     NOT NULL,
    similarity_score NUMERIC(6, 4),
    computed_at      DATE DEFAULT CURRENT_DATE,
    UNIQUE (symbol, peer_rank)
);
CREATE INDEX IF NOT EXISTS idx_peers_symbol ON fact_peer_mapping(symbol);
"""

DDL_YEAR_DATE = """
ALTER TABLE dim_year ADD COLUMN IF NOT EXISTS year_date DATE;
UPDATE dim_year
   SET year_date = MAKE_DATE(fiscal_year, 3, 31)
 WHERE year_date IS NULL AND fiscal_year IS NOT NULL;
"""


def create_tables(engine):
    with engine.begin() as conn:
        conn.execute(text(DDL_ANOMALY))
        conn.execute(text(DDL_PEERS))
        conn.execute(text(DDL_YEAR_DATE))
    print("  Tables and columns verified / created.")


# ---------------------------------------------------------------------------
# Load anomaly flags
# ---------------------------------------------------------------------------

def load_anomaly_flags(engine):
    csv = CLEAN_DIR / "anomaly_flags.csv"
    if not csv.exists():
        print(f"  [SKIP] {csv} not found — run notebook 03 first.")
        return

    df = pd.read_csv(csv)

    # Keep only rows referencing known symbols
    known = set(pd.read_sql("SELECT symbol FROM dim_company", engine)["symbol"])
    before = len(df)
    df = df[df["symbol"].isin(known)].copy()
    print(f"  anomaly_flags: {before} rows → {len(df)} after FK filter")

    if df.empty:
        return

    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")
    df["z_score"]     = pd.to_numeric(df["z_score"],     errors="coerce")
    df = df.where(pd.notna(df), other=None)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_anomaly_flags"))

    df[["symbol","fiscal_year","metric","z_score","direction","method","flagged_at"]].to_sql(
        "fact_anomaly_flags", con=engine,
        if_exists="append", index=False, method="multi",
    )
    print(f"  fact_anomaly_flags: inserted {len(df)} rows.")


# ---------------------------------------------------------------------------
# Load peer mapping
# ---------------------------------------------------------------------------

def load_peer_mapping(engine):
    if not PEER_FILE.exists():
        print(f"  [SKIP] {PEER_FILE} not found — run notebook 05 first.")
        return

    df = pd.read_csv(PEER_FILE)
    known = set(pd.read_sql("SELECT symbol FROM dim_company", engine)["symbol"])

    before = len(df)
    df = df[df["symbol"].isin(known) & df["peer_symbol"].isin(known)].copy()
    print(f"  peer_mapping: {before} rows → {len(df)} after FK filter")

    if df.empty:
        return

    df["similarity_score"] = pd.to_numeric(df["similarity_score"], errors="coerce")
    df["peer_rank"]        = pd.to_numeric(df["peer_rank"], errors="coerce").astype(int)
    df = df.where(pd.notna(df), other=None)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_peer_mapping"))

    df[["symbol","peer_symbol","peer_rank","similarity_score"]].to_sql(
        "fact_peer_mapping", con=engine,
        if_exists="append", index=False, method="multi",
    )
    print(f"  fact_peer_mapping: inserted {len(df)} rows.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n=== ETL Script 6 — Load anomaly flags + peer mapping ===\n")
    engine = get_engine()

    print("Step 1: Create / verify tables …")
    create_tables(engine)

    print("\nStep 2: Load anomaly flags …")
    load_anomaly_flags(engine)

    print("\nStep 3: Load peer mapping …")
    load_peer_mapping(engine)

    print("\n✔ ETL Script 6 complete.\n")


if __name__ == "__main__":
    main()