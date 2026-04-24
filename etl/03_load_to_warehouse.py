"""
ETL Script 3 — Load clean CSVs into the PostgreSQL star-schema warehouse.
Run: python etl/03_load_to_warehouse.py
Requires: pip install sqlalchemy psycopg2-binary pandas
Set DB credentials via environment variables or edit DB_URL below.
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import MetaData

CLEAN_DIR = Path("data/clean")

# ---------------------------------------------------------------------------
# Database connection  — override via environment variables
# ---------------------------------------------------------------------------
DB_URL = os.getenv(
    "NIFTY_DB_URL",
    "postgresql://postgres:postgres@localhost:5433/nifty100"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_engine():
    return create_engine(DB_URL, echo=False)


def safe_float(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            df[c] = df[c].where(df[c].notna(), other=None)
    return df


def safe_bool(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(
                {"True": True, "False": False, True: True, False: False}
            ).fillna(False)
    return df


# ---------------------------------------------------------------------------
# Core upsert — chunked + ON CONFLICT DO UPDATE
# ---------------------------------------------------------------------------

UPSERT_CHUNK_SIZE = 200


def upsert(engine, df: pd.DataFrame, table: str, pk_cols: list):
    """
    Idempotent upsert using PostgreSQL INSERT ... ON CONFLICT DO UPDATE.
    * Deduplicates the DataFrame on pk_cols first.
    * Inserts in chunks of UPSERT_CHUNK_SIZE to stay under the
      65 535-parameter limit.
    * Safe to run multiple times (full idempotency).
    """
    if df.empty:
        print(f"    [SKIP] {table} — DataFrame is empty")
        return

    # Replace NaN / NaT with None for SQL NULL
    df = df.where(pd.notna(df), other=None)

    # Deduplicate on PKs — keep last occurrence
    before_dedup = len(df)
    df = df.drop_duplicates(subset=pk_cols, keep="last").reset_index(drop=True)
    if len(df) < before_dedup:
        print(f"      INFO: {table} — dropped {before_dedup - len(df)} "
              f"duplicate PK rows from source")

    # Reflect table metadata so SQLAlchemy knows column types
    meta = MetaData()
    meta.reflect(bind=engine, only=[table])
    tbl = meta.tables[table]

    non_pk_cols = [c for c in df.columns if c not in pk_cols]

    with engine.begin() as conn:
        before = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()

        for start in range(0, len(df), UPSERT_CHUNK_SIZE):
            chunk = df.iloc[start: start + UPSERT_CHUNK_SIZE]
            records = chunk.to_dict(orient="records")

            stmt = pg_insert(tbl).values(records)

            if non_pk_cols:
                stmt = stmt.on_conflict_do_update(
                    index_elements=pk_cols,
                    set_={col: stmt.excluded[col] for col in non_pk_cols},
                )
            else:
                stmt = stmt.on_conflict_do_nothing()

            conn.execute(stmt)

        after = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()

    print(f"    {table:<30} before={before:>6}  upserted={len(df):>5}  after={after:>6}")


# ---------------------------------------------------------------------------
# FK helpers — keep only symbols known to dim_company
# ---------------------------------------------------------------------------

def _get_known_symbols(engine) -> set:
    return set(pd.read_sql("SELECT symbol FROM dim_company", engine)["symbol"].tolist())


def _filter_to_known_symbols(df: pd.DataFrame, known: set, label: str) -> pd.DataFrame:
    mask = df["symbol"].isin(known)
    dropped = int((~mask).sum())
    if dropped:
        missing = sorted(df.loc[~mask, "symbol"].unique().tolist())
        print(f"      WARNING: {label} — dropping {dropped} rows for "
              f"{len(missing)} unknown symbol(s): {missing}")
    return df[mask].reset_index(drop=True)


def _map_year_ids(df: pd.DataFrame, engine) -> pd.DataFrame:
    """Join on year_label → year_id, drop unmapped rows."""
    year_map = pd.read_sql("SELECT year_id, year_label FROM dim_year", engine)
    df = df.merge(year_map, on="year_label", how="left")
    missing = int(df["year_id"].isna().sum())
    if missing:
        print(f"      WARNING: {missing} rows have unmapped year_label — dropped")
        df = df.dropna(subset=["year_id"])
    df["year_id"] = df["year_id"].astype(int)
    return df


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS dim_company (
    symbol              VARCHAR(20) PRIMARY KEY,
    company_name        TEXT,
    sector              VARCHAR(50),
    company_logo        TEXT,
    website             TEXT,
    nse_profile         TEXT,
    bse_profile         TEXT,
    chart_link          TEXT,
    about_company       TEXT,
    face_value          NUMERIC(10,2),
    book_value          NUMERIC(12,2),
    roce_percentage     NUMERIC(8,2),
    roe_percentage      NUMERIC(8,2)
);

CREATE TABLE IF NOT EXISTS dim_sector (
    sector_id   SERIAL PRIMARY KEY,
    sector_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_year (
    year_id      INT PRIMARY KEY,
    year_label   VARCHAR(20) UNIQUE NOT NULL,
    fiscal_year  INT,
    sort_order   INT,
    is_ttm       BOOLEAN DEFAULT FALSE,
    is_half_year BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_health_label (
    label_id   SERIAL PRIMARY KEY,
    label_name VARCHAR(20) UNIQUE NOT NULL,
    min_score  NUMERIC(5,2),
    max_score  NUMERIC(5,2),
    color_hex  VARCHAR(7)
);

CREATE TABLE IF NOT EXISTS fact_profit_loss (
    symbol                  VARCHAR(20) REFERENCES dim_company(symbol),
    year_id                 INT REFERENCES dim_year(year_id),
    sales                   NUMERIC(18,2),
    expenses                NUMERIC(18,2),
    operating_profit        NUMERIC(18,2),
    opm_pct                 NUMERIC(8,2),
    other_income            NUMERIC(18,2),
    interest                NUMERIC(18,2),
    depreciation            NUMERIC(18,2),
    profit_before_tax       NUMERIC(18,2),
    tax_pct                 NUMERIC(8,2),
    net_profit              NUMERIC(18,2),
    eps                     NUMERIC(10,2),
    dividend_payout_pct     NUMERIC(8,2),
    net_profit_margin_pct   NUMERIC(8,2),
    expense_ratio_pct       NUMERIC(8,2),
    interest_coverage       NUMERIC(10,2),
    PRIMARY KEY (symbol, year_id)
);

CREATE TABLE IF NOT EXISTS fact_balance_sheet (
    symbol              VARCHAR(20) REFERENCES dim_company(symbol),
    year_id             INT REFERENCES dim_year(year_id),
    equity_capital      NUMERIC(18,2),
    reserves            NUMERIC(18,2),
    borrowings          NUMERIC(18,2),
    other_liabilities   NUMERIC(18,2),
    total_liabilities   NUMERIC(18,2),
    fixed_assets        NUMERIC(18,2),
    cwip                NUMERIC(18,2),
    investments         NUMERIC(18,2),
    other_asset         NUMERIC(18,2),
    total_assets        NUMERIC(18,2),
    debt_to_equity      NUMERIC(10,4),
    equity_ratio        NUMERIC(10,4),
    PRIMARY KEY (symbol, year_id)
);

CREATE TABLE IF NOT EXISTS fact_cash_flow (
    symbol              VARCHAR(20) REFERENCES dim_company(symbol),
    year_id             INT REFERENCES dim_year(year_id),
    operating_activity  NUMERIC(18,2),
    investing_activity  NUMERIC(18,2),
    financing_activity  NUMERIC(18,2),
    net_cash_flow       NUMERIC(18,2),
    free_cash_flow      NUMERIC(18,2),
    PRIMARY KEY (symbol, year_id)
);

CREATE TABLE IF NOT EXISTS fact_analysis (
    symbol                          VARCHAR(20) REFERENCES dim_company(symbol),
    period                          VARCHAR(10),
    compounded_sales_growth_pct     NUMERIC(8,2),
    compounded_profit_growth_pct    NUMERIC(8,2),
    stock_price_cagr_pct            NUMERIC(8,2),
    roe_pct                         NUMERIC(8,2),
    PRIMARY KEY (symbol, period)
);

CREATE TABLE IF NOT EXISTS fact_pros_cons (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(20) REFERENCES dim_company(symbol),
    is_pro      BOOLEAN,
    text        TEXT,
    source      VARCHAR(10) DEFAULT 'MANUAL',
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_documents (
    id                  SERIAL PRIMARY KEY,
    symbol              VARCHAR(20) REFERENCES dim_company(symbol),
    year                VARCHAR(10),
    annual_report_url   TEXT
);

CREATE TABLE IF NOT EXISTS fact_ml_scores (
    symbol              VARCHAR(20) REFERENCES dim_company(symbol),
    computed_at         TIMESTAMP DEFAULT NOW(),
    overall_score       NUMERIC(6,2),
    profitability_score NUMERIC(6,2),
    growth_score        NUMERIC(6,2),
    leverage_score      NUMERIC(6,2),
    cashflow_score      NUMERIC(6,2),
    dividend_score      NUMERIC(6,2),
    trend_score         NUMERIC(6,2),
    health_label        VARCHAR(20),
    PRIMARY KEY (symbol, computed_at)
);
"""

SEED_HEALTH_LABELS = [
    ("EXCELLENT", 80, 100, "#22c55e"),
    ("GOOD",      60,  80, "#84cc16"),
    ("AVERAGE",   40,  60, "#f59e0b"),
    ("WEAK",      20,  40, "#f97316"),
    ("POOR",       0,  20, "#ef4444"),
]


# ---------------------------------------------------------------------------
# Dimension loaders
# ---------------------------------------------------------------------------

def load_dim_company(engine):
    df = pd.read_csv(CLEAN_DIR / "companies.csv", dtype=str)
    df = safe_float(df, ["face_value", "book_value", "roce_percentage", "roe_percentage"])
    cols = ["symbol", "company_name", "sector", "company_logo", "website",
            "nse_profile", "bse_profile", "chart_link", "about_company",
            "face_value", "book_value", "roce_percentage", "roe_percentage"]
    upsert(engine, df[[c for c in cols if c in df.columns]], "dim_company", ["symbol"])


def load_dim_sector(engine):
    df = pd.read_csv(CLEAN_DIR / "sector_mapping.csv", dtype=str)
    sectors = df["sector"].dropna().unique()
    with engine.begin() as conn:
        for s in sorted(sectors):
            conn.execute(
                text("INSERT INTO dim_sector (sector_name) VALUES (:s) "
                     "ON CONFLICT (sector_name) DO NOTHING"),
                {"s": s},
            )
    print(f"    dim_sector                     seeded {len(sectors)} sectors")


def load_dim_year(engine):
    df = pd.read_csv(CLEAN_DIR / "dim_year.csv", dtype=str)
    df = safe_bool(df, ["is_ttm", "is_half_year"])

    sql = text("""
        INSERT INTO dim_year (year_id, year_label, fiscal_year, sort_order, is_ttm, is_half_year)
        VALUES (:year_id, :year_label, :fiscal_year, :sort_order, :is_ttm, :is_half_year)
        ON CONFLICT (year_id) DO UPDATE SET
            year_label   = EXCLUDED.year_label,
            fiscal_year  = EXCLUDED.fiscal_year,
            sort_order   = EXCLUDED.sort_order,
            is_ttm       = EXCLUDED.is_ttm,
            is_half_year = EXCLUDED.is_half_year
    """)

    def si(x):
        """String/float/nan → plain Python int, or None."""
        try:
            s = str(x).strip()
            if s in ("", "nan", "None", "NaN", "NULL"):
                return None
            return int(float(s))
        except (ValueError, TypeError):
            return None

    with engine.begin() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM dim_year")).scalar()
        count = 0
        for _, row in df.iterrows():
            year_id = si(row.get("year_id"))
            if year_id is None:
                continue          # skip rows with no PK
            conn.execute(sql, {
                "year_id":      year_id,
                "year_label":   str(row.get("year_label", "") or ""),
                "fiscal_year":  si(row.get("fiscal_year")),
                "sort_order":   si(row.get("sort_order")),
                "is_ttm":       bool(row.get("is_ttm", False)),
                "is_half_year": bool(row.get("is_half_year", False)),
            })
            count += 1
        after = conn.execute(text("SELECT COUNT(*) FROM dim_year")).scalar()

    print(f"    {'dim_year':<30} before={before:>6}  upserted={count:>5}  after={after:>6}")

def load_dim_health_labels(engine):
    with engine.begin() as conn:
        for name, mn, mx, color in SEED_HEALTH_LABELS:
            conn.execute(
                text("""
                    INSERT INTO dim_health_label (label_name, min_score, max_score, color_hex)
                    VALUES (:n, :mn, :mx, :c)
                    ON CONFLICT (label_name) DO UPDATE
                    SET min_score = :mn, max_score = :mx, color_hex = :c
                """),
                {"n": name, "mn": mn, "mx": mx, "c": color},
            )
    print(f"    dim_health_label               seeded {len(SEED_HEALTH_LABELS)} labels")


# ---------------------------------------------------------------------------
# Fact loaders
# ---------------------------------------------------------------------------

def load_fact_profit_loss(engine):
    known = _get_known_symbols(engine)
    df = pd.read_csv(CLEAN_DIR / "profitandloss.csv", dtype=str)
    df = _map_year_ids(df, engine)
    df = df.rename(columns={
        "company_id":       "symbol",
        "opm_percentage":   "opm_pct",
        "tax_percentage":   "tax_pct",
        "dividend_payout":  "dividend_payout_pct",
    })
    df = _filter_to_known_symbols(df, known, "fact_profit_loss")
    num_cols = ["sales", "expenses", "operating_profit", "opm_pct", "other_income",
                "interest", "depreciation", "profit_before_tax", "tax_pct",
                "net_profit", "eps", "dividend_payout_pct",
                "net_profit_margin_pct", "expense_ratio_pct", "interest_coverage"]
    df = safe_float(df, num_cols)
    keep = ["symbol", "year_id"] + num_cols
    upsert(engine, df[[c for c in keep if c in df.columns]], "fact_profit_loss",
           ["symbol", "year_id"])


def load_fact_balance_sheet(engine):
    known = _get_known_symbols(engine)
    df = pd.read_csv(CLEAN_DIR / "balancesheet.csv", dtype=str)
    df = _map_year_ids(df, engine)
    df = df.rename(columns={"company_id": "symbol"})
    df = _filter_to_known_symbols(df, known, "fact_balance_sheet")
    num_cols = ["equity_capital", "reserves", "borrowings", "other_liabilities",
                "total_liabilities", "fixed_assets", "cwip", "investments",
                "other_asset", "total_assets", "debt_to_equity", "equity_ratio"]
    df = safe_float(df, num_cols)
    keep = ["symbol", "year_id"] + num_cols
    upsert(engine, df[[c for c in keep if c in df.columns]], "fact_balance_sheet",
           ["symbol", "year_id"])


def load_fact_cash_flow(engine):
    known = _get_known_symbols(engine)
    df = pd.read_csv(CLEAN_DIR / "cashflow.csv", dtype=str)
    df = _map_year_ids(df, engine)
    df = df.rename(columns={"company_id": "symbol"})
    df = _filter_to_known_symbols(df, known, "fact_cash_flow")
    num_cols = ["operating_activity", "investing_activity", "financing_activity",
                "net_cash_flow", "free_cash_flow"]
    df = safe_float(df, num_cols)
    keep = ["symbol", "year_id"] + num_cols
    upsert(engine, df[[c for c in keep if c in df.columns]], "fact_cash_flow",
           ["symbol", "year_id"])


def load_fact_analysis(engine):
    known = _get_known_symbols(engine)
    df = pd.read_csv(CLEAN_DIR / "analysis.csv", dtype=str)
    df = df.rename(columns={"company_id": "symbol"})
    df = _filter_to_known_symbols(df, known, "fact_analysis")
    num_cols = ["compounded_sales_growth_pct", "compounded_profit_growth_pct",
                "stock_price_cagr_pct", "roe_pct"]
    df = safe_float(df, num_cols)
    upsert(engine, df, "fact_analysis", ["symbol", "period"])


def load_fact_pros_cons(engine):
    known = _get_known_symbols(engine)
    df = pd.read_csv(CLEAN_DIR / "prosandcons.csv", dtype=str)
    df = df.rename(columns={"company_id": "symbol"})
    df = _filter_to_known_symbols(df, known, "fact_pros_cons")
    df = safe_bool(df, ["is_pro"])
    df["source"] = "MANUAL"
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_pros_cons WHERE source = 'MANUAL'"))
    # pros_cons has a SERIAL PK so plain insert is fine (no conflict possible)
    for start in range(0, len(df), UPSERT_CHUNK_SIZE):
        chunk = df.iloc[start: start + UPSERT_CHUNK_SIZE]
        chunk[["symbol", "is_pro", "text", "source"]].to_sql(
            "fact_pros_cons", con=engine, if_exists="append",
            index=False, method="multi",
        )
    print(f"    {'fact_pros_cons':<30} inserted {len(df)} rows")


def load_fact_documents(engine):
    known = _get_known_symbols(engine)
    df = pd.read_csv(CLEAN_DIR / "documents.csv", dtype=str)
    df = df.rename(columns={"company_id": "symbol"})
    df = _filter_to_known_symbols(df, known, "fact_documents")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE fact_documents RESTART IDENTITY"))
    for start in range(0, len(df), UPSERT_CHUNK_SIZE):
        chunk = df.iloc[start: start + UPSERT_CHUNK_SIZE]
        chunk[["symbol", "year", "annual_report_url"]].to_sql(
            "fact_documents", con=engine, if_exists="append",
            index=False, method="multi",
        )
    print(f"    {'fact_documents':<30} inserted {len(df)} rows")


# ---------------------------------------------------------------------------
# Data quality checks
# ---------------------------------------------------------------------------

QUALITY_CHECKS = [
    (
        "dim_company row count >= 80",
        "SELECT COUNT(*) FROM dim_company",
        lambda v: v >= 80,
    ),
    (
        "fact_profit_loss has no orphan symbols",
        """SELECT COUNT(*) FROM fact_profit_loss f
           LEFT JOIN dim_company c ON f.symbol = c.symbol
           WHERE c.symbol IS NULL""",
        lambda v: v == 0,
    ),
    (
        "fact_balance_sheet has no orphan symbols",
        """SELECT COUNT(*) FROM fact_balance_sheet f
           LEFT JOIN dim_company c ON f.symbol = c.symbol
           WHERE c.symbol IS NULL""",
        lambda v: v == 0,
    ),
    (
        "fact_cash_flow has no orphan symbols",
        """SELECT COUNT(*) FROM fact_cash_flow f
           LEFT JOIN dim_company c ON f.symbol = c.symbol
           WHERE c.symbol IS NULL""",
        lambda v: v == 0,
    ),
    (
        "dim_year sort_order is unique",
        """SELECT COUNT(*) FROM (
               SELECT sort_order, COUNT(*) c
               FROM dim_year
               GROUP BY sort_order
               HAVING COUNT(*) > 1
           ) t""",
        lambda v: v == 0,
    ),
    (
        "dim_health_label seeded with 5 rows",
        "SELECT COUNT(*) FROM dim_health_label",
        lambda v: v == 5,
    ),
]


def run_quality_checks(engine):
    print("\n  Running data quality checks …")
    all_pass = True
    for name, sql, check_fn in QUALITY_CHECKS:
        val = pd.read_sql(text(sql), engine).iloc[0, 0]
        ok = check_fn(val)
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False
        print(f"    [{status}] {name}  (value={val})")
    if all_pass:
        print("  All quality checks passed ✓")
    else:
        print("  Some checks FAILED — review above output")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("ETL Step 3 — Loading data warehouse")
    print(f"  Target: {DB_URL.split('@')[-1]}")
    print("=" * 60)

    engine = get_engine()

    # 3a. Create schema
    print("\n  Creating schema …")
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("  Schema ready.")

    # 3b. Dimension tables first (FK dependencies)
    print("\n  Loading dimension tables …")
    load_dim_health_labels(engine)
    load_dim_sector(engine)
    load_dim_company(engine)
    load_dim_year(engine)

    # 3c. Fact tables
    print("\n  Loading fact tables …")
    load_fact_profit_loss(engine)
    load_fact_balance_sheet(engine)
    load_fact_cash_flow(engine)
    load_fact_analysis(engine)
    load_fact_pros_cons(engine)
    load_fact_documents(engine)

    # 3d. Quality checks
    run_quality_checks(engine)

    print("\nLoad complete.\n")


if __name__ == "__main__":
    main()