"""
ETL Script 5 — Compute & populate fact_analysis for ALL companies.

FIXES:
  1. ROE computed PER PERIOD directly from P&L (Net Profit / cumulative retained
     equity proxy). No balance sheet table required.
  2. Stock CAGR fetched from Yahoo Finance via yfinance for each NSE symbol,
     then stored in fact_stock_price and used for CAGR by period.

Run:
    pip install yfinance sqlalchemy pandas numpy psycopg2-binary
    python etl/05_compute_analysis.py
"""

import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect

# ── optional yfinance ──────────────────────────────────────────────────────
try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False
    print("  [WARN] yfinance not installed. Stock CAGR will be null.")
    print("         Install with: pip install yfinance")

DB_URL = os.getenv(
    "NIFTY_DB_URL",
    "postgresql://postgres:postgres@localhost:5433/nifty100"
)


def get_engine():
    return create_engine(DB_URL, echo=False)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def cagr(start_val, end_val, years: float):
    """Return CAGR % or None."""
    try:
        sv, ev = float(start_val), float(end_val)
    except (TypeError, ValueError):
        return None
    if sv <= 0 or years <= 0:
        return None
    return round(((ev / sv) ** (1.0 / years) - 1) * 100, 2)


# ─────────────────────────────────────────────────────────────────────────────
# LOAD P&L
# ─────────────────────────────────────────────────────────────────────────────

def load_pl(engine) -> pd.DataFrame:
    sql = """
        SELECT
            f.symbol,
            d.fiscal_year,
            d.sort_order,
            d.is_ttm,
            f.sales,
            f.net_profit,
            f.opm_pct,
            f.net_profit_margin_pct,
            f.eps
        FROM fact_profit_loss f
        JOIN dim_year d ON f.year_id = d.year_id
        ORDER BY f.symbol, d.sort_order
    """
    df = pd.read_sql(text(sql), engine)
    for col in ["sales", "net_profit", "eps"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_company_meta(engine) -> pd.DataFrame:
    return pd.read_sql(
        text("SELECT symbol, roe_percentage FROM dim_company"),
        engine
    )


# ─────────────────────────────────────────────────────────────────────────────
# ROE FROM P&L  (no balance sheet needed)
# ─────────────────────────────────────────────────────────────────────────────
#
# Method: Proxy equity using cumulative retained earnings.
#   equity_t ≈ equity_base + sum(net_profit for all years up to t)
# We don't know equity_base, so we use a relative approach:
#   ROE for period = avg_net_profit_in_window / avg_equity_in_window
# where equity_in_window is approximated by the running cumulative sum
# of net profits (oldest year = 0 base, each year adds net_profit).
#
# This gives VARYING ROE per period even without a balance sheet.
# ─────────────────────────────────────────────────────────────────────────────

def compute_roe_per_period(profits: list, window: int) -> float | None:
    """
    profits: list of net_profit values, oldest → newest (non-TTM only).
    window:  number of years for this period (3, 5, 10).
    Returns ROE% for the window.
    """
    if len(profits) < window + 1:
        return None

    # Slice the window: last `window` years
    window_profits = profits[-(window):]
    if not window_profits or all(p is None for p in window_profits):
        return None

    avg_profit = np.nanmean([p for p in window_profits if p is not None])

    # Proxy equity: cumulative sum of ALL historical profits up to this window
    # Acts like a "book value" proxy
    all_profits_to_end = [p for p in profits if p is not None]
    cumulative = np.cumsum(all_profits_to_end)

    # Equity values corresponding to last `window` year-ends
    if len(cumulative) < window:
        return None
    equity_window = cumulative[-(window):]
    avg_equity = np.mean(equity_window)

    if avg_equity <= 0:
        return None

    return round((avg_profit / avg_equity) * 100, 2)


# ─────────────────────────────────────────────────────────────────────────────
# STOCK PRICES via yfinance
# ─────────────────────────────────────────────────────────────────────────────

def ensure_stock_price_table(engine):
    """Create fact_stock_price if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_stock_price (
                symbol      VARCHAR(20) NOT NULL,
                price_date  DATE        NOT NULL,
                close_price NUMERIC(12,4),
                PRIMARY KEY (symbol, price_date)
            )
        """))
    print("  fact_stock_price table ready.")


def fetch_and_store_stock_prices(symbols: list, engine):
    """
    Fetch ~15 years of monthly closing prices from Yahoo Finance for each
    NSE symbol (appends .NS suffix). Stores in fact_stock_price.
    """
    if not HAS_YF:
        return

    ensure_stock_price_table(engine)

    end_dt   = datetime.today()
    start_dt = end_dt - timedelta(days=365 * 15)

    print(f"\n  Fetching stock prices for {len(symbols)} symbols via yfinance …")
    print("  (This may take a few minutes on first run)")

    UPSERT = text("""
        INSERT INTO fact_stock_price (symbol, price_date, close_price)
        VALUES (:symbol, :price_date, :close_price)
        ON CONFLICT (symbol, price_date) DO UPDATE
            SET close_price = EXCLUDED.close_price
    """)

    ok_count = 0
    fail_count = 0

    # Batch download in groups of 20 for speed
    BATCH = 20
    for i in range(0, len(symbols), BATCH):
        batch = symbols[i:i+BATCH]
        tickers = [s + ".NS" for s in batch]
        try:
            raw = yf.download(
                tickers,
                start=start_dt.strftime("%Y-%m-%d"),
                end=end_dt.strftime("%Y-%m-%d"),
                interval="1mo",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if raw.empty:
                fail_count += len(batch)
                continue

            # yf returns MultiIndex columns when multiple tickers
            if isinstance(raw.columns, pd.MultiIndex):
                close = raw["Close"]
            else:
                close = raw[["Close"]]
                close.columns = tickers

            records = []
            for sym, ticker in zip(batch, tickers):
                if ticker not in close.columns:
                    fail_count += 1
                    continue
                s = close[ticker].dropna()
                if s.empty:
                    fail_count += 1
                    continue
                for dt, price in s.items():
                    records.append({
                        "symbol":      sym,
                        "price_date":  dt.date() if hasattr(dt, "date") else dt,
                        "close_price": round(float(price), 4),
                    })
                ok_count += 1

            if records:
                with engine.begin() as conn:
                    conn.execute(UPSERT, records)

        except Exception as e:
            print(f"    [WARN] Batch {batch[:3]}…: {e}")
            fail_count += len(batch)

        # Polite delay to avoid rate-limiting
        time.sleep(0.5)
        if (i // BATCH) % 5 == 0:
            print(f"    Progress: {min(i+BATCH, len(symbols))}/{len(symbols)} symbols …")

    print(f"  Stock prices fetched: {ok_count} OK, {fail_count} failed/no data")


def load_stock_prices(engine) -> pd.DataFrame:
    """Load fact_stock_price from DB."""
    insp = inspect(engine)
    if "fact_stock_price" not in insp.get_table_names():
        return pd.DataFrame()
    df = pd.read_sql(
        text("SELECT symbol, price_date, close_price FROM fact_stock_price ORDER BY symbol, price_date"),
        engine
    )
    df["price_date"]  = pd.to_datetime(df["price_date"])
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    return df.dropna(subset=["close_price"])


def stock_cagr_for_period(sym: str, stock_df: pd.DataFrame, years: int) -> float | None:
    """CAGR of stock price over `years` ending at latest available date."""
    if stock_df.empty:
        return None
    s = stock_df[stock_df["symbol"] == sym].sort_values("price_date")
    if len(s) < 2:
        return None

    end_date   = s["price_date"].max()
    start_date = end_date - pd.DateOffset(years=years)

    # Closest price at or before start_date (within 45 days tolerance)
    before = s[s["price_date"] <= start_date + pd.Timedelta(days=45)]
    if before.empty:
        return None

    end_price   = s.iloc[-1]["close_price"]
    start_price = before.iloc[-1]["close_price"]

    actual_years = (end_date - before.iloc[-1]["price_date"]).days / 365.25
    if actual_years < 0.5:
        return None

    return cagr(start_price, end_price, actual_years)


# ─────────────────────────────────────────────────────────────────────────────
# COMPUTE ANALYSIS ROWS PER COMPANY
# ─────────────────────────────────────────────────────────────────────────────

PERIODS = {"10Y": 10, "5Y": 5, "3Y": 3}


def compute_analysis(
    symbol: str,
    pl_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    static_roe: float | None,
) -> list[dict]:

    rows = []

    hist = pl_df[pl_df["is_ttm"] == False].sort_values("sort_order")
    ttm  = pl_df[pl_df["is_ttm"] == True]

    hist_sales  = hist["sales"].tolist()
    hist_profit = hist["net_profit"].tolist()

    for label, years in PERIODS.items():
        n = len(hist_sales)
        if n < 2:
            continue
        window = min(years, n - 1)

        # Sales CAGR
        sc = cagr(hist_sales[-(window+1)], hist_sales[-1], window)

        # Profit CAGR
        ps = hist_profit[-(window+1)] if len(hist_profit) >= window+1 else None
        pe = hist_profit[-1] if hist_profit else None
        pc = cagr(ps, pe, window) if (ps and pe and ps > 0 and pe > 0) else None

        # ROE — computed per period from P&L proxy
        roe = compute_roe_per_period(hist_profit, window)
        # If proxy gives None (not enough data), fall back to static
        if roe is None:
            roe = static_roe

        # Stock CAGR
        stk = stock_cagr_for_period(symbol, stock_df, years)

        rows.append({
            "symbol": symbol, "period": label,
            "compounded_sales_growth_pct":  sc,
            "compounded_profit_growth_pct": pc,
            "stock_price_cagr_pct":         stk,
            "roe_pct":                      roe,
        })

    # TTM row
    if not ttm.empty:
        tr = ttm.iloc[-1]
        tsc = cagr(hist_sales[-1], float(tr["sales"]), 1) if hist_sales and tr["sales"] else None
        pp  = hist_profit[-1] if hist_profit else None
        tp  = tr["net_profit"]
        tpc = cagr(pp, float(tp), 1) if (pp and tp and float(pp) > 0 and float(tp) > 0) else None

        # TTM ROE: latest year window=1
        roe_ttm = compute_roe_per_period(hist_profit, 1)
        if roe_ttm is None:
            roe_ttm = static_roe

        rows.append({
            "symbol": symbol, "period": "TTM",
            "compounded_sales_growth_pct":  tsc,
            "compounded_profit_growth_pct": tpc,
            "stock_price_cagr_pct":         None,  # TTM stock CAGR not meaningful
            "roe_pct":                      roe_ttm,
        })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# UPSERT
# ─────────────────────────────────────────────────────────────────────────────

UPSERT_SQL = text("""
    INSERT INTO fact_analysis
        (symbol, period, compounded_sales_growth_pct,
         compounded_profit_growth_pct, stock_price_cagr_pct, roe_pct)
    VALUES
        (:symbol, :period, :compounded_sales_growth_pct,
         :compounded_profit_growth_pct, :stock_price_cagr_pct, :roe_pct)
    ON CONFLICT (symbol, period) DO UPDATE SET
        compounded_sales_growth_pct  = EXCLUDED.compounded_sales_growth_pct,
        compounded_profit_growth_pct = EXCLUDED.compounded_profit_growth_pct,
        stock_price_cagr_pct         = EXCLUDED.stock_price_cagr_pct,
        roe_pct                      = EXCLUDED.roe_pct
""")


def write_analysis(records: list[dict], engine):
    if not records:
        return
    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, records)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("ETL Step 5 — Compute CAGR + ROE + Stock CAGR")
    print("=" * 60)

    engine = get_engine()

    print("\n  Loading P&L data …")
    pl_df = load_pl(engine)

    meta_df  = load_company_meta(engine)
    roe_map  = dict(zip(meta_df["symbol"], meta_df["roe_percentage"]))
    symbols  = sorted(pl_df["symbol"].unique().tolist())
    print(f"  Companies: {len(symbols)}")

    # ── Fetch / refresh stock prices ────────────────────────────────────────
    fetch_and_store_stock_prices(symbols, engine)
    stock_df = load_stock_prices(engine)
    print(f"  Stock price rows loaded: {len(stock_df)}")
    print(f"  Symbols with stock data: {stock_df['symbol'].nunique() if not stock_df.empty else 0}")

    # ── Compute analysis ────────────────────────────────────────────────────
    before = pd.read_sql(text("SELECT COUNT(*) FROM fact_analysis"), engine).iloc[0, 0]
    print(f"\n  fact_analysis rows before: {before}")

    all_records, skipped = [], []

    for sym in symbols:
        pl_sym   = pl_df[pl_df["symbol"] == sym].copy()
        static_r = roe_map.get(sym)
        if static_r is not None:
            try:
                static_r = float(static_r)
            except (TypeError, ValueError):
                static_r = None

        recs = compute_analysis(sym, pl_sym, stock_df, static_r)
        if recs:
            all_records.extend(recs)
        else:
            skipped.append(sym)

    print(f"  Computed {len(all_records)} rows for {len(symbols)-len(skipped)} companies")
    if skipped:
        print(f"  Skipped: {skipped}")

    print("\n  Writing to fact_analysis …")
    BATCH = 100
    for i in range(0, len(all_records), BATCH):
        write_analysis(all_records[i:i+BATCH], engine)

    after = pd.read_sql(text("SELECT COUNT(*) FROM fact_analysis"), engine).iloc[0, 0]
    print(f"  fact_analysis rows after: {after}  (+{after - before})")

    # ── Verify ROE variation ─────────────────────────────────────────────────
    roe_check = pd.read_sql(text("""
        SELECT symbol,
               MAX(roe_pct) - MIN(roe_pct) AS roe_spread
        FROM fact_analysis
        WHERE roe_pct IS NOT NULL
        GROUP BY symbol
        ORDER BY roe_spread DESC
        LIMIT 10
    """), engine)
    print("\n  Top 10 companies by ROE spread across periods (should be > 0):")
    print(roe_check.to_string(index=False))

    # ── Verify stock CAGR ────────────────────────────────────────────────────
    stk_check = pd.read_sql(text("""
        SELECT COUNT(*) AS total_rows,
               COUNT(stock_price_cagr_pct) AS rows_with_stock_cagr
        FROM fact_analysis
    """), engine).iloc[0]
    print(f"\n  Stock CAGR: {stk_check['rows_with_stock_cagr']}/{stk_check['total_rows']} rows populated")

    # Sample
    sample = pd.read_sql(text("""
        SELECT symbol, period,
               compounded_sales_growth_pct  AS sales_cagr,
               compounded_profit_growth_pct AS profit_cagr,
               stock_price_cagr_pct         AS stock_cagr,
               roe_pct
        FROM fact_analysis
        ORDER BY symbol, period
        LIMIT 20
    """), engine)
    print("\n  Sample:\n", sample.to_string(index=False))
    print("\nDone ✓")
    print("Now refresh dashboards 05 & 07 — ROE will vary per period,")
    print("Stock CAGR will show real values from Yahoo Finance.")


if __name__ == "__main__":
    main()