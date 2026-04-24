"""
ETL Script 4 — ML Company Health Scoring
Reads fact tables from PostgreSQL, computes 7-dimension scores,
writes results to fact_ml_scores and updates fact_pros_cons with ML insights.
Run: python etl/04_ml_scores.py
"""

import os
import warnings
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

DB_URL = os.getenv("NIFTY_DB_URL", "postgresql://postgres:postgres@localhost:5433/nifty100")


def get_engine():
    return create_engine(DB_URL, echo=False)


# ---------------------------------------------------------------------------
# Score normalisation helpers
# ---------------------------------------------------------------------------

def minmax(series: pd.Series, low: float, high: float) -> pd.Series:
    """Clip to [low, high] then scale to 0-100."""
    clipped = series.clip(lower=low, upper=high)
    return ((clipped - low) / (high - low)) * 100


def percentile_score(series: pd.Series) -> pd.Series:
    """Rank-based score 0-100 within the series."""
    return series.rank(pct=True) * 100


# ---------------------------------------------------------------------------
# Load data from warehouse
# ---------------------------------------------------------------------------

def load_data(engine):
    pl = pd.read_sql("""
        SELECT f.symbol, d.fiscal_year,
               f.sales, f.net_profit, f.operating_profit,
               f.interest, f.eps, f.dividend_payout_pct,
               f.net_profit_margin_pct, f.expense_ratio_pct, f.interest_coverage
        FROM fact_profit_loss f
        JOIN dim_year d ON f.year_id = d.year_id
        WHERE d.is_ttm = FALSE AND d.fiscal_year IS NOT NULL
        ORDER BY f.symbol, d.fiscal_year
    """, engine)

    bs = pd.read_sql("""
        SELECT f.symbol, d.fiscal_year,
               f.equity_capital, f.reserves, f.borrowings,
               f.total_assets, f.debt_to_equity, f.equity_ratio
        FROM fact_balance_sheet f
        JOIN dim_year d ON f.year_id = d.year_id
        WHERE d.is_ttm = FALSE AND d.fiscal_year IS NOT NULL
        ORDER BY f.symbol, d.fiscal_year
    """, engine)

    cf = pd.read_sql("""
        SELECT f.symbol, d.fiscal_year,
               f.operating_activity, f.free_cash_flow, f.net_cash_flow
        FROM fact_cash_flow f
        JOIN dim_year d ON f.year_id = d.year_id
        WHERE d.is_ttm = FALSE AND d.fiscal_year IS NOT NULL
        ORDER BY f.symbol, d.fiscal_year
    """, engine)

    an = pd.read_sql("""
        SELECT symbol, period,
               compounded_sales_growth_pct, compounded_profit_growth_pct,
               stock_price_cagr_pct, roe_pct
        FROM fact_analysis
    """, engine)

    companies = pd.read_sql("SELECT symbol, sector FROM dim_company", engine)

    return pl, bs, cf, an, companies


# ---------------------------------------------------------------------------
# Score dimensions
# ---------------------------------------------------------------------------

def score_profitability(pl: pd.DataFrame) -> pd.DataFrame:
    """Latest year profit metrics → 0-100."""
    latest = pl.sort_values("fiscal_year").groupby("symbol").last().reset_index()
    s = pd.DataFrame({"symbol": latest["symbol"]})
    s["npm"]  = minmax(latest["net_profit_margin_pct"].fillna(0), -20, 40)
    s["opm"]  = minmax((latest["operating_profit"] / latest["sales"].replace(0, np.nan) * 100).fillna(0), -10, 50)
    s["ic"]   = minmax(latest["interest_coverage"].fillna(0).clip(upper=30), 0, 30)
    s["profitability_score"] = (s["npm"] * 0.4 + s["opm"] * 0.4 + s["ic"] * 0.2).round(2)
    return s[["symbol", "profitability_score"]]


def score_growth(pl: pd.DataFrame, an: pd.DataFrame) -> pd.DataFrame:
    """CAGR and TTM growth → 0-100."""
    # 3Y analysis data
    an3 = an[an["period"] == "3Y"][["symbol", "compounded_sales_growth_pct",
                                     "compounded_profit_growth_pct"]].copy()

    # YoY revenue growth from P&L
    pl_s = pl.sort_values("fiscal_year")
    pl_s["sales_yoy"] = pl_s.groupby("symbol")["sales"].pct_change() * 100
    yoy = pl_s.groupby("symbol")["sales_yoy"].mean().reset_index()
    yoy.columns = ["symbol", "avg_sales_yoy"]

    df = yoy.merge(an3, on="symbol", how="left")
    df["csg"] = minmax(df["compounded_sales_growth_pct"].fillna(df["avg_sales_yoy"].fillna(0)), -5, 30)
    df["cpg"] = minmax(df["compounded_profit_growth_pct"].fillna(0), -10, 40)
    df["growth_score"] = (df["csg"] * 0.5 + df["cpg"] * 0.5).round(2)
    return df[["symbol", "growth_score"]]


def score_leverage(bs: pd.DataFrame) -> pd.DataFrame:
    """Debt metrics → 0-100 (higher = less debt = better)."""
    latest = bs.sort_values("fiscal_year").groupby("symbol").last().reset_index()
    # Lower D/E is better → invert
    dte = latest["debt_to_equity"].fillna(5).clip(0, 10)
    equity_r = latest["equity_ratio"].fillna(0).clip(0, 1)
    s = pd.DataFrame({"symbol": latest["symbol"]})
    s["dte_score"]    = minmax(-dte, -10, 0)        # invert: 0 debt = 100
    s["equity_score"] = minmax(equity_r, 0, 1)
    s["leverage_score"] = (s["dte_score"] * 0.6 + s["equity_score"] * 0.4).round(2)
    return s[["symbol", "leverage_score"]]


def score_cashflow(cf: pd.DataFrame, pl: pd.DataFrame) -> pd.DataFrame:
    """Operating cash flow quality → 0-100."""
    latest_cf = cf.sort_values("fiscal_year").groupby("symbol").last().reset_index()
    latest_pl = pl.sort_values("fiscal_year").groupby("symbol").last().reset_index()[
        ["symbol", "net_profit"]]
    df = latest_cf.merge(latest_pl, on="symbol", how="left")

    s = pd.DataFrame({"symbol": df["symbol"]})
    s["ocf_score"] = minmax(df["operating_activity"].fillna(0), -5000, 50000)
    s["fcf_score"] = minmax(df["free_cash_flow"].fillna(0), -10000, 40000)

    # Cash conversion: OCF / net profit (>1 = good)
    ccr = (df["operating_activity"] / df["net_profit"].replace(0, np.nan)).fillna(0).clip(-2, 5)
    s["ccr_score"] = minmax(ccr, -2, 5)

    s["cashflow_score"] = (s["ocf_score"] * 0.4 + s["fcf_score"] * 0.3 + s["ccr_score"] * 0.3).round(2)
    return s[["symbol", "cashflow_score"]]


def score_dividend(pl: pd.DataFrame) -> pd.DataFrame:
    """Dividend consistency → 0-100."""
    # Count years with dividend > 0
    div = pl[pl["dividend_payout_pct"].notna() & (pl["dividend_payout_pct"] > 0)]
    consistency = div.groupby("symbol").size().reset_index(name="div_years")
    latest = pl.sort_values("fiscal_year").groupby("symbol").last().reset_index()[
        ["symbol", "dividend_payout_pct"]]
    df = latest.merge(consistency, on="symbol", how="left")
    df["div_years"] = df["div_years"].fillna(0)
    df["div_payout"] = df["dividend_payout_pct"].fillna(0)

    s = pd.DataFrame({"symbol": df["symbol"]})
    s["consistency"] = minmax(df["div_years"], 0, 12)
    s["payout"]      = minmax(df["div_payout"].clip(0, 80), 0, 80)
    s["dividend_score"] = (s["consistency"] * 0.6 + s["payout"] * 0.4).round(2)
    return s[["symbol", "dividend_score"]]


def score_trend(pl: pd.DataFrame) -> pd.DataFrame:
    """3-year revenue & profit trend slope → 0-100."""
    recent = pl[pl["fiscal_year"] >= pl["fiscal_year"].max() - 3].copy()

    records = []
    for sym, grp in recent.groupby("symbol"):
        grp = grp.sort_values("fiscal_year")
        if len(grp) < 2:
            records.append({"symbol": sym, "sales_slope": 0, "profit_slope": 0})
            continue
        x = np.arange(len(grp), dtype=float)
        s_slope = np.polyfit(x, grp["sales"].fillna(0).values, 1)[0] if grp["sales"].notna().sum() > 1 else 0
        p_slope = np.polyfit(x, grp["net_profit"].fillna(0).values, 1)[0] if grp["net_profit"].notna().sum() > 1 else 0
        records.append({"symbol": sym, "sales_slope": s_slope, "profit_slope": p_slope})

    df = pd.DataFrame(records)
    s = pd.DataFrame({"symbol": df["symbol"]})
    s["sales_trend"]  = percentile_score(df["sales_slope"])
    s["profit_trend"] = percentile_score(df["profit_slope"])
    s["trend_score"]  = (s["sales_trend"] * 0.5 + s["profit_trend"] * 0.5).round(2)
    return s[["symbol", "trend_score"]]


# ---------------------------------------------------------------------------
# Combine scores → overall + health label
# ---------------------------------------------------------------------------

WEIGHTS = {
    "profitability_score": 0.25,
    "growth_score":        0.20,
    "leverage_score":      0.15,
    "cashflow_score":      0.20,
    "dividend_score":      0.10,
    "trend_score":         0.10,
}

HEALTH_BANDS = [
    (80, "EXCELLENT"),
    (60, "GOOD"),
    (40, "AVERAGE"),
    (20, "WEAK"),
    (0,  "POOR"),
]


def assign_label(score: float) -> str:
    for threshold, label in HEALTH_BANDS:
        if score >= threshold:
            return label
    return "POOR"


def combine_scores(*dfs) -> pd.DataFrame:
    result = dfs[0]
    for df in dfs[1:]:
        result = result.merge(df, on="symbol", how="outer")

    for col in WEIGHTS:
        result[col] = result[col].fillna(50.0)  # neutral default for missing

    result["overall_score"] = sum(
        result[col] * w for col, w in WEIGHTS.items()
    ).round(2)
    result["health_label"] = result["overall_score"].apply(assign_label)
    result["computed_at"]  = datetime.utcnow()
    return result


# ---------------------------------------------------------------------------
# Auto-generate pros & cons from scores
# ---------------------------------------------------------------------------

def generate_pros_cons(scores: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in scores.iterrows():
        sym = r["symbol"]

        # Pros
        if r["profitability_score"] >= 70:
            rows.append({"symbol": sym, "is_pro": True,
                         "text": f"Strong profitability with score {r['profitability_score']:.0f}/100",
                         "source": "ML", "confidence": r["profitability_score"] / 100})
        if r["growth_score"] >= 65:
            rows.append({"symbol": sym, "is_pro": True,
                         "text": f"Consistent revenue and profit growth (score {r['growth_score']:.0f}/100)",
                         "source": "ML", "confidence": r["growth_score"] / 100})
        if r["cashflow_score"] >= 65:
            rows.append({"symbol": sym, "is_pro": True,
                         "text": f"Healthy free cash flow generation (score {r['cashflow_score']:.0f}/100)",
                         "source": "ML", "confidence": r["cashflow_score"] / 100})
        if r["leverage_score"] >= 70:
            rows.append({"symbol": sym, "is_pro": True,
                         "text": "Low debt burden with strong equity base",
                         "source": "ML", "confidence": r["leverage_score"] / 100})
        if r["dividend_score"] >= 60:
            rows.append({"symbol": sym, "is_pro": True,
                         "text": "Consistent dividend history rewarding shareholders",
                         "source": "ML", "confidence": r["dividend_score"] / 100})

        # Cons
        if r["profitability_score"] < 35:
            rows.append({"symbol": sym, "is_pro": False,
                         "text": f"Below-average profitability (score {r['profitability_score']:.0f}/100)",
                         "source": "ML", "confidence": (100 - r["profitability_score"]) / 100})
        if r["leverage_score"] < 35:
            rows.append({"symbol": sym, "is_pro": False,
                         "text": "High debt levels relative to equity — watch interest costs",
                         "source": "ML", "confidence": (100 - r["leverage_score"]) / 100})
        if r["cashflow_score"] < 35:
            rows.append({"symbol": sym, "is_pro": False,
                         "text": "Weak free cash flow — earnings may not be converting to cash",
                         "source": "ML", "confidence": (100 - r["cashflow_score"]) / 100})
        if r["growth_score"] < 30:
            rows.append({"symbol": sym, "is_pro": False,
                         "text": "Sluggish revenue growth over recent periods",
                         "source": "ML", "confidence": (100 - r["growth_score"]) / 100})

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Write to DB
# ---------------------------------------------------------------------------

def write_scores(scores: pd.DataFrame, engine):
    cols = ["symbol", "computed_at", "overall_score",
            "profitability_score", "growth_score", "leverage_score",
            "cashflow_score", "dividend_score", "trend_score", "health_label"]
    df = scores[cols].copy()

    with engine.begin() as conn:
        # Delete today's run for idempotency
        conn.execute(text(
            "DELETE FROM fact_ml_scores WHERE computed_at::date = CURRENT_DATE"
        ))

    df.to_sql("fact_ml_scores", con=engine, if_exists="append", index=False, method="multi")
    print(f"    fact_ml_scores                 written {len(df)} rows")


def write_ml_pros_cons(pros_cons: pd.DataFrame, engine):
    if pros_cons.empty:
        return
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_pros_cons WHERE source = 'ML'"))
    pros_cons[["symbol", "is_pro", "text", "source"]].to_sql(
        "fact_pros_cons", con=engine, if_exists="append", index=False, method="multi"
    )
    print(f"    fact_pros_cons (ML)            written {len(pros_cons)} rows")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("ETL Step 4 — ML Company Health Scoring")
    print("=" * 60)

    engine = get_engine()

    print("\n  Loading data from warehouse …")
    pl, bs, cf, an, companies = load_data(engine)
    print(f"    P&L rows: {len(pl)}  |  BS rows: {len(bs)}  |  "
          f"CF rows: {len(cf)}  |  Analysis rows: {len(an)}")

    print("\n  Computing dimension scores …")
    s_profit   = score_profitability(pl)
    s_growth   = score_growth(pl, an)
    s_leverage = score_leverage(bs)
    s_cashflow = score_cashflow(cf, pl)
    s_dividend = score_dividend(pl)
    s_trend    = score_trend(pl)

    print("  Combining scores …")
    scores = combine_scores(s_profit, s_growth, s_leverage,
                            s_cashflow, s_dividend, s_trend)
    scores = scores.merge(companies[["symbol"]], on="symbol", how="inner")

    # Print summary
    label_counts = scores["health_label"].value_counts()
    print("\n  Health label distribution:")
    for label, count in label_counts.items():
        print(f"    {label:<12} {count:>3} companies")

    print(f"\n  Top 5 companies by overall score:")
    top5 = scores.nlargest(5, "overall_score")[["symbol", "overall_score", "health_label"]]
    for _, r in top5.iterrows():
        print(f"    {r['symbol']:<15} {r['overall_score']:>6.1f}  [{r['health_label']}]")

    print("\n  Writing scores to warehouse …")
    write_scores(scores, engine)

    print("  Generating ML pros & cons …")
    pros_cons = generate_pros_cons(scores)
    write_ml_pros_cons(pros_cons, engine)

    print("\nML scoring complete.\n")


if __name__ == "__main__":
    main()