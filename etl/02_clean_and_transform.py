"""
ETL Script 2 — Clean, standardise, and enrich raw CSVs.
Reads data/raw/, applies all transformations, saves to data/clean/.
Run: python etl/02_clean_and_transform.py
"""

import re
import pandas as pd
import numpy as np
from pathlib import Path

RAW_DIR = Path("data/raw")
CLEAN_DIR = Path("data/clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# 1. SECTOR MAPPING  (all 92 Nifty 100 companies hand-classified)
# ---------------------------------------------------------------------------

SECTOR_MAP = {
    # IT
    "TCS": "IT", "INFY": "IT", "WIPRO": "IT", "HCLTECH": "IT",
    "TECHM": "IT", "LTIM": "IT", "PERSISTENT": "IT", "COFORGE": "IT",
    "MPHASIS": "IT",
    # Banking
    "HDFCBANK": "Banking", "ICICIBANK": "Banking", "KOTAKBANK": "Banking",
    "SBIN": "Banking", "AXISBANK": "Banking", "INDUSINDBK": "Banking",
    "BANKBARODA": "Banking", "CANBK": "Banking", "PNB": "Banking",
    "FEDERALBNK": "Banking", "IDFCFIRSTB": "Banking",
    # NBFC / Finance
    "BAJFINANCE": "NBFC", "BAJAJFINSV": "NBFC", "CHOLAFIN": "NBFC",
    "MUTHOOTFIN": "NBFC", "PFC": "NBFC", "RECLTD": "NBFC",
    # Insurance
    "SBILIFE": "Insurance", "HDFCLIFE": "Insurance", "ICICIGI": "Insurance",
    "ICICIPRULI": "Insurance",
    # Energy / Oil & Gas
    "RELIANCE": "Energy", "ONGC": "Energy", "IOC": "Energy",
    "BPCL": "Energy", "HINDPETRO": "Energy", "GAIL": "Energy",
    "ATGL": "Energy", "MGL": "Energy",
    # Power
    "ADANIGREEN": "Power", "ADANIPOWER": "Power", "ADANIENSOL": "Power",
    "NTPC": "Power", "POWERGRID": "Power", "TATAPOWER": "Power",
    # Ports & Infrastructure
    "ADANIPORTS": "Infrastructure", "ADANIENT": "Infrastructure",
    "LTTS": "Infrastructure", "LT": "Infrastructure",
    # Cement
    "AMBUJACEM": "Cement", "ULTRACEMCO": "Cement", "SHREECEM": "Cement",
    "ACC": "Cement",
    # Healthcare / Pharma
    "APOLLOHOSP": "Healthcare", "FORTIS": "Healthcare",
    "SUNPHARMA": "Pharma", "DRREDDY": "Pharma", "CIPLA": "Pharma",
    "DIVISLAB": "Pharma", "AUROPHARMA": "Pharma", "LUPIN": "Pharma",
    "ALKEM": "Pharma", "TORNTPHARM": "Pharma",
    # Auto
    "BAJAJ-AUTO": "Auto", "MARUTI": "Auto", "TATAMOTORS": "Auto",
    "EICHERMOT": "Auto", "HEROMOTOCO": "Auto", "M&M": "Auto",
    "TVSMOTOR": "Auto", "BALKRISIND": "Auto",
    # Paints
    "ASIANPAINT": "Paints", "BERGEPAINT": "Paints",
    # Consumer Goods / FMCG
    "HINDUNILVR": "FMCG", "ITC": "FMCG", "NESTLEIND": "FMCG",
    "BRITANNIA": "FMCG", "DABUR": "FMCG", "GODREJCP": "FMCG",
    "MARICO": "FMCG", "COLPAL": "FMCG", "EMAMILTD": "FMCG",
    # Metals & Mining
    "TATASTEEL": "Metals", "HINDALCO": "Metals", "JSWSTEEL": "Metals",
    "VEDL": "Metals", "COALINDIA": "Metals", "NMDC": "Metals",
    "SAIL": "Metals",
    # Telecom
    "BHARTIARTL": "Telecom",
    # Retail / Consumer Discretionary
    "TRENT": "Retail", "DMART": "Retail",
    # Financial Services / Holding
    "BAJAJHLDNG": "Holding Company",
    # Industrials
    "ABB": "Industrials", "SIEMENS": "Industrials", "HAVELLS": "Industrials",
    "CUMMINSIND": "Industrials", "BHEL": "Industrials",
    # Conglomerate / Diversified
    "ADANIENTERP": "Conglomerate",
    # Real Estate
    "DLF": "Real Estate", "GODREJPROP": "Real Estate",
}


# ---------------------------------------------------------------------------
# 2. YEAR STANDARDISATION
# ---------------------------------------------------------------------------

MONTH_SHORT = {
    "jan": "Jan", "feb": "Feb", "mar": "Mar", "apr": "Apr",
    "may": "May", "jun": "Jun", "jul": "Jul", "aug": "Aug",
    "sep": "Sep", "oct": "Oct", "nov": "Nov", "dec": "Dec",
}

MONTH_TO_NUM = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# Fiscal year ends in March for most Indian companies.
# For companies with Dec year-end, the fiscal year = calendar year.
def _fiscal_year(month: str, year_int: int) -> int:
    """Return fiscal year integer (ending year convention)."""
    m = MONTH_TO_NUM.get(month, 3)
    if m < 4:          # Jan / Feb / Mar → same year label
        return year_int
    else:              # Apr–Dec → next calendar year is the FY label
        return year_int + 1


def _sort_order(month: str, year_int: int) -> int:
    """Integer for chronological sorting (YYYYMM)."""
    m = MONTH_TO_NUM.get(month, 3)
    return year_int * 100 + m


def standardise_year(raw: str) -> dict:
    """
    Convert any raw year string to a canonical dict:
      year_label   : 'Mar 2024'
      fiscal_year  : 2024  (int)
      sort_order   : 202403 (int)
      is_ttm       : bool
      is_half_year : bool
    """
    raw = str(raw).strip()
    result = {
        "year_label": raw,
        "fiscal_year": None,
        "sort_order": 0,
        "is_ttm": False,
        "is_half_year": False,
    }

    if raw.upper() in ("TTM", "TTM "):
        result["year_label"] = "TTM"
        result["is_ttm"] = True
        result["sort_order"] = 999999
        return result

    # Check for half-year suffix e.g. '2024.5'
    if raw.endswith(".5"):
        try:
            yr = int(float(raw))
            result["year_label"] = f"Sep {yr}"
            result["fiscal_year"] = _fiscal_year("Sep", yr)
            result["sort_order"] = _sort_order("Sep", yr)
            result["is_half_year"] = True
            return result
        except ValueError:
            pass

    # Pure integer year e.g. '2024', '2013'
    if re.fullmatch(r"\d{4}", raw):
        yr = int(raw)
        result["year_label"] = f"Mar {yr}"
        result["fiscal_year"] = yr
        result["sort_order"] = _sort_order("Mar", yr)
        return result

    # 'Mar-24' style (abbreviation with 2-digit year)
    m = re.fullmatch(r"([A-Za-z]{3})-(\d{2})", raw)
    if m:
        mon = MONTH_SHORT.get(m.group(1).lower(), m.group(1).capitalize())
        yr = 2000 + int(m.group(2))
        label = f"{mon} {yr}"
        result["year_label"] = label
        result["fiscal_year"] = _fiscal_year(mon, yr)
        result["sort_order"] = _sort_order(mon, yr)
        return result

    # 'Mar 2024' or 'Mar 2016 9m' or 'Mar 2023 15'
    m = re.fullmatch(r"([A-Za-z]{3})\s+(\d{4})(\s+.*)?", raw)
    if m:
        mon = MONTH_SHORT.get(m.group(1).lower(), m.group(1).capitalize())
        yr = int(m.group(2))
        suffix = (m.group(3) or "").strip()
        label = f"{mon} {yr}"
        if suffix:
            label += f" ({suffix})"
            result["is_half_year"] = True
        result["year_label"] = label
        result["fiscal_year"] = _fiscal_year(mon, yr)
        result["sort_order"] = _sort_order(mon, yr)
        return result

    # Fallback — leave as-is
    return result


def add_year_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add year_label, fiscal_year, sort_order, is_ttm, is_half_year columns."""
    parsed = df["year"].apply(standardise_year).apply(pd.Series)
    return pd.concat([df.drop(columns=["year"]), parsed], axis=1)


# ---------------------------------------------------------------------------
# 3. ANALYSIS TABLE PARSER
# ---------------------------------------------------------------------------

def _parse_growth_cell(cell: str) -> tuple[str | None, float | None]:
    """
    '10 Years: 21%' → ('10Y', 21.0)
    '5 Years:  24%' → ('5Y', 24.0)
    '3 Years:   9%' → ('3Y', 9.0)
    'TTM:       7%' → ('TTM', 7.0)
    Returns (period, value) or (None, None)
    """
    if not cell or str(cell).strip() in ("", "nan", "None"):
        return None, None
    cell = str(cell).strip()
    m = re.search(r"(\d+)\s*[Yy]ears?", cell)
    period = f"{m.group(1)}Y" if m else ("TTM" if "TTM" in cell.upper() else None)
    v = re.search(r"([-\d.]+)\s*%", cell)
    value = float(v.group(1)) if v else None
    return period, value


def parse_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Explode wide analysis rows into long format:
    (company_id, period, compounded_sales_growth_pct, compounded_profit_growth_pct,
     stock_price_cagr_pct, roe_pct)
    """
    records = []
    for _, row in df.iterrows():
        company = row["company_id"]
        # Each metric column may have a period prefix like "10 Years: 21%"
        # We derive the period from any non-null column
        period = None
        for col in ["compounded_sales_growth", "compounded_profit_growth", "stock_price_cagr", "roe"]:
            p, _ = _parse_growth_cell(row.get(col))
            if p:
                period = p
                break

        if not period:
            continue

        _, csg = _parse_growth_cell(row.get("compounded_sales_growth"))
        _, cpg = _parse_growth_cell(row.get("compounded_profit_growth"))
        _, cagr = _parse_growth_cell(row.get("stock_price_cagr"))
        _, roe = _parse_growth_cell(row.get("roe"))

        records.append({
            "company_id": company,
            "period": period,
            "compounded_sales_growth_pct": csg,
            "compounded_profit_growth_pct": cpg,
            "stock_price_cagr_pct": cagr,
            "roe_pct": roe,
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 4. NUMERIC COERCION
# ---------------------------------------------------------------------------

def to_numeric_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# 5. COMPUTED METRICS
# ---------------------------------------------------------------------------

def add_bs_computed(df: pd.DataFrame) -> pd.DataFrame:
    df = to_numeric_cols(df, ["equity_capital", "reserves", "borrowings",
                               "total_liabilities", "total_assets"])
    equity = df["equity_capital"].fillna(0) + df["reserves"].fillna(0)
    df["debt_to_equity"] = np.where(equity > 0, df["borrowings"] / equity, np.nan)
    df["equity_ratio"] = np.where(df["total_assets"] > 0, equity / df["total_assets"], np.nan)
    return df


def add_pl_computed(df: pd.DataFrame) -> pd.DataFrame:
    df = to_numeric_cols(df, ["sales", "expenses", "operating_profit",
                               "net_profit", "interest"])
    df["net_profit_margin_pct"] = np.where(
        df["sales"] > 0, (df["net_profit"] / df["sales"]) * 100, np.nan)
    df["expense_ratio_pct"] = np.where(
        df["sales"] > 0, (df["expenses"] / df["sales"]) * 100, np.nan)
    df["interest_coverage"] = np.where(
        df["interest"] > 0, df["operating_profit"] / df["interest"], np.nan)
    return df


def add_cf_computed(df: pd.DataFrame) -> pd.DataFrame:
    df = to_numeric_cols(df, ["operating_activity", "investing_activity",
                               "financing_activity", "net_cash_flow"])
    df["free_cash_flow"] = df["operating_activity"].fillna(0) + df["investing_activity"].fillna(0)
    return df


# ---------------------------------------------------------------------------
# 6. PROS / CONS  — explode into long format
# ---------------------------------------------------------------------------

def explode_proscons(df: pd.DataFrame) -> pd.DataFrame:
    """Convert wide (company_id, pros, cons) → long (company_id, is_pro, text)."""
    rows = []
    for _, row in df.iterrows():
        cid = row["company_id"]
        if pd.notna(row.get("pros")):
            rows.append({"company_id": cid, "is_pro": True, "text": str(row["pros"]).strip()})
        if pd.notna(row.get("cons")):
            rows.append({"company_id": cid, "is_pro": False, "text": str(row["cons"]).strip()})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("ETL Step 2 — Cleaning & transforming raw CSVs")
    print("=" * 60)

    # ── Companies ──────────────────────────────────────────────────────────
    co = pd.read_csv(RAW_DIR / "companies.csv", dtype=str)
    co["company_name"] = co["company_name"].str.strip()
    co["sector"] = co["symbol"].map(SECTOR_MAP).fillna("Other")
    co = to_numeric_cols(co, ["face_value", "book_value", "roce_percentage", "roe_percentage"])
    co.to_csv(CLEAN_DIR / "companies.csv", index=False)
    print(f"  companies          → {len(co)} rows")

    # Save sector mapping separately
    sector_df = co[["symbol", "sector"]].copy()
    sector_df.to_csv(CLEAN_DIR / "sector_mapping.csv", index=False)
    print(f"  sector_mapping     → {len(sector_df)} rows")

    # ── Analysis ────────────────────────────────────────────────────────────
    an = pd.read_csv(RAW_DIR / "analysis.csv", dtype=str)
    an_long = parse_analysis(an)
    an_long.to_csv(CLEAN_DIR / "analysis.csv", index=False)
    print(f"  analysis           → {len(an_long)} rows (long format)")

    # ── Balance Sheet ────────────────────────────────────────────────────────
    bs = pd.read_csv(RAW_DIR / "balancesheet.csv", dtype=str)
    bs = add_year_columns(bs)
    bs = add_bs_computed(bs)
    bs.to_csv(CLEAN_DIR / "balancesheet.csv", index=False)
    print(f"  balancesheet       → {len(bs)} rows")

    # ── Profit & Loss ────────────────────────────────────────────────────────
    pl = pd.read_csv(RAW_DIR / "profitandloss.csv", dtype=str)
    pl = add_year_columns(pl)
    pl = add_pl_computed(pl)
    pl.to_csv(CLEAN_DIR / "profitandloss.csv", index=False)
    print(f"  profitandloss      → {len(pl)} rows")

    # ── Cash Flow ────────────────────────────────────────────────────────────
    cf = pd.read_csv(RAW_DIR / "cashflow.csv", dtype=str)
    cf = add_year_columns(cf)
    cf = add_cf_computed(cf)
    cf.to_csv(CLEAN_DIR / "cashflow.csv", index=False)
    print(f"  cashflow           → {len(cf)} rows")

    # ── Pros & Cons ──────────────────────────────────────────────────────────
    pc = pd.read_csv(RAW_DIR / "prosandcons.csv", dtype=str)
    pc_long = explode_proscons(pc)
    pc_long.to_csv(CLEAN_DIR / "prosandcons.csv", index=False)
    print(f"  prosandcons        → {len(pc_long)} rows (long format)")

    # ── Documents ────────────────────────────────────────────────────────────
    docs = pd.read_csv(RAW_DIR / "documents.csv", dtype=str)
    docs["year"] = docs["year"].str.strip()
    docs.to_csv(CLEAN_DIR / "documents.csv", index=False)
    print(f"  documents          → {len(docs)} rows")

    # ── dim_year (derived from all year_label values across fact tables) ─────
    all_years = []
    for csv_name in ["balancesheet.csv", "profitandloss.csv", "cashflow.csv"]:
        df = pd.read_csv(CLEAN_DIR / csv_name, dtype=str)
        if "year_label" in df.columns:
            sub = df[["year_label", "fiscal_year", "sort_order", "is_ttm", "is_half_year"]].drop_duplicates()
            all_years.append(sub)

    dim_year = pd.concat(all_years).drop_duplicates(subset=["year_label"])
    dim_year["year_id"] = range(1, len(dim_year) + 1)
    dim_year = dim_year.sort_values("sort_order").reset_index(drop=True)
    dim_year.to_csv(CLEAN_DIR / "dim_year.csv", index=False)
    print(f"  dim_year           → {len(dim_year)} distinct year labels")

    print(f"\nClean CSVs saved to: {CLEAN_DIR.resolve()}")
    print("Done.\n")


if __name__ == "__main__":
    main()