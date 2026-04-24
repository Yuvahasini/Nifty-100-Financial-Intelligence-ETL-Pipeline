"""
ETL Script 1 — Extract raw data from Excel source files.
Reads all 7 xlsx files, normalises headers, and saves clean CSVs to data/raw/.
Run: python etl/01_extract_from_excel.py
"""

import pandas as pd
from pathlib import Path

SRC_DIR = Path("data/source")   # place the 7 xlsx files here
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def load_table(filename: str, col_names: list[str]) -> pd.DataFrame:
    """
    Each xlsx has a branding row (row 0) + header row (row 1) + data rows.
    We skip row 0, use row 1 as header, then rename to snake_case col_names.
    """
    path = SRC_DIR / filename
    df = pd.read_excel(path, header=1, skiprows=[0], dtype=str)
    df = df.iloc[1:].reset_index(drop=True)          # drop the 'header-copy' row
    df.columns = col_names
    df.replace({"NULL": None, "Null": None, "null": None, "nan": None, "": None}, inplace=True)
    df = df.dropna(how="all")
    return df


# ---------------------------------------------------------------------------
# Table definitions  (filename, output_csv, columns)
# ---------------------------------------------------------------------------

TABLES = [
    (
        "companies.xlsx",
        "companies.csv",
        ["id", "company_logo", "company_name", "chart_link", "about_company",
         "website", "nse_profile", "bse_profile", "face_value", "book_value",
         "roce_percentage", "roe_percentage"],
    ),
    (
        "analysis.xlsx",
        "analysis.csv",
        ["id", "company_id", "compounded_sales_growth", "compounded_profit_growth",
         "stock_price_cagr", "roe"],
    ),
    (
        "balancesheet.xlsx",
        "balancesheet.csv",
        ["id", "company_id", "year", "equity_capital", "reserves", "borrowings",
         "other_liabilities", "total_liabilities", "fixed_assets", "cwip",
         "investments", "other_asset", "total_assets"],
    ),
    (
        "profitandloss.xlsx",
        "profitandloss.csv",
        ["id", "company_id", "year", "sales", "expenses", "operating_profit",
         "opm_percentage", "other_income", "interest", "depreciation",
         "profit_before_tax", "tax_percentage", "net_profit", "eps",
         "dividend_payout"],
    ),
    (
        "cashflow.xlsx",
        "cashflow.csv",
        ["id", "company_id", "year", "operating_activity", "investing_activity",
         "financing_activity", "net_cash_flow"],
    ),
    (
        "prosandcons.xlsx",
        "prosandcons.csv",
        ["id", "company_id", "pros", "cons"],
    ),
    (
        "documents.xlsx",
        "documents.csv",
        ["id", "company_id", "year", "annual_report_url"],
    ),
]

# ---------------------------------------------------------------------------
# Special handler: companies table uses row 0 as branding, col 0 as id
# The companies file has the company symbol/id in the first data column
# but the branding header calls it differently — we handle col mapping here.
# ---------------------------------------------------------------------------

def extract_companies() -> pd.DataFrame:
    path = SRC_DIR / "companies.xlsx"
    df = pd.read_excel(path, header=1, skiprows=[0], dtype=str)
    df = df.iloc[1:].reset_index(drop=True)
    df.columns = ["id", "company_logo", "company_name", "chart_link", "about_company",
                  "website", "nse_profile", "bse_profile", "face_value", "book_value",
                  "roce_percentage", "roe_percentage"]
    # The 'id' column actually contains the NSE symbol (e.g. 'ABB', 'TCS')
    df.rename(columns={"id": "symbol"}, inplace=True)
    df.replace({"NULL": None, "Null": None, "nan": None, "": None}, inplace=True)
    df = df.dropna(subset=["symbol"])
    df["company_name"] = df["company_name"].str.strip()
    return df


# ---------------------------------------------------------------------------
# Run extraction
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("ETL Step 1 — Extracting raw data from Excel files")
    print("=" * 60)

    # Companies (special handler)
    companies = extract_companies()
    out = RAW_DIR / "companies.csv"
    companies.to_csv(out, index=False)
    print(f"  companies.csv        → {len(companies):>5} rows | cols: {companies.columns.tolist()}")

    # All other tables
    for filename, csv_name, cols in TABLES[1:]:   # skip companies — already done
        try:
            df = load_table(filename, cols)
            out = RAW_DIR / csv_name
            df.to_csv(out, index=False)
            print(f"  {csv_name:<25} → {len(df):>5} rows | cols: {cols}")
        except FileNotFoundError:
            print(f"  [SKIP] {filename} not found in {SRC_DIR}")

    print("\nRaw CSVs saved to:", RAW_DIR.resolve())
    print("Done.\n")


if __name__ == "__main__":
    main()