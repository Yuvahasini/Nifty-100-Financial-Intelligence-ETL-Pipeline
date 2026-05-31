"""
ETL Script 0 — Generate a MariaDB/MySQL SQL dump from Excel source files.
Writes INSERT statements for all source tables to `data/source/scriptticker.sql`.
Run: python etl/00_generate_sql_dump.py
"""

import re
from pathlib import Path
import pandas as pd

SRC_DIR = Path("data/source")
DUMP_FILE = SRC_DIR / "scriptticker.sql"

TABLE_FILES = {
    "companies": "companies.xlsx",
    "analysis": "analysis.xlsx",
    "balancesheet": "balancesheet.xlsx",
    "profitandloss": "profitandloss.xlsx",
    "cashflow": "cashflow.xlsx",
    "prosandcons": "prosandcons.xlsx",
    "documents": "documents.xlsx",
}

COMPANIES_COLS = [
    "symbol", "company_logo", "company_name", "chart_link", "about_company",
    "website", "nse_profile", "bse_profile", "face_value", "book_value",
    "roce_percentage", "roe_percentage",
]

DEFAULT_COLS = {
    "analysis": ["company_id", "compounded_sales_growth", "compounded_profit_growth", "stock_price_cagr", "roe"],
    "balancesheet": ["company_id", "year", "equity_capital", "reserves", "borrowings", "other_liabilities", "total_liabilities", "fixed_assets", "cwip", "investments", "other_asset", "total_assets"],
    "profitandloss": ["company_id", "year", "sales", "expenses", "operating_profit", "opm_percentage", "other_income", "interest", "depreciation", "profit_before_tax", "tax_percentage", "net_profit", "eps", "dividend_payout"],
    "cashflow": ["company_id", "year", "operating_activity", "investing_activity", "financing_activity", "net_cash_flow"],
    "prosandcons": ["company_id", "pros", "cons"],
    "documents": ["company_id", "year", "annual_report_url"],
}


def sql_literal(value):
    if pd.isna(value):
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    s = str(value)
    s = s.replace("\\", "\\\\")
    s = s.replace("'", "''")
    return f"'{s}'"


def normalise_companies(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    if df.shape[1] >= len(COMPANIES_COLS):
        df = df.iloc[:, : len(COMPANIES_COLS)]
        df.columns = COMPANIES_COLS
    else:
        df.columns = df.columns.astype(str)
        df = df.rename(columns={df.columns[0]: "symbol"})
    df["company_name"] = df["company_name"].astype(str).str.strip()
    return df[COMPANIES_COLS]


def normalise_default(table: str, df: pd.DataFrame) -> pd.DataFrame:
    cols = DEFAULT_COLS[table]
    df = df.reset_index(drop=True)
    if df.shape[1] >= len(cols):
        df = df.iloc[:, : len(cols)]
        df.columns = cols
    else:
        df.columns = df.columns.astype(str)
    return df[cols]


def write_insert(table: str, columns: list[str], rows: list[list], file_handle):
    if not rows:
        return
    col_list = ", ".join(f"`{c}`" for c in columns)
    for row in rows:
        values = ", ".join(sql_literal(v) for v in row)
        file_handle.write(f"INSERT INTO `{table}` ({col_list}) VALUES ({values});\n")


def load_table(table: str, path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    if table == "companies":
        df = pd.read_excel(path, header=1, skiprows=[0], dtype=str)
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = COMPANIES_COLS
        df["symbol"] = df["symbol"].astype(str).str.strip()
        return df
    df = pd.read_excel(path, header=1, skiprows=[0], dtype=str)
    return normalise_default(table, df)


def main():
    print("=" * 60)
    print("ETL Step 0 — Generating SQL dump from Excel source files")
    print("=" * 60)
    missing = []
    with DUMP_FILE.open("w", encoding="utf-8") as out:
        for table, filename in TABLE_FILES.items():
            path = SRC_DIR / filename
            if not path.exists():
                missing.append(filename)
                continue
            df = load_table(table, path)
            write_insert(table, df.columns.tolist(), df.values.tolist(), out)
            print(f"  Wrote {len(df)} rows for {table}")

    if missing:
        print("\nMissing files:")
        for name in missing:
            print(f"  - {name}")
        print("SQL dump generation incomplete.")
        return

    print(f"\nSQL dump written to {DUMP_FILE}")
    print("Done.")


if __name__ == "__main__":
    main()
