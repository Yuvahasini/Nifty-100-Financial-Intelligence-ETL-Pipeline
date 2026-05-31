"""
ETL Script 1 — Extract raw data from a SQL dump file.
Reads INSERT INTO statements from a MariaDB/MySQL dump and writes raw CSVs to data/raw/.
Run: python etl/01_extract_from_mysql.py
"""

import os
import re
from pathlib import Path
import pandas as pd

SRC_DIR = Path("data/source")
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

SQL_FILE = Path(os.getenv("SQL_DUMP_PATH", SRC_DIR / "scriptticker.sql"))

TABLES = {
    "companies": "companies.csv",
    "analysis": "analysis.csv",
    "balancesheet": "balancesheet.csv",
    "profitandloss": "profitandloss.csv",
    "cashflow": "cashflow.csv",
    "prosandcons": "prosandcons.csv",
    "documents": "documents.csv",
}

RE_INSERT = re.compile(
    r"INSERT\s+INTO\s+`?(?P<table>\w+)`?\s*(?:\((?P<cols>[^)]+)\))?\s*VALUES\s*(?P<vals>.+);",
    re.IGNORECASE | re.DOTALL,
)


def _split_statements(sql_text: str) -> list[str]:
    statements = []
    i = 0
    while True:
        idx = sql_text.find("INSERT INTO", i)
        if idx == -1:
            break
        j = idx
        in_string = False
        quote_char = None
        escape = False
        while j < len(sql_text):
            ch = sql_text[j]
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == quote_char:
                    if j + 1 < len(sql_text) and sql_text[j + 1] == quote_char:
                        j += 1
                    else:
                        in_string = False
            else:
                if ch in ("'", '"'):
                    in_string = True
                    quote_char = ch
                elif ch == ";":
                    statements.append(sql_text[idx:j + 1])
                    i = j + 1
                    break
            j += 1
        else:
            break
    return statements


def _parse_value(text: str, i: int) -> tuple[object, int]:
    i = _skip_whitespace(text, i)
    if i >= len(text):
        return None, i

    ch = text[i]
    if ch == "'":
        i += 1
        value = []
        while i < len(text):
            if text[i] == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
                    value.append("'")
                    i += 2
                    continue
                i += 1
                break
            if text[i] == "\\" and i + 1 < len(text):
                i += 1
                value.append(text[i])
                i += 1
                continue
            value.append(text[i])
            i += 1
        return "".join(value), i

    match = re.match(r"NULL\b", text[i:], re.IGNORECASE)
    if match:
        return None, i + match.end()

    token = []
    while i < len(text) and text[i] not in ",)\n\r\t ":
        token.append(text[i])
        i += 1
    token_str = "".join(token)
    if token_str == "":
        return None, i
    if token_str.upper() in ("TRUE", "FALSE"):
        return token_str.upper() == "TRUE", i
    try:
        if "." in token_str or "e" in token_str.lower():
            return float(token_str), i
        return int(token_str), i
    except ValueError:
        return token_str, i


def _skip_whitespace(text: str, i: int) -> int:
    while i < len(text) and text[i].isspace():
        i += 1
    return i


def _parse_row(text: str, i: int) -> tuple[list[object], int]:
    row = []
    i = _skip_whitespace(text, i)
    if i >= len(text) or text[i] != "(":
        raise ValueError("Expected row start '('")
    i += 1
    while i < len(text):
        i = _skip_whitespace(text, i)
        if i < len(text) and text[i] == ")":
            return row, i + 1
        value, i = _parse_value(text, i)
        row.append(value)
        i = _skip_whitespace(text, i)
        if i < len(text) and text[i] == ",":
            i += 1
            continue
        if i < len(text) and text[i] == ")":
            return row, i + 1
    raise ValueError("Unterminated row")


def _parse_values(text: str) -> list[list[object]]:
    values = []
    i = 0
    while i < len(text):
        i = _skip_whitespace(text, i)
        if i >= len(text):
            break
        if text[i] == ",":
            i += 1
            continue
        if text[i] == "(":
            row, i = _parse_row(text, i)
            values.append(row)
            continue
        i += 1
    return values


def parse_insert_statement(stmt: str) -> tuple[str, list[str], list[list[object]]]:
    match = RE_INSERT.search(stmt)
    if not match:
        raise ValueError("Could not parse INSERT statement")
    table = match.group("table")
    cols = match.group("cols")
    vals = match.group("vals").rstrip(";\n \r")
    if cols:
        cols = [c.strip().strip('`"') for c in cols.split(",")]
    values = _parse_values(vals)
    return table, cols, values


def main():
    print("=" * 60)
    print("ETL Step 1 — Extracting raw data from SQL dump")
    print("=" * 60)

    if not SQL_FILE.exists():
        print(f"SQL dump not found: {SQL_FILE}")
        print("Place your dump file in data/source/ or set SQL_DUMP_PATH.")
        return

    raw_sql = SQL_FILE.read_text(encoding="utf-8", errors="replace")
    statements = _split_statements(raw_sql)
    tables = {}

    for stmt in statements:
        try:
            table, cols, values = parse_insert_statement(stmt)
        except Exception as exc:
            print(f"  [WARN] Skipping statement: {exc}")
            continue
        if table not in TABLES:
            continue
        if not values:
            continue
        if cols is None:
            max_cols = max(len(row) for row in values)
            cols = [f"col{i+1}" for i in range(max_cols)]
        tables.setdefault(table, []).extend(values)

    for table, rows in tables.items():
        cols = [f"col{i+1}" for i in range(len(rows[0]))]
        df = pd.DataFrame(rows, columns=cols)
        out = RAW_DIR / TABLES[table]
        df.to_csv(out, index=False)
        print(f"  {out.name:<25} → {len(df):>5} rows | cols: {list(df.columns)}")

    if not tables:
        print("No supported INSERT statements found.")
    else:
        print("\nRaw CSVs saved to:", RAW_DIR.resolve())
        print("Done.\n")


if __name__ == "__main__":
    main()
