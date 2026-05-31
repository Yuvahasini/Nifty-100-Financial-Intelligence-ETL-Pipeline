# Power BI Reference

This folder contains the DAX measures and model notes used by the Power BI dashboards.

## What is included
- `dax_measures.md`: reusable DAX measure definitions and relationship guidance.

## What is not included
- Power BI Desktop `.pbix` files are not stored in this repository.

## How to use
1. Open Power BI Desktop.
2. Connect to PostgreSQL at `localhost:5433`, database `nifty100` (or your configured warehouse).
3. Import the required dim_* and fact_* tables.
4. Use the DAX formulas in `dax_measures.md` to create a dedicated Measures table.
