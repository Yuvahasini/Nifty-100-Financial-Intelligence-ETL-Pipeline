# Nifty 100 Financial Intelligence — ETL Pipeline & BI Dashboards

Overview
--------

Nifty 100 Financial Intelligence is a full-stack data project that ingests
Screener/Excel exports for the Nifty 100 companies, runs a Python ETL pipeline
to normalise and load data into a PostgreSQL warehouse, computes ML-driven
health scores and analysis, exposes a Django REST API, and serves seven
interactive dashboards (static HTML) for exploration.

Key components
--------------
- ETL (Python + pandas): extracts raw Excel, cleans and transforms, and loads into PostgreSQL.
- ML scoring: computes multi-dimensional health scores and generates pros/cons.
- Backend: Django + Django REST Framework serving APIs used by the dashboards.
- Orchestration: Celery + Redis for scheduled ETL/analysis tasks.
- Dashboard layer: 7 static HTML dashboards (Chart.js) served from `dashboards/`.

Architecture (high level)
-------------------------

Excel source files → ETL (etl/*.py) → data/clean/*.csv → PostgreSQL (docker) → Django API → Dashboards

Repository structure (short)
---------------------------
The important folders and files:

- `data/` — source Excel, raw CSVs and cleaned CSVs used by the ETL.
- `etl/` — extraction, transform and load scripts plus scoring and analysis.
- `notebooks/` — exploratory and analysis notebooks used during development.
- `dashboards/` — 7 static dashboard HTML pages and shared CSS.
- `intelligence/` — Django app with models, serializers, views and Celery tasks.
- `nifty100/` — Django project settings and WSGI/ASGI entry points.
- `docker-compose.yml` & `Dockerfile` — local development containers (Postgres, Redis, web, celery).

Data flow summary
-----------------
1. Place Excel exports in `data/source/`.
2. Run `etl/01_extract_from_excel.py` → writes `data/raw/` CSVs.
3. Run `etl/02_clean_and_transform.py` → writes `data/clean/` CSVs.
4. Run `etl/03_load_to_warehouse.py` → loads into PostgreSQL.
5. Run `etl/05_compute_analysis.py` and `etl/04_ml_scores.py` to compute analytics and ML scores.

## Quick phased runbook (PHASE 1 → PHASE 10)

Run all commands from PowerShell at the project root:

```powershell
cd "C:\Users\dudde\OneDrive\Documents\Nifty-100-Financial-Intelligence-ETL-Pipeline"
```

PHASE 1 — Start Docker
```bash
docker-compose up -d --build
docker-compose ps
```

PHASE 2 — Install packages
```bash
pip install -r requirements.txt
pip install jupyter statsmodels scikit-learn scipy yfinance
```

PHASE 3 — Run ETL pipeline (exact order)
```bash
python etl/01_extract_from_excel.py
python etl/02_clean_and_transform.py
python etl/03_load_to_warehouse.py
python etl/05_compute_analysis.py
python etl/04_ml_scores.py
```

PHASE 4 — Fix `dim_year` column (run once)
```bash
docker exec -it nifty-pg psql -U postgres -d nifty100 -c "ALTER TABLE dim_year ADD COLUMN IF NOT EXISTS year_date DATE;"
# Then re-run the warehouse load
python etl/03_load_to_warehouse.py
```

PHASE 5 — Run notebooks (execute selected notebooks)
```bash
jupyter nbconvert --to notebook --execute notebooks/03_anomaly_detection.ipynb
jupyter nbconvert --to notebook --execute notebooks/05_peer_comparison_engine.ipynb
```

PHASE 6 — Load anomaly + peers into DB
```bash
python etl/06_load_anomaly_peers.py
```

PHASE 7 — Verify database
```bash
docker exec -it nifty-pg psql -U postgres -d nifty100 -c "SELECT COUNT(*) FROM dim_company;"
docker exec -it nifty-pg psql -U postgres -d nifty100 -c "SELECT COUNT(*) FROM fact_profit_loss;"
docker exec -it nifty-pg psql -U postgres -d nifty100 -c "SELECT COUNT(*) FROM fact_ml_scores;"
docker exec -it nifty-pg psql -U postgres -d nifty100 -c "SELECT health_label, COUNT(*) FROM fact_ml_scores GROUP BY health_label;"
docker exec -it nifty-pg psql -U postgres -d nifty100 -c "SELECT COUNT(*) FROM fact_anomaly_flags;"
docker exec -it nifty-pg psql -U postgres -d nifty100 -c "SELECT COUNT(*) FROM fact_peer_mapping;"
```

PHASE 8 — Test API
```bash
curl http://localhost:8000/api/companies/
curl http://localhost:8000/api/leaderboard/
curl http://localhost:8000/api/companies/TCS/full/
curl http://localhost:8000/api/companies/TCS/peers/
# Or open interactive docs in the browser:
# http://localhost:8000/api/docs/
```

PHASE 9 — Launch dashboards (website)
```bash
cd dashboards
python -m http.server 5500
# Open these URLs in your browser:
# http://localhost:5500/01_executive_overview.html
# http://localhost:5500/02_company_deep_dive.html
# http://localhost:5500/03_sector_comparison.html
# http://localhost:5500/04_health_scorecard.html
# http://localhost:5500/05_growth_analytics.html
# http://localhost:5500/06_debt_leverage.html
# http://localhost:5500/07_dividend_returns.html
```

PHASE 10 — Next time you restart (fast path)
```bash
docker-compose up -d
cd dashboards && python -m http.server 5500
```

## Notes & troubleshooting highlights
- PostgreSQL is exposed on `localhost:5433` by the Docker Compose setup.
- If dashboards show "Cannot reach API", ensure the Django server is running on port `8000`.
- If stock CAGR data is missing, run `pip install yfinance` and re-run `python etl/05_compute_analysis.py` (may take 3–5 minutes).
- Power BI `.pbix` files are not included in this repo; dashboards are static HTML that call the API.

If you want, I can add a small `powerbi/BUILD_PBIX.md` checklist for the 7 dashboards or create a one-shot script that runs PHASE 1 → PHASE 9 automatically.

Project goals
-------------
- Provide an opinionated, reproducible ETL pipeline for the Nifty 100 universe.
- Produce clean, analysis-ready datasets and ML-driven health scores for each company.
- Expose an easy-to-use REST API for dashboards and downstream consumers.
- Ship lightweight, static dashboards that visualise scores, trends and peer comparisons.

Data model (high-level)
-----------------------
- `dim_company` — company master (symbol, name, sector, key ratios)
- `dim_year` — fiscal years and TTM mapping
- `dim_sector` — sector master mapping used by dashboards
- `fact_profit_loss` — annual P&L rows per company
- `fact_balance_sheet` — annual balance sheet rows per company
- `fact_cash_flow` — annual cash flow rows per company
- `fact_analysis` — computed CAGR and stock returns per period
- `fact_ml_scores` — computed 6-dimension ML scores + label
- `fact_pros_cons` — ML-generated insight rows (pro / con)

How it works (developer view)
----------------------------
1. Drop Screener/Excel exports into `data/source/` (or use `data/source/scriptticker.sql`).
2. `01_extract_from_excel.py` reads Excel files and outputs raw CSVs in `data/raw/`.
3. `02_clean_and_transform.py` normalises column names, fixes types and writes `data/clean/` CSVs.
4. `03_load_to_warehouse.py` creates dimensions/facts and loads CSVs into the `nifty100` database.
5. `05_compute_analysis.py` fetches stock prices (`yfinance`) and computes CAGR/ROE; `04_ml_scores.py` computes health scores using those features.

Extending the project
---------------------
- To add a new dashboard, create an HTML page in `dashboards/` that hits the API endpoints under `/api/`.
- To add new ETL logic, add a script to `etl/` and document its place in the PHASE runbook.
- To add scheduled pipelines, register Celery beat tasks in `nifty100/celery.py` and `intelligence/tasks.py`.

Testing and verification
------------------------
- Use the PHASE 7 verification queries to validate row counts in the DB.
- Use `curl` to test API endpoints (examples in PHASE 8).
- Notebooks in `notebooks/` contain exploratory checks and can be executed with `nbconvert`.

Contributing
------------
- Fork the repo and open a pull request with your changes.
- Keep commits small and focused; use descriptive commit messages.

Contact / Maintainers
---------------------
Maintainer: Yuvahasini — open an issue or PR on the GitHub repo.

---

If you'd like, I can also:
- Add example API responses to the README.
- Add a CONTRIBUTING.md and developer quickstart script.
- Generate a small diagram (Mermaid) for the architecture section.

