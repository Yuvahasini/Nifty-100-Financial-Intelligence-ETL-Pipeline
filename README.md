# Nifty 100 Financial Intelligence — ETL Pipeline & BI Dashboards

A full-stack financial intelligence platform for the Nifty 100 stock universe.
This repository includes an ETL pipeline (Excel → PostgreSQL), ML health scoring,
a Django REST API, and 7 static dashboards served from the `dashboards/` folder.

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
