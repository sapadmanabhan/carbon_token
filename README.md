# Carbon Trust Engine

A pollution-budgeted API gateway that meters AI workloads against live US grid carbon data from EIA.

## How it works
- Every state has a daily CO2 budget derived from its grid's 24h avg carbon intensity
- Users get a fair share (max-min fairness) via a token system
- Per-request cost scales with *live* grid conditions — dirty hour = higher cost
- Blocked requests get real-time migration suggestions

## Data source
US EIA API v2: `electricity/rto/fuel-type-data` — hourly fuel-mix by balancing authority

## Running
Backend: `python server.py` (port 5050)
UI: `cd carbon_ui && npm run dev` (port 5173)
Tests: `python -m pytest` (87 tests pass)

## Deployment
AWS App Runner — see AWS_DEPLOY.md
