# DashFolio

DashFolio is a hybrid Flask and TypeScript application that helps long-term investors
monitor portfolio health, understand risk exposure, and document cash activity in a
single, data-rich workspace. The backend orchestrates market-data ingestion, risk
analytics, and onboarding workflows, while the front-end delivers an interactive
experience for exploring holdings, allocations, and simulated risk scenarios.

## Key capabilities

- **Holistic portfolio overview** – track equity, ETF, and cash positions with live
  pricing updates, target allocations, and customizable benchmarks.
- **Risk and scenario analysis** – compute exponentially weighted moving average
  (EWMA) returns, trailing-stop hit probabilities, and Value-at-Risk (VaR) estimates
  so you can anticipate drawdowns before they happen.
- **Cash-flow awareness** – log deposits, withdrawals, dividends, and interest
  adjustments, and apply them to derived holdings in a normalized ledger.
- **Allocation sandboxing** – model hypothetical assets directly in the allocation
  planner and explore rebalancing actions without impacting live holdings.
- **Audit-friendly data trail** – persist every snapshot, calculation, and user event
  in an embedded SQLite database for reproducibility and historical comparisons.
- **Single-user secure onboarding** – password-protected authentication, guided
  funding steps, and CSV transaction ingestion for streamlined setup.

## Architecture at a glance

| Layer | Responsibilities | Representative modules |
| --- | --- | --- |
| **Presentation** | Flask templates render the onboarding, holdings, allocations, and risk dashboards; static CSS styles cards, charts, and modal dialogs. | `templates/`, `static/css/app.css` |
| **Application services** | Authentication, configuration, formatting, and portfolio state helpers wrap Flask routes with reusable business logic. | `services/auth.py`, `services/configuration.py`, `services/portfolio.py` |
| **Calculations engine** | Loads transactions, prices, and benchmarks, then runs analytics such as EWMA statistics, trailing-stop simulations, and snapshot caching. | `Calculations/` package |
| **Data layer** | An embedded SQLite database stores price history, derived holdings, risk simulations, and cached snapshots for fast recomputation. Persistent files (database, `config.json`, `portfolio.json`) live under `/mnt/config/dashfolio` by default so they can be mounted from the host when running in Docker. | `Calculations/storage.py`, `/mnt/config/dashfolio/dashfolio.db` |

## Data sources

DashFolio consumes data from multiple sources:

- **Portfolio configuration** – base holdings, target allocations, and security metadata
  live in `portfolio.json` (stored under `/mnt/config/dashfolio/` by default) and are synchronized with transaction-derived holdings.
- **User preferences** – `config.json` (also persisted in `/mnt/config/dashfolio/`) defines the analysis window, stop-loss ranges,
  EWMA spans, benchmark tickers, and UI auto-refresh cadence.
- **Market data** – live quotes and historical candles are pulled from Yahoo Finance via
  [`yfinance`](https://pypi.org/project/yfinance/), then normalized and cached locally.
- **User transactions** – CSV uploads and manual adjustments are validated, normalized,
  and written into SQLite tables to produce reproducible holdings snapshots.

## Getting started

1. **Create a Python environment** (3.11+ recommended) and install the application
   dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
   _If a `requirements.txt` file is not yet available, install Flask, pandas,
   yfinance, numpy, and SQLAlchemy equivalents manually._
2. **Provision the database** by running the calculation engine once:
   ```bash
   python main.py
   ```
   This fetches the initial market data, updates holdings prices, and seeds the
   risk-analysis tables.
3. **Launch the Flask application** and explore the dashboard:
   ```bash
   export FLASK_APP=app.py
   flask run --debug
   ```
   Visit `http://127.0.0.1:5000` to complete onboarding, upload transactions, and
   review portfolio analytics.
4. **Run the TypeScript unit tests** that cover the client-side allocation logic:
   ```bash
   npm install
   npm test
   ```

### Running with Docker

1. Build the container image and ensure the host directory that will persist the
   configuration, portfolio JSON, and SQLite database exists:
   ```bash
   docker compose build
   mkdir -p ./dashfolio-data
   ```
2. Start the application with the data directory mounted into the container at
   `/mnt/config/dashfolio` (the location expected by the codebase):
   ```bash
   docker compose up
   ```
   You can alternatively run the image directly:
   ```bash
   docker run \
     -p 5000:5000 \
     -e FLASK_APP=app.py \
     -e DASHFOLIO_DATA_DIR=/mnt/config/dashfolio \
     -v /absolute/host/path:/mnt/config/dashfolio \
     dashfolio:latest
   ```
3. Visit `http://127.0.0.1:5000` and proceed through onboarding. Files written to
   `/mnt/config/dashfolio` inside the container will now persist on the host.

## Dashboard walkthrough

1. **Onboarding & authentication** – register the primary user, define funding
   balances, and upload CSV transaction history. On success, onboarding updates the
   SQLite-backed portfolio tables and unlocks the analytics experience.
2. **Portfolio analysis** – inspect each holding’s live price, target vs. actual
   allocation, and recent performance metrics. Drill into holdings for transaction
   history, price series, and recommended rebalancing moves.
3. **Risk analysis** – experiment with trailing-stop ranges and confidence levels to
   understand the probability of stop activation and the projected loss magnitude at
   different VaR thresholds.
4. **Transactions** – review the normalized transaction ledger, including deposits,
   withdrawals, dividends, and interest adjustments that reconcile to the cash balance.
5. **Settings** – tune the analysis horizon, EWMA span, benchmark ticker, and session
   duration. Changes propagate immediately to the calculation engine and cached
   snapshots.

## Contributing

Pull requests are welcome! Please accompany changes with unit tests (`npm test` for the
TypeScript utilities and any relevant Python tests), and update documentation when you
extend the data model or dashboard workflows.

## License

This project is distributed under the MIT License. See `LICENSE` (if present) for the
full text.
