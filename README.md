# CGR Analysis & Backtest

OI analysis, ranking, and Phase 1–3 backtest for options trading strategies.
Reads market data via **TickerFlow API**; analysis module can optionally query TimescaleDB directly.

## Setup

```bash
git clone <your-repo-url> analysis_backtest
cd analysis_backtest

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env with your actual credentials
```

## Running

All commands run from the repo root (`analysis_backtest/`):

```bash
# Phase 1 – OI analysis backtest
python -m src.backtest.2025.run_backtest
python -m src.backtest.2026.run_backtest_2026

# Phase 2 – Stock simulation (pass results_dir from Phase 1)
python -m src.backtest.2025.run_phase2 src/backtest/results/<run_dir>
python -m src.backtest.2026.run_phase2_2026 src/backtest/results/<run_dir>

# Phase 3 – Grade A, 10L capital F&O sim
python -m src.backtest.2025.run_gradeA_10l src/backtest/results/<run_dir>
python -m src.backtest.2026.run_gradeA_10l_2026 src/backtest/results/<run_dir>

# Phase 3 – Full F&O options simulation
python -m src.backtest.utils.run_phase3_fno src/backtest/results/<run_dir>

# Full pipeline shell scripts
bash src/backtest/2025/run_full_backtest.sh
bash src/backtest/2026/run_2026q1_backtest.sh
```

## Config

- **`.env`** at repo root: `TICKERFLOW_API_URL`, DB credentials (`DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME`).
- **`db_config.py`**: Reads `.env` and exposes `DATABASE_URL` / `DB_CONNECTION_STRING` for the analysis module.
- Scripts auto-discover `.env` from the repo root via `Path(__file__).resolve().parents[3]`.

## Project structure

```
analysis_backtest/
├── .env.example
├── .gitignore
├── db_config.py
├── requirements.txt
├── README.md
└── src/
    ├── __init__.py
    ├── analysis/          # OI analysis pipeline (MVC)
    │   ├── config/
    │   ├── controllers/
    │   ├── models/
    │   ├── services/
    │   └── views/
    ├── backtest/           # Backtest runners
    │   ├── __init__.py
    │   ├── 2025/           # Full-year 2025 backtest
    │   ├── 2026/           # Q1 2026 backtest
    │   ├── utils/          # Shared: tickerflow_client, run_phase3_fno
    │   ├── archive/        # Older/experimental runners
    │   └── results/        # Output dir (gitignored)
    └── setup/
        └── setup_jump_server.sh
```
