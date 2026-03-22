# TickerFlow / Phase 1 stress tests

Isolated load tests to pick **`BACKTEST_DATE_CONCURRENCY`** and **`TICKERFLOW_API_CONCURRENCY`** without editing code.

## What gets measured

- **`benchmark_fetch.py`** — Same **HTTP pattern** as Phase 1 (per date: instrument batches → tick batches). **No** `analyze_stock` CPU work and **no** market-context/index calls, so times are **lower-bound I/O** only. Full Phase 1 will be slower and use more RAM.
- **`sweep.py`** — Runs the benchmark across a grid of `(parallel_dates, api_concurrency)` in **separate processes** so each run gets a fresh API semaphore.

## Requirements

- Repo `.env` with working `TICKERFLOW_URL` / `TICKERFLOW_API_KEY` (same as backtest).
- Close or throttle other heavy jobs if you want numbers specific to “quiet” machine; with other services running, treat results as **your** capacity snapshot.

## Quick sweep (~10–25 min depending on API)

From **repository root**:

```bash
source .venv/bin/activate   # if you use venv
python stress_tests/sweep.py --quick
```

## Full grid (longer, tighter search)

```bash
python stress_tests/sweep.py
```

## Custom grid

```bash
python stress_tests/sweep.py --dc-list 6,8,10 --ac-list 48,72,96 --stock-limit 120
```

- **`--stock-limit`**: Fewer stocks → faster runs; use **0** for all stocks (slow). Default is 80 (`--quick`) or 160 (full sweep).
- **`--dates-offset`**: Skip the first N trading days in the config so repeated sweeps don’t always hit the same cached data on the server.

## Output

- CSV under `stress_tests/results/sweep_<timestamp>.csv`
- Console: **fastest error-free** pair and suggested `.env` lines

## Interpreting “optimal”

1. Pick the **fastest row with `exit_code=0` and `total_errors=0`**.
2. If the best time plateaus, choose the **lowest concurrency** among ties to reduce load on TickerFlow and on **other services** on the same host.
3. If you see failures at high `api_concurrency`, cap there and/or scale TickerFlow/Postgres instead of pushing the client further.

## Enterprise

- Run sweeps in **CI or a dedicated runner** with production-like network path; laptop Wi‑Fi vs wired changes numbers.
- For production backtests, prefer **secrets from vault** over `.env` on shared machines.
- **Re-run** the sweep after TickerFlow version/ingress/DB changes.
