# Backtest Run Examples

Sample commands for running the backtest pipeline. Run from the project root with the virtual environment activated (`source .venv/bin/activate`) or use `.venv/bin/python` explicitly.

---

## Full year 2025 (all 4 phases)

```bash
# Full pipeline — Phase 1 + 2 + 3a + 3b (~1–1.5 hrs with optimizations)
python -m src.backtest.cli --config full_2025.json
```

---

## Q1 2026 (all 4 phases)

```bash
# 46 trading days from Jan–Mar 2026
python -m src.backtest.cli --config q1_2026.json
```

---

## Single date — quick smoke test

```bash
# Any date from the 2025 calendar (overrides the config's trading_dates)
python -m src.backtest.cli --config full_2025.json --date 2025-06-15

# Using the single_day preset
python -m src.backtest.cli --config single_day.json --date 2025-02-28

# A Q1 2026 date
python -m src.backtest.cli --config q1_2026.json --date 2026-02-10
```

---

## Run individual phases

```bash
# Only Phase 1 (OI analysis + ranking) — useful to verify data quality first
python -m src.backtest.cli --config full_2025.json --phase 1

# Only Phase 2 (OHLC trade simulation) — needs Phase 1 results
python -m src.backtest.cli --config full_2025.json --phase 2

# Only Phase 3a (F&O flat per-trade validation) — needs Phase 2 results
python -m src.backtest.cli --config full_2025.json --phase 3a

# Only Phase 3b (Grade A+B capital compounding sim) — needs Phase 2 results
python -m src.backtest.cli --config full_2025.json --phase 3b
```

---

## Run multiple phases

```bash
# Run Phase 1 + 2 only (skip F&O/capital sim)
python -m src.backtest.cli --config full_2025.json --phase 1,2

# Run both Phase 3 variants together (needs existing Phase 2 results)
python -m src.backtest.cli --config full_2025.json --phase 3a,3b

# Run Phase 2 + 3a + 3b (skip re-running Phase 1)
python -m src.backtest.cli --config q1_2026.json --phase 2,3a,3b
```

---

## Resume from a previous run

```bash
# Re-run Phase 3a/3b on an existing results directory (skip Phase 1+2)
python -m src.backtest.cli --config full_2025.json --phase 3a,3b \
  --results-dir src/backtest/results/full_2025_pct_sl_20260310_061735

# Re-run only Phase 2 from existing Phase 1 output
python -m src.backtest.cli --config full_2025.json --phase 2 \
  --results-dir src/backtest/results/full_2025_pct_sl_20260310_061735

# Tweak Phase 3b settings and re-simulate capital without redoing everything
python -m src.backtest.cli --config full_2025.json --phase 3b \
  --results-dir src/backtest/results/full_2025_pct_sl_20260310_061735
```

---

## Single date + single phase (fastest test)

```bash
# Phase 1 only for one date — ~30 seconds
python -m src.backtest.cli --config full_2025.json --date 2025-03-10 --phase 1

# Full pipeline for one date — ~2–3 minutes
python -m src.backtest.cli --config full_2025.json --date 2025-07-22
```

---

## Custom config file (absolute path)

```bash
# Point to any JSON config file on disk
python -m src.backtest.cli --config /path/to/my_custom_backtest.json

# Custom config + single date override
python -m src.backtest.cli --config /path/to/my_custom_backtest.json --date 2025-11-15
```

---

## Background runs (nohup)

See [scripts/README.md](../scripts/README.md) and the `scripts/` directory for `run_backtest_2025_nohup.sh` and `run_backtest_q1_2026_nohup.sh`.

---

## Phase 1 performance tuning (environment)

Phase 1 throughput is limited by **network + TickerFlow/DB** and by **how many dates** run at once. Tune via `.env` or the shell **before** starting the pipeline:

| Variable | Default | Role |
|----------|---------|------|
| `BACKTEST_DATE_CONCURRENCY` | `6` | Concurrent **trading dates** in Phase 1 (clamped 1–32). Higher → more RAM and API load. |
| `TICKERFLOW_API_CONCURRENCY` | `40` | Max concurrent **HTTP requests** to TickerFlow across all dates (clamped 1–256). Raise together with date concurrency; if too high you may see **429**, timeouts, or client OOM. |

Examples:

```bash
# Slightly more aggressive on a machine with headroom (e.g. 16 GB RAM)
export BACKTEST_DATE_CONCURRENCY=10
export TICKERFLOW_API_CONCURRENCY=72
python -m src.backtest.cli --config q1_2026.json --phase 1
```

Logs at Phase 1 startup print the effective values. **Enterprise:** cap rate limits on the API side, monitor DB CPU, and use secrets management instead of ad-hoc exports for production runners.

To **measure** good values for your machine (including when other services are running), use the isolated load sweep in [`stress_tests/README.md`](../stress_tests/README.md) (`stress_tests/sweep.py`).

---

## Suggested workflow

1. **Smoke test** a single date first: `--date 2025-02-28`
2. **Run Phase 1** for the full period: `--phase 1` (produces predictions Excel + pickle)
3. **Review predictions**, then run Phase 2: `--phase 2 --results-dir <run_dir>`
4. **Run Phase 3a/3b** on the same results: `--phase 3a,3b --results-dir <run_dir>`

This avoids re-running expensive phases when iterating on strategy parameters in later phases.
