# Scripts

## Nohup backtest runs

Run long backtests in the background so they survive closing the terminal.

| Script | Config | Description |
|--------|--------|-------------|
| `run_backtest_2025_nohup.sh` | full_2025.json | Full year 2025 (all phases) |
| `run_backtest_q1_2026_nohup.sh` | q1_2026.json | Q1 2026 (all phases) |
| `run_backtest_march_2026_nohup.sh` | march_2026.json | March 2026 only (all phases) |

**Usage (from project root):**

```bash
chmod +x scripts/run_backtest_2025_nohup.sh
chmod +x scripts/run_backtest_q1_2026_nohup.sh

# Full 2025
./scripts/run_backtest_2025_nohup.sh
# Prints PID and log path; follow with: tail -f logs/backtest_2025_*.log

# Q1 2026
./scripts/run_backtest_q1_2026_nohup.sh
# Follow: tail -f logs/backtest_q1_2026_*.log

# March 2026 only
./scripts/run_backtest_march_2026_nohup.sh
# Follow: tail -f logs/backtest_march_2026_*.log
```

Logs are written under `logs/` with a timestamp in the filename. Results go to `src/backtest/results/` as usual.

## Release notes

- `update-release-notes.sh` — appends a new release entry to `RELEASE_NOTES.md` (used by CI on push to master).
