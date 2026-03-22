#!/usr/bin/env bash
# Run Q1 2026 backtest in background with nohup.
# Logs go to logs/backtest_q1_2026_YYYYMMDD_HHMMSS.log
# Usage: ./scripts/run_backtest_q1_2026_nohup.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/backtest_q1_2026_${TIMESTAMP}.log"

PYTHON="${PROJECT_ROOT}/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  echo "Virtual env not found. Create with: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

echo "Starting Q1 2026 backtest in background."
echo "  Config: q1_2026.json"
echo "  Log:    $LOG_FILE"
echo "  PID:    (see below)"
nohup "$PYTHON" -m src.backtest.cli --config q1_2026.json \
  >> "$LOG_FILE" 2>&1 &
echo $!
echo "Run: tail -f $LOG_FILE"
