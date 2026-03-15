#!/bin/bash
# Q1 2026 Backtest: Phase 1 (OI signal gen) + Phase 2+3 (F&O option sim)
# Grade A+B | 1:3 R:R | 10L Capital | 45 trading days (Jan-Mar 2026)
set -e

cd /home/cgraaaj/cgr-trades
source venv/bin/activate

LOGFILE="src/backtest/q1_2026_backtest.log"

echo "========================================" | tee "$LOGFILE"
echo "Q1 2026 Backtest - Started $(date)"      | tee -a "$LOGFILE"
echo "========================================" | tee -a "$LOGFILE"
echo ""                                         | tee -a "$LOGFILE"

python3 -m src.backtest.run_2026q1 2>&1 | tee -a "$LOGFILE"
EXIT_CODE=${PIPESTATUS[0]}

echo ""                                          | tee -a "$LOGFILE"
if [ $EXIT_CODE -ne 0 ]; then
    echo "BACKTEST FAILED with exit code $EXIT_CODE" | tee -a "$LOGFILE"
    exit $EXIT_CODE
fi

echo "=========================================" | tee -a "$LOGFILE"
echo "Q1 2026 Backtest - COMPLETED $(date)"     | tee -a "$LOGFILE"
echo "=========================================" | tee -a "$LOGFILE"
