#!/bin/bash
# Full year 2025 backtest: Phase 1 (signal generation) + Phase 2 (trade simulation)
set -e

cd /home/cgraaaj/cgr-trades
source venv/bin/activate

LOGFILE="src/backtest/full_2025_backtest.log"

echo "========================================" | tee "$LOGFILE"
echo "Full 2025 Backtest - Started $(date)"    | tee -a "$LOGFILE"
echo "========================================" | tee -a "$LOGFILE"

echo ""                                         | tee -a "$LOGFILE"
echo ">>> PHASE 1: Signal Generation (220 days)" | tee -a "$LOGFILE"
echo ">>> Started at $(date)"                   | tee -a "$LOGFILE"
echo ""                                         | tee -a "$LOGFILE"

python3 src/backtest/run_backtest.py 2>&1 | tee -a "$LOGFILE"
P1_EXIT=${PIPESTATUS[0]}

if [ $P1_EXIT -ne 0 ]; then
    echo "PHASE 1 FAILED with exit code $P1_EXIT" | tee -a "$LOGFILE"
    exit $P1_EXIT
fi

echo ""                                         | tee -a "$LOGFILE"
echo ">>> PHASE 2: Trade Simulation"            | tee -a "$LOGFILE"
echo ">>> Started at $(date)"                   | tee -a "$LOGFILE"
echo ""                                         | tee -a "$LOGFILE"

python3 src/backtest/run_phase2.py 2>&1 | tee -a "$LOGFILE"
P2_EXIT=${PIPESTATUS[0]}

if [ $P2_EXIT -ne 0 ]; then
    echo "PHASE 2 FAILED with exit code $P2_EXIT" | tee -a "$LOGFILE"
    exit $P2_EXIT
fi

echo ""                                          | tee -a "$LOGFILE"
echo "=========================================" | tee -a "$LOGFILE"
echo "Full 2025 Backtest - COMPLETED $(date)"   | tee -a "$LOGFILE"
echo "=========================================" | tee -a "$LOGFILE"
