"""
Backtest package: Phase 1 (OI), Phase 2 (OHLC), Phase 3 (F&O) runners live in
2025/ and 2026/; shared helpers (tickerflow_client, run_phase3_fno) in utils/.

Re-export utils so existing imports still work:
  from src.backtest import tickerflow_client
  from src.backtest.run_phase3_fno import find_results_dir, ...
"""
import sys

from src.backtest.utils import tickerflow_client
from src.backtest.utils import run_phase3_fno

# Backward compatibility: scripts use "from src.backtest import tickerflow_client"
# and "from src.backtest.run_phase3_fno import ..."
sys.modules["src.backtest.tickerflow_client"] = tickerflow_client
sys.modules["src.backtest.run_phase3_fno"] = run_phase3_fno

__all__ = ["tickerflow_client", "run_phase3_fno"]
