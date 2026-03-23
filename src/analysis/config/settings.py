"""
Options Analysis Configuration Settings
======================================
Centralized configuration for the options analysis system.
Aligned with the cgr-trades data collection pipeline and tickerflow backend.
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class Grade(Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"


@dataclass
class TradingConfig:
    TRADING_MINUTES_PER_DAY: int = 375  # 9:15 AM to 3:30 PM IST
    DEFAULT_INTERVAL: int = 15  # minutes
    MARKET_START_TIME: str = "09:15:00"
    MARKET_END_TIME: str = "15:30:00"
    TRADING_START_TIME: str = "09:30:00"
    TRADING_END_TIME: str = "14:00:00"


@dataclass
class AnalysisConfig:
    TN_RATIO_THRESHOLD: int = 60
    MAX_TIME_INTERVALS: int = 25
    ACCEPTED_GRADES: List[str] = None
    CONSECUTIVE_WINDOW_MINUTES: int = 15
    USE_OI_MAGNITUDE_WEIGHTING: bool = True
    # 0.0 = pure strike-count, 1.0 = pure OI magnitude, 0.5 = blended
    OI_WEIGHT_BLEND: float = 1.0

    def __post_init__(self):
        if self.ACCEPTED_GRADES is None:
            self.ACCEPTED_GRADES = ["A", "B", "C", "D"]


@dataclass
class StrategyConfig:
    """Configurable R:R trade execution parameters."""
    REWARD_RATIO: float = 2.0           # the 'R' in 1:R  (1:2 = 2.0)
    SL_METHOD: str = "vwap"             # "vwap", "candle_low", "percentage", "atr"
    SL_PERCENTAGE: float = 1.0          # used when SL_METHOD = "percentage"
    ATR_PERIOD: int = 14                # used when SL_METHOD = "atr"
    ATR_MULTIPLIER: float = 1.5         # SL = entry -/+ ATR * multiplier
    MAX_HOLDING_CANDLES: int = 24       # force exit after N candles (~2 hrs at 5min)
    ENTRY_CANDLE_OFFSET: int = 1        # enter N candles after signal (0 = same candle)
    MIN_RISK_POINTS: float = 0.5        # skip trades where risk is negligibly small

    # VWAP-based entry & SL
    USE_VWAP_BIAS: bool = True          # require price on correct side of VWAP
    VWAP_SL_BUFFER_PCT: float = 0.1     # buffer below/above VWAP for SL (0.1 = 0.1%)

    # Intraday hard exit
    MAX_EXIT_TIME: str = "14:00:00"     # force exit (aligned with analysis session end)

    # Support / Resistance (disabled by default)
    USE_SR_LEVELS: bool = False         # pivot-point S/R; enable later
    SR_SL_BUFFER_PCT: float = 0.1       # buffer around S/R levels


@dataclass
class MarketContextConfig:
    """Index-level trend filter to avoid trading against the market."""
    ENABLED: bool = True
    PRIMARY_INDEX: str = "NIFTY 50"
    # name → Upstox instrument key  (NSE_INDEX segment)
    INDICES: Dict[str, str] = None
    # "vwap" = price vs index VWAP at signal time; "price_sma" = SMA crossover
    INDEX_TREND_METHOD: str = "vwap"
    SMA_FAST_PERIOD: int = 5            # candles (for price_sma method)
    SMA_SLOW_PERIOD: int = 20           # candles (for price_sma method)
    # "block" rejects conflicting signals; "downgrade" drops one grade level
    FILTER_MODE: str = "block"
    # Per-stock override: stock_name → index_name
    SECTOR_INDEX_MAP: Dict[str, str] = None
    # Truncate candles to signal time to avoid lookahead bias in backtesting
    TRUNCATE_TO_SIGNAL_TIME: bool = True

    def __post_init__(self):
        if self.INDICES is None:
            self.INDICES = {
                "NIFTY 50": "NSE_INDEX|Nifty 50",
                "NIFTY BANK": "NSE_INDEX|Nifty Bank",
                "NIFTY IT": "NSE_INDEX|Nifty IT",
                "NIFTY FIN SERVICE": "NSE_INDEX|Nifty Financial Services",
            }
        if self.SECTOR_INDEX_MAP is None:
            self.SECTOR_INDEX_MAP = {}


@dataclass
class ProcessingConfig:
    BATCH_SIZE: int = 50
    MAX_CONCURRENT_TASKS: int = 10
    MAX_CONCURRENT_REQUESTS: int = 5
    MAX_RETRIES: int = 3


@dataclass
class DatabaseConfig:
    POOL_SIZE: int = 20
    MAX_OVERFLOW: int = 30
    POOL_RECYCLE: int = 3600
    ECHO: bool = False
    TICKER_TABLE: str = "options.ticker_ts"
    INSTRUMENT_TABLE: str = "options.instrument"
    STOCK_TABLE: str = "options.stock"


@dataclass
class APIConfig:
    UPSTOX_BASE_URL: str = "https://api.upstox.com/v2/historical-candle"
    DEFAULT_INTERVAL: str = "5"  # 5-minute candles


class MarketActions:
    BULLISH = ["Short Cover", "Long Buildup"]
    BEARISH = ["Long Unwind", "Short Buildup"]


# Singleton instances
trading_config = TradingConfig()
analysis_config = AnalysisConfig()
strategy_config = StrategyConfig()
market_context_config = MarketContextConfig()
processing_config = ProcessingConfig()
database_config = DatabaseConfig()
api_config = APIConfig()
market_actions = MarketActions()


def get_project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))


def get_analysis_dir() -> str:
    return os.path.join(get_project_root(), 'src', 'analysis')
