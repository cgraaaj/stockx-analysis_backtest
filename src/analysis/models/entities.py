"""
Data Models and Entities for Options Analysis
=============================================
Aligned with the TimescaleDB schema used by data collection and tickerflow.

Key schema facts:
  - options.stock:      id (UUID), name, instrument_key, is_active
  - options.instrument: id (UUID), instrument_seq (BIGSERIAL), stock_id, ...
  - options.ticker_ts:  instrument_id (int = instrument_seq), time_stamp, OHLCV+OI
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any

from ..config.settings import Grade


@dataclass
class Stock:
    id: str
    name: str
    symbol: str
    is_active: bool = True
    instrument_key: Optional[str] = None
    sector: Optional[str] = None
    lot_size: Optional[int] = None


@dataclass
class IndexContext:
    """Snapshot of a market index trend at a point in time."""
    name: str                          # e.g. "NIFTY 50"
    trend: str                         # "bullish", "bearish", "neutral"
    current_price: float
    sma_fast: float
    sma_slow: float
    timestamp: Optional[datetime] = None
    confidence: float = 0.0            # 0-1 strength of the trend signal


@dataclass
class Instrument:
    id: str  # UUID primary key
    instrument_seq: int  # BIGSERIAL surrogate key used in ticker_ts
    stock_id: str
    segment: str
    name: str
    exchange: str
    expiry: str
    expiry_epoch: int
    instrument_type: str  # 'CE', 'PE', 'FUT'
    asset_symbol: str
    underlying_symbol: str
    instrument_key: str
    lot_size: int
    freeze_quantity: int
    exchange_token: str
    minimum_lot: int
    asset_key: str
    underlying_key: str
    tick_size: float
    asset_type: str
    underlying_type: str
    trading_symbol: str
    strike_price: float
    weekly: bool


@dataclass
class TickerData:
    """Tick data from options.ticker_ts (TimescaleDB hypertable)."""
    instrument_id: int  # instrument_seq integer, NOT UUID
    time_stamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    open_interest: int
    ltp: Optional[float] = None
    ltp_change: Optional[float] = None
    open_interest_change: Optional[int] = None


@dataclass
class OptionAnalysis:
    bullish: int
    bearish: int
    percentage: float
    grade: str
    tn_ratio: int


@dataclass
class TimeAnalysisData:
    time_stamp: datetime
    calls: OptionAnalysis
    puts: OptionAnalysis
    call_trend: bool
    put_trend: bool


@dataclass
class StockPrediction:
    stock: str
    timestamp: datetime
    grade: str
    option_type: str  # 'call' or 'put'
    tn_ratio: int
    bullish_count: int
    bearish_count: int
    bullish_oi_weight: float = 0.0
    bearish_oi_weight: float = 0.0
    market_trend: Optional[str] = None       # "bullish", "bearish", "neutral"
    trend_alignment: Optional[str] = None    # "along", "against", "neutral"


@dataclass
class PredictionResult:
    call: List[Dict[str, Any]] = field(default_factory=list)
    put: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class OHLCData:
    """OHLC candle data from Upstox external API."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    open_interest: Optional[int] = None
    stock: Optional[str] = None
    date: Optional[str] = None
    instrument_key: Optional[str] = None
    grade: Optional[str] = None
    tn_ratio: Optional[int] = None
    sheet_type: Optional[str] = None


@dataclass
class CEPEPair:
    """Call-Put option pair keyed by instrument_seq for ticker_ts lookups."""
    ce_seq: Optional[int]  # instrument_seq for CE leg
    pe_seq: Optional[int]  # instrument_seq for PE leg
    strike_price: float


@dataclass
class TrendAnalysisResult:
    oi_action_x: str
    oi_action_y: str
    trend_x: Optional[str]
    trend_y: Optional[str]


@dataclass
class ProcessingBatch:
    stocks: List[Stock]
    trade_date: str
    expiry_date: str
    batch_id: int


@dataclass
class APIRequest:
    url: str
    stock_info: Dict[str, Any]
    retry_count: int = 0


@dataclass
class ExportConfig:
    filename: str
    format: str  # 'excel', 'csv', 'pickle'
    include_summary: bool = True
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class PerformanceMetrics:
    total_stocks_processed: int
    successful_predictions: int
    failed_predictions: int
    processing_time_seconds: float
    grade_distribution: Dict[str, int] = field(default_factory=dict)
    api_success_rate: Optional[float] = None


@dataclass
class AnalysisResultDTO:
    success: bool
    data: Optional[Any] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ExportResultDTO:
    success: bool
    files_created: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


@dataclass
class TradeSignal:
    """A fully evaluated trade with entry, SL, target, and simulated outcome."""
    stock: str
    signal_time: datetime
    option_type: str               # "call" or "put"
    grade: str
    tn_ratio: int
    entry_price: float
    stop_loss: float
    target: float
    risk: float                    # |entry - SL|
    reward: float                  # |target - entry|
    instrument_key: Optional[str] = None
    outcome: Optional[str] = None  # "target_hit", "sl_hit", "time_exit"
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    pnl_points: float = 0.0
    holding_candles: int = 0
    market_trend: Optional[str] = None      # index context at signal time
    trend_alignment: Optional[str] = None   # "along", "against", "neutral"
    vwap_at_signal: Optional[float] = None
    sl_method_used: Optional[str] = None