"""Backtest configuration schema and loading utilities."""

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class BacktestConfig:
    """Unified backtest configuration (Phase 1+2, 3a FNO, 3b Capital sim)."""

    # Strategy (Phase 1+2) - required
    name: str
    trading_dates: list[str]

    # Strategy (Phase 1+2) - optional with defaults
    description: str = ""
    # None or "" = no Phase 1 time filter (keep all signals within analysis hours).
    # Set "HH:MM:SS" to drop predictions after that clock time on each date.
    signal_cutoff_time: str | None = None
    batch_size: int = 10
    reward_ratio: float = 3.0
    sl_method: str = "percentage"
    sl_percentage: float = 1.0
    max_holding_candles: int = 24
    entry_candle_offset: int = 1
    use_vwap_bias: bool = True
    max_exit_time: str = "14:00:00"

    # Analysis
    oi_weight_blend: float = 1.0
    tn_ratio_threshold: int = 60

    # Market context
    market_context_enabled: bool = True
    primary_index: str = "NIFTY 50"
    filter_mode: str = "tag"
    index_trend_method: str = "vwap"
    truncate_to_signal_time: bool = True

    # Phase 3a FNO
    fno_capital_per_trade: int = 100_000
    fno_sl_pct: float = 7.0
    fno_target_pct: float = 14.0
    fno_use_rsi_filter: bool = True
    fno_rsi_overbought: float = 80.0
    fno_entry_mode: str = "immediate"
    fno_entry_candle_offset: int = 1

    # Phase 3b Capital sim
    starting_capital: int = 10_00_000
    max_capital_per_trade: int = 2_00_000
    grade_sl_pct: float = 10.0
    grade_target_pct: float = 30.0
    accepted_grades: list[str] = field(default_factory=lambda: ["A", "B"])
    grade_rsi_overbought: float = 80.0
    grade_entry_offset: int = 5

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict."""
        return asdict(self)


def load_config(path: str | Path) -> BacktestConfig:
    """Load BacktestConfig from JSON file. Relative paths resolve to config/presets/."""
    p = Path(path)
    if not p.is_absolute():
        p = Path(__file__).parent / "presets" / p
    with open(p) as f:
        data = json.load(f)
    fields = BacktestConfig.__dataclass_fields__
    return BacktestConfig(**{k: v for k, v in data.items() if k in fields})


def apply_to_globals(cfg: BacktestConfig) -> None:
    """Mutate analysis module global config singletons for backward compatibility."""
    from src.analysis.config.settings import (
        analysis_config,
        market_context_config,
        strategy_config,
    )

    strategy_config.REWARD_RATIO = cfg.reward_ratio
    strategy_config.SL_METHOD = cfg.sl_method
    strategy_config.MAX_HOLDING_CANDLES = cfg.max_holding_candles
    strategy_config.ENTRY_CANDLE_OFFSET = cfg.entry_candle_offset
    strategy_config.USE_VWAP_BIAS = cfg.use_vwap_bias
    strategy_config.SL_PERCENTAGE = cfg.sl_percentage
    strategy_config.MAX_EXIT_TIME = cfg.max_exit_time

    analysis_config.OI_WEIGHT_BLEND = cfg.oi_weight_blend
    analysis_config.USE_OI_MAGNITUDE_WEIGHTING = True
    analysis_config.TN_RATIO_THRESHOLD = cfg.tn_ratio_threshold

    market_context_config.ENABLED = cfg.market_context_enabled
    market_context_config.PRIMARY_INDEX = cfg.primary_index
    market_context_config.FILTER_MODE = cfg.filter_mode
    market_context_config.INDEX_TREND_METHOD = cfg.index_trend_method
    market_context_config.TRUNCATE_TO_SIGNAL_TIME = cfg.truncate_to_signal_time
