"""
Options Analysis Module
======================
Clean MVC architecture for options trading analysis.

Aligned with:
  - cgr-trades data collection pipeline (ticker_ts hypertable, instrument_seq)
  - tickerflow Django backend (shared TimescaleDB, options schema)
"""

from .main import OptionsAnalysisApp
from .controllers.analysis_controller import AnalysisController
from .controllers.data_processor_controller import DataProcessorController
from .models.entities import (
    Stock, StockPrediction, PredictionResult,
    AnalysisResultDTO, ExportResultDTO,
    TradeSignal, IndexContext,
)
from .config.settings import (
    trading_config, analysis_config, processing_config,
    database_config, api_config,
    strategy_config, market_context_config,
)

__version__ = "3.1.0"

__all__ = [
    "OptionsAnalysisApp",
    "AnalysisController",
    "DataProcessorController",
    "Stock",
    "StockPrediction",
    "PredictionResult",
    "AnalysisResultDTO",
    "ExportResultDTO",
    "TradeSignal",
    "IndexContext",
    "trading_config",
    "analysis_config",
    "processing_config",
    "database_config",
    "api_config",
    "strategy_config",
    "market_context_config",
]
