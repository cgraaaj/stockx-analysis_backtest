"""
Options Analysis Module
======================
Clean MVC architecture for options trading analysis.

Aligned with:
  - cgr-trades data collection pipeline (ticker_ts hypertable, instrument_seq)
  - tickerflow Django backend (shared TimescaleDB, options schema)

Package exports are loaded lazily so ``from src.analysis.config.settings import ...``
and other submodule imports do not pull in DatabaseService / ``databases`` unless
the interactive app entrypoints are used.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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

_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "OptionsAnalysisApp": (".main", "OptionsAnalysisApp"),
    "AnalysisController": (".controllers.analysis_controller", "AnalysisController"),
    "DataProcessorController": (
        ".controllers.data_processor_controller",
        "DataProcessorController",
    ),
    "Stock": (".models.entities", "Stock"),
    "StockPrediction": (".models.entities", "StockPrediction"),
    "PredictionResult": (".models.entities", "PredictionResult"),
    "AnalysisResultDTO": (".models.entities", "AnalysisResultDTO"),
    "ExportResultDTO": (".models.entities", "ExportResultDTO"),
    "TradeSignal": (".models.entities", "TradeSignal"),
    "IndexContext": (".models.entities", "IndexContext"),
    "trading_config": (".config.settings", "trading_config"),
    "analysis_config": (".config.settings", "analysis_config"),
    "processing_config": (".config.settings", "processing_config"),
    "database_config": (".config.settings", "database_config"),
    "api_config": (".config.settings", "api_config"),
    "strategy_config": (".config.settings", "strategy_config"),
    "market_context_config": (".config.settings", "market_context_config"),
}

if TYPE_CHECKING:
    from .config.settings import (
        analysis_config as analysis_config,
        api_config as api_config,
        database_config as database_config,
        market_context_config as market_context_config,
        processing_config as processing_config,
        strategy_config as strategy_config,
        trading_config as trading_config,
    )
    from .controllers.analysis_controller import AnalysisController as AnalysisController
    from .controllers.data_processor_controller import (
        DataProcessorController as DataProcessorController,
    )
    from .main import OptionsAnalysisApp as OptionsAnalysisApp
    from .models.entities import (
        AnalysisResultDTO as AnalysisResultDTO,
        ExportResultDTO as ExportResultDTO,
        IndexContext as IndexContext,
        PredictionResult as PredictionResult,
        Stock as Stock,
        StockPrediction as StockPrediction,
        TradeSignal as TradeSignal,
    )


def __getattr__(name: str) -> Any:
    spec = _LAZY_ATTRS.get(name)
    if spec is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    mod, attr = spec
    import importlib

    module = importlib.import_module(mod, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
