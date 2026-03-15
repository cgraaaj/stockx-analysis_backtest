# Options Analysis — MVC Architecture (v3.1)

## Overview

Options chain analysis pipeline that reads tick data from TimescaleDB, classifies OI actions, generates grade-based predictions, evaluates trades with configurable R:R, and filters signals using market index context.

**Aligned with:**
- **Data collection** (`src/data_collection/opt-stk-data-to-db-upstox-nosync.py`) — writes to `options.ticker_ts` using `instrument_seq`
- **TickerFlow** (`~/tickerflow/`) — Django SaaS API that reads from the same `options.ticker_ts` hypertable

## Architecture

```
src/analysis/
├── config/
│   └── settings.py                    # TradingConfig, AnalysisConfig, StrategyConfig,
│                                      # MarketContextConfig, ProcessingConfig, etc.
├── models/
│   └── entities.py                    # Stock, StockPrediction, TradeSignal, IndexContext, DTOs
├── services/
│   ├── database_service.py            # TimescaleDB queries (ticker_ts)
│   ├── analysis_service.py            # OI action classification, OI magnitude weighting, grading
│   ├── external_api_service.py        # Upstox API + instrument key mapping
│   ├── market_context_service.py      # Index trend evaluation (SMA crossover) + signal filtering
│   └── trade_service.py              # R:R trade execution (entry/SL/target/outcome simulation)
├── controllers/
│   ├── analysis_controller.py         # Analysis workflow orchestration + market context filter
│   └── data_processor_controller.py   # Excel processing + OHLC fetching + trade evaluation
├── views/
│   └── export_view.py                 # Excel, CSV, Pickle, Trade Signals exports
├── main.py                            # App entry point & CLI
└── __init__.py
```

## Database Schema (shared with data collection & tickerflow)

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `options.stock` | `id` (UUID), `name`, `instrument_key`, `is_active` | `instrument_key` populated by data collection |
| `options.instrument` | `id` (UUID), `instrument_seq` (BIGSERIAL), `stock_id`, `instrument_type`, `strike_price`, `expiry` | `instrument_seq` is the FK used in `ticker_ts` |
| `options.ticker_ts` | `instrument_id` (int = `instrument_seq`), `time_stamp`, OHLCV+OI | TimescaleDB hypertable |

## New in v3.1: Strategy Features

### 1. Configurable R:R Exit Signal

Trades are automatically evaluated with a configurable Risk:Reward ratio (default 1:2).

```python
from src.analysis.config.settings import strategy_config

strategy_config.REWARD_RATIO = 2.0         # 1:2 R:R
strategy_config.SL_METHOD = "candle_low"   # "candle_low", "percentage", "atr"
strategy_config.SL_PERCENTAGE = 1.0        # for percentage method
strategy_config.ATR_PERIOD = 14            # for ATR method
strategy_config.ATR_MULTIPLIER = 1.5       # for ATR method
strategy_config.MAX_HOLDING_CANDLES = 24   # force exit after N candles
strategy_config.ENTRY_CANDLE_OFFSET = 1    # enter 1 candle after signal
```

### 2. OI Magnitude Weighting

Instead of only counting bullish vs bearish strikes (binary), the system now also weights by the absolute OI change magnitude — giving more influence to strikes with heavy institutional positioning.

```python
from src.analysis.config.settings import analysis_config

analysis_config.USE_OI_MAGNITUDE_WEIGHTING = True
analysis_config.OI_WEIGHT_BLEND = 0.5  # 0.0=count-only, 1.0=OI-only, 0.5=blended
```

### 3. Market Context Filtering

Signals are filtered through configurable index trend analysis. If NIFTY is bearish, call signals are blocked or downgraded.

```python
from src.analysis.config.settings import market_context_config

market_context_config.ENABLED = True
market_context_config.PRIMARY_INDEX = "NIFTY 50"
market_context_config.INDICES = {
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "NIFTY BANK": "NSE_INDEX|Nifty Bank",
    "NIFTY IT": "NSE_INDEX|Nifty IT",
}
market_context_config.INDEX_TREND_METHOD = "price_sma"
market_context_config.SMA_FAST_PERIOD = 5    # 5 candles
market_context_config.SMA_SLOW_PERIOD = 20   # 20 candles
market_context_config.FILTER_MODE = "block"  # "block" or "downgrade"
market_context_config.SECTOR_INDEX_MAP = {
    "INFY": "NIFTY IT",
    "TCS": "NIFTY IT",
    "HDFCBANK": "NIFTY BANK",
}
```

## Quick Start

### Programmatic

```python
from src.analysis import OptionsAnalysisApp

app = OptionsAnalysisApp()
result = await app.run_full_analysis_workflow()

if result["success"]:
    print(f"Files: {result['all_files_created']}")
```

### CLI

```bash
# Full workflow (analysis + data processing + trade evaluation)
python -m src.analysis.main --mode full

# Analysis only
python -m src.analysis.main --mode analysis

# Data processing only (requires existing predictions Excel)
python -m src.analysis.main --mode processing
```

## Analysis Pipeline

1. **Fetch** active stocks and their CE/PE instruments from the DB
2. **Market Context** — fetch index candles, compute SMA trend for NIFTY/BANKNIFTY/NIFTY IT
3. **Read** 1-minute tick data from `options.ticker_ts` for the given trading date
4. **Pair** CE and PE instruments by strike price
5. **Classify** each candle's OI action: Long Buildup, Short Cover, Short Buildup, Long Unwind
6. **Grade** each time-slice (A/B/C/D) with optional OI magnitude blending
7. **Rank** stocks that appear in consecutive 15-minute intervals with high TN ratio
8. **Filter** — block or downgrade signals that conflict with the market index trend
9. **Trade Evaluation** — compute entry/SL/target per R:R, simulate forward for outcome
10. **Export** predictions + trade signals to Excel/CSV/Pickle

## Output Files

| File | Format | Contents |
|------|--------|----------|
| `analyzed_stocks_data_optimized_YYYYMMDD.pickle` | Pickle | Raw analysis results per stock |
| `option_predictions_optimized_YYYYMMDD.xlsx` | Excel | Calls/Puts/Summary sheets |
| `prediction_optimized_YYYYMMDD.pickle` | Pickle | Serialized PredictionResult |
| `option_predictions_calls_puts_*.xlsx` | Excel | Enhanced with instrument keys |
| `calls_ohlc_data_*.csv` | CSV | 5-minute OHLC candles for call predictions |
| `puts_ohlc_data_*.csv` | CSV | 5-minute OHLC candles for put predictions |
| `trade_signals_*.xlsx` | Excel | Trade Signals + Summary (W/L/T, PnL, win rate) |

## Configuration Reference

All settings in `config/settings.py`:

| Config Class | Key Parameters | Purpose |
|-------------|---------------|---------|
| `TradingConfig` | `DEFAULT_INTERVAL`, `TRADING_START_TIME` | Market hours & interval |
| `AnalysisConfig` | `TN_RATIO_THRESHOLD`, `OI_WEIGHT_BLEND` | Signal quality thresholds |
| `StrategyConfig` | `REWARD_RATIO`, `SL_METHOD`, `MAX_HOLDING_CANDLES` | Trade execution rules |
| `MarketContextConfig` | `INDICES`, `FILTER_MODE`, `SMA_*_PERIOD` | Index trend filter |
| `ProcessingConfig` | `BATCH_SIZE`, `MAX_CONCURRENT_REQUESTS` | Concurrency limits |
| `DatabaseConfig` | `TICKER_TABLE`, `POOL_SIZE` | DB connection settings |
| `APIConfig` | `UPSTOX_BASE_URL`, `DEFAULT_INTERVAL` | Upstox endpoint config |
