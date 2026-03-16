"""
Backtest pipeline package.

Entry point:  python -m src.backtest.cli --config <preset>.json [--phase ...]

Modules:
    config/     – BacktestConfig dataclass + JSON presets
    core/       – Shared analysis, simulation, market filter, capital logic
    data/       – TickerFlow API client, Upstox candle fetcher
    phases/     – Phase 1 (OI), Phase 2 (OHLC), Phase 3a (FNO), Phase 3b (Capital)
    pipeline.py – Orchestrator (PipelineContext, run_pipeline)
    cli.py      – argparse CLI
"""
