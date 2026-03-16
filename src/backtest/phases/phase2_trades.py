"""Phase 2: OHLC Trade Simulation.

Reads predictions from Phase 1, fetches OHLC candles from Upstox,
applies VWAP bias check, then simulates trades with configurable
R:R, SL method, and hard intraday exit.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.analysis.config.settings import strategy_config
from src.analysis.models.entities import StockPrediction, TradeSignal
from src.analysis.services.trade_service import TradeExecutionService
from src.analysis.views.export_view import ExportView
from src.backtest.config.schema import BacktestConfig
from src.backtest.data import tickerflow as tf
from src.backtest.data.upstox import fetch_all_candles

logger = logging.getLogger("Phase2")


def load_predictions(pred_file: Path) -> list[StockPrediction]:
    """Load predictions from Phase 1 Excel."""
    preds = []
    for sheet, opt_type in [("Calls", "call"), ("Puts", "put")]:
        try:
            df = pd.read_excel(str(pred_file), sheet_name=sheet)
        except Exception:
            continue
        if df.empty:
            continue

        for row in df.itertuples(index=False):
            date = pd.to_datetime(row.Date)
            time_str = str(row.Time)
            ts_parts = time_str.split(":")
            signal_ts = datetime(
                date.year, date.month, date.day,
                int(ts_parts[0]), int(ts_parts[1]),
                int(ts_parts[2]) if len(ts_parts) > 2 else 0,
            )
            market_trend = getattr(row, "Market_Trend", "") or None
            trend_alignment = getattr(row, "Trend_Alignment", "") or None
            preds.append(StockPrediction(
                stock=row.Stock,
                timestamp=signal_ts,
                grade=row.Grade,
                option_type=opt_type,
                tn_ratio=int(row.TN_Ratio),
                bullish_count=int(row.Bullish_Count),
                bearish_count=int(row.Bearish_Count),
                market_trend=market_trend,
                trend_alignment=trend_alignment,
            ))
    return preds


def _run_trade_simulation(
    predictions: list[StockPrediction],
    candles_by_key: dict[str, dict],
) -> list[TradeSignal]:
    trade_svc = TradeExecutionService()
    signals = []
    skipped_vwap = 0

    for pred in predictions:
        date_str = pred.timestamp.strftime("%Y-%m-%d")
        lookup_key = f"{pred.stock}_{date_str}"
        data = candles_by_key.get(lookup_key)
        if data is None:
            logger.warning(f"No OHLC for {pred.stock} on {date_str}, skipping")
            continue

        ohlc_5m = data["ohlc_5m"]
        raw_1m = data["raw_1m"]

        vwap = None
        if trade_svc.cfg.USE_VWAP_BIAS or trade_svc.cfg.SL_METHOD == "vwap":
            vwap = trade_svc.check_vwap_bias(pred, raw_1m)
            if vwap is None and trade_svc.cfg.USE_VWAP_BIAS:
                skipped_vwap += 1
                logger.info(f"  {pred.stock} ({date_str}): VWAP bias rejected ({pred.option_type})")
                continue

        result = trade_svc._build_trade_signal(pred, ohlc_5m, vwap=vwap)
        if result:
            signals.append(result)
            logger.info(
                f"  {result.stock} ({date_str}): "
                f"VWAP={result.vwap_at_signal} | "
                f"entry={result.entry_price}, SL={result.stop_loss}, "
                f"target={result.target}, outcome={result.outcome}, "
                f"PnL={result.pnl_points:+.2f}pts ({result.holding_candles} candles)"
            )
        else:
            logger.warning(f"  {pred.stock} ({date_str}): could not build trade signal")

    if skipped_vwap:
        logger.info(f"  VWAP bias rejected {skipped_vwap} signals")
    return signals


async def run(config: BacktestConfig, run_dir: Path, *, predictions_file: Path | None = None):
    """Execute Phase 2.

    Returns list[TradeSignal].
    """
    start = time.time()

    if predictions_file is None:
        matches = list(run_dir.glob("predictions_*.xlsx"))
        if not matches:
            logger.error(
                "Phase 2 cannot run: no predictions Excel from Phase 1. "
                "Re-run Phase 1 first."
            )
            return []
        predictions_file = matches[0]

    logger.info("=" * 60)
    logger.info("Phase 2: Trade Execution Simulation")
    logger.info("=" * 60)
    logger.info(f"R:R = 1:{strategy_config.REWARD_RATIO}")
    logger.info(f"SL method = {strategy_config.SL_METHOD}")
    logger.info(f"VWAP bias = {strategy_config.USE_VWAP_BIAS}")
    logger.info(f"Max holding = {strategy_config.MAX_HOLDING_CANDLES} candles")
    logger.info(f"Hard exit = {strategy_config.MAX_EXIT_TIME}")

    preds = load_predictions(predictions_file)
    if not preds:
        logger.error("No predictions found")
        return []
    logger.info(f"Loaded {len(preds)} predictions from {predictions_file.name}")

    key_map = await tf.aget_stock_key_map()
    logger.info(f"Loaded {len(key_map)} instrument key mappings")

    pairs = set()
    for p in preds:
        pairs.add((p.stock, p.timestamp.strftime("%Y-%m-%d")))

    candles = await fetch_all_candles(pairs, key_map)

    logger.info("=" * 60)
    logger.info("Simulating trades...")
    logger.info("=" * 60)
    signals = _run_trade_simulation(preds, candles)

    if not signals:
        logger.error("No trade signals generated")
        return []

    export = ExportView()
    output_file = str(run_dir / "trade_signals_phase2.xlsx")
    result = export.export_trade_signals_to_excel(signals, output_file)
    if result.success:
        logger.info(f"Trade signals exported to {output_file}")

    wins = sum(1 for s in signals if s.outcome == "target_hit")
    losses = sum(1 for s in signals if s.outcome == "sl_hit")
    time_exits = sum(1 for s in signals if s.outcome == "time_exit")
    total = len(signals)
    total_pnl = sum(s.pnl_points for s in signals)

    logger.info("=" * 60)
    logger.info("PHASE 2 RESULTS")
    logger.info(f"  Total trades  : {total}")
    logger.info(f"  Target hit (W): {wins}")
    logger.info(f"  SL hit (L)    : {losses}")
    logger.info(f"  Time exit (T) : {time_exits}")
    logger.info(f"  Win rate      : {wins / total * 100:.1f}%" if total else "  Win rate      : N/A")
    logger.info(f"  Total PnL     : {total_pnl:+.2f} pts")
    logger.info("=" * 60)

    def _serialize(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, pd.Timestamp):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    summary = {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "time_exits": time_exits,
        "win_rate_pct": round(wins / total * 100, 1) if total else 0,
        "total_pnl_points": round(total_pnl, 2),
        "avg_pnl_per_trade": round(total_pnl / total, 2) if total else 0,
        "rr_ratio": f"1:{strategy_config.REWARD_RATIO}",
        "sl_method": strategy_config.SL_METHOD,
        "vwap_bias": strategy_config.USE_VWAP_BIAS,
        "max_exit_time": strategy_config.MAX_EXIT_TIME,
        "max_holding_candles": strategy_config.MAX_HOLDING_CANDLES,
        "execution_time_seconds": round(time.time() - start, 1),
        "trades": [
            {
                "stock": s.stock,
                "option_type": s.option_type,
                "signal_time": str(s.signal_time),
                "grade": s.grade,
                "vwap_at_signal": s.vwap_at_signal,
                "entry_price": s.entry_price,
                "stop_loss": s.stop_loss,
                "target": s.target,
                "risk": s.risk,
                "reward": s.reward,
                "outcome": s.outcome,
                "exit_price": s.exit_price,
                "exit_time": str(s.exit_time),
                "pnl_points": s.pnl_points,
                "holding_candles": s.holding_candles,
                "market_trend": s.market_trend,
                "trend_alignment": s.trend_alignment,
            }
            for s in signals
        ],
    }

    with open(run_dir / "phase2_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=_serialize)

    logger.info(f"Results saved to {run_dir}")
    logger.info(f"Phase 2 completed in {time.time() - start:.1f}s")

    return signals
