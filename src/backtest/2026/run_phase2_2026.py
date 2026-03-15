"""
Phase 2: Trade Execution Simulation
====================================
Reads predictions from Phase 1, fetches OHLC candles from Upstox,
applies VWAP bias check, then simulates trades with configurable
R:R, SL method (VWAP / candle_low / etc.), and hard intraday exit.

Usage:
  python -m src.backtest.run_phase2 [results_dir]

  If results_dir is omitted, uses the most recent run in src/backtest/results/.

Data sources:
  - TickerFlow API for stock instrument-key lookups
  - Upstox public API for stock equity candles
"""

import asyncio
import json
import logging
import os
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

import aiohttp
import pandas as pd

from src.analysis.config.settings import strategy_config, analysis_config
from src.analysis.models.entities import StockPrediction, TradeSignal
from src.analysis.services.trade_service import TradeExecutionService
from src.analysis.views.export_view import ExportView
from src.backtest import tickerflow_client as tf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("Phase2")

UPSTOX_BASE = "https://api.upstox.com/v2/historical-candle"


def find_results_dir() -> Path:
    """Find the latest results directory or use CLI arg."""
    if len(sys.argv) > 1:
        return Path(sys.argv[1])
    base = Path(__file__).parent / "results"
    dirs = sorted(base.iterdir(), key=lambda d: d.stat().st_mtime, reverse=True)
    for d in dirs:
        if d.is_dir():
            return d
    raise FileNotFoundError("No results directory found")


def apply_config_from_backtest(run_dir: Path):
    """Read backtest_config.json and apply strategy overrides."""
    cfg_file = run_dir / "backtest_config.json"
    if not cfg_file.exists():
        logger.warning("No backtest_config.json found, using defaults")
        return

    with open(cfg_file) as f:
        cfg = json.load(f)

    sc = cfg.get("strategy", {})
    strategy_config.REWARD_RATIO = sc.get("reward_ratio", 2.0)
    strategy_config.SL_METHOD = sc.get("sl_method", "vwap")
    strategy_config.MAX_HOLDING_CANDLES = sc.get("max_holding_candles", 24)
    strategy_config.ENTRY_CANDLE_OFFSET = sc.get("entry_candle_offset", 1)
    strategy_config.USE_VWAP_BIAS = sc.get("use_vwap_bias", True)
    strategy_config.SL_PERCENTAGE = sc.get("sl_percentage", 1.0)
    strategy_config.MAX_EXIT_TIME = sc.get("max_exit_time", "15:15:00")
    analysis_config.OI_WEIGHT_BLEND = cfg.get("analysis", {}).get("oi_weight_blend", 1.0)


def find_predictions_file(run_dir: Path) -> Path:
    matches = list(run_dir.glob("predictions_*.xlsx"))
    if not matches:
        raise FileNotFoundError(f"No predictions Excel in {run_dir}")
    return matches[0]


def load_predictions(pred_file: Path) -> list[StockPrediction]:
    preds = []
    for sheet, opt_type in [("Calls", "call"), ("Puts", "put")]:
        try:
            df = pd.read_excel(str(pred_file), sheet_name=sheet)
        except Exception:
            continue
        if df.empty:
            continue

        for _, row in df.iterrows():
            date = pd.to_datetime(row["Date"])
            time_str = str(row["Time"])
            ts_parts = time_str.split(":")
            signal_ts = datetime(
                date.year, date.month, date.day,
                int(ts_parts[0]), int(ts_parts[1]),
                int(ts_parts[2]) if len(ts_parts) > 2 else 0,
            )
            preds.append(StockPrediction(
                stock=row["Stock"],
                timestamp=signal_ts,
                grade=row["Grade"],
                option_type=opt_type,
                tn_ratio=int(row["TN_Ratio"]),
                bullish_count=int(row["Bullish_Count"]),
                bearish_count=int(row["Bearish_Count"]),
            ))
    return preds


def load_instrument_keys() -> dict[str, str]:
    """Fetch {stock_name: instrument_key} via TickerFlow API."""
    return tf.get_stock_key_map()


def resample_to_5min(df_1m: pd.DataFrame) -> pd.DataFrame:
    df = df_1m.set_index("timestamp")
    stock_name = df["stock"].iloc[0] if "stock" in df.columns else ""

    ohlc = df.resample("5min", label="left", closed="left").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna()

    ohlc.reset_index(inplace=True)
    ohlc["stock"] = stock_name
    return ohlc


async def fetch_stock_candles_raw(
    session: aiohttp.ClientSession,
    instrument_key: str,
    date: str,
    stock_name: str,
) -> pd.DataFrame:
    """Fetch raw 1-minute candles from Upstox."""
    encoded = urllib.parse.quote(instrument_key, safe="")
    url = f"{UPSTOX_BASE}/{encoded}/1minute/{date}/{date}"

    for attempt in range(3):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "success":
                        candles = data.get("data", {}).get("candles", [])
                        if candles:
                            df = pd.DataFrame(candles, columns=[
                                "timestamp", "open", "high", "low", "close", "volume", "oi",
                            ])
                            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
                            df["stock"] = stock_name
                            df.sort_values("timestamp", inplace=True)
                            df.reset_index(drop=True, inplace=True)
                            return df
                logger.warning(f"HTTP {resp.status} for {stock_name} on {date}")
        except Exception as e:
            logger.error(f"Attempt {attempt+1} for {stock_name}: {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    return pd.DataFrame()


async def fetch_all_candles(
    predictions: list[StockPrediction],
    key_map: dict[str, str],
) -> dict[str, dict]:
    """Fetch 1-min candles and return both raw and 5-min resampled per (stock, date)."""
    pairs = set()
    for p in predictions:
        date_str = p.timestamp.strftime("%Y-%m-%d")
        pairs.add((p.stock, date_str))

    logger.info(f"Fetching candles for {len(pairs)} (stock, date) pairs...")
    result: dict[str, dict] = {}

    async with aiohttp.ClientSession() as session:
        for stock, date in pairs:
            ikey = key_map.get(stock)
            if not ikey:
                logger.warning(f"No instrument_key for {stock}")
                continue

            raw_1m = await fetch_stock_candles_raw(session, ikey, date, stock)
            if not raw_1m.empty:
                resampled_5m = resample_to_5min(raw_1m)
                lookup_key = f"{stock}_{date}"
                result[lookup_key] = {
                    "raw_1m": raw_1m,
                    "ohlc_5m": resampled_5m,
                }
                logger.info(f"  {stock} ({date}): {len(raw_1m)} 1m → {len(resampled_5m)} 5m candles")
            else:
                logger.warning(f"  {stock} ({date}): no candles")

    return result


def run_trade_simulation(
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
                logger.info(
                    f"  {pred.stock} ({date_str}): VWAP bias rejected "
                    f"({pred.option_type})"
                )
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


async def main():
    start = time.time()

    run_dir = find_results_dir()
    logger.info(f"Using results from: {run_dir}")

    apply_config_from_backtest(run_dir)
    pred_file = find_predictions_file(run_dir)

    logger.info("=" * 60)
    logger.info("Phase 2: Trade Execution Simulation")
    logger.info("=" * 60)
    logger.info(f"R:R = 1:{strategy_config.REWARD_RATIO}")
    logger.info(f"SL method = {strategy_config.SL_METHOD}")
    logger.info(f"VWAP bias = {strategy_config.USE_VWAP_BIAS}")
    logger.info(f"VWAP SL buffer = {strategy_config.VWAP_SL_BUFFER_PCT}%")
    logger.info(f"Max holding = {strategy_config.MAX_HOLDING_CANDLES} candles")
    logger.info(f"Hard exit = {strategy_config.MAX_EXIT_TIME}")
    logger.info(f"Entry offset = {strategy_config.ENTRY_CANDLE_OFFSET} candle(s) after signal")

    preds = load_predictions(pred_file)
    if not preds:
        logger.error("No predictions found")
        return
    logger.info(f"Loaded {len(preds)} predictions from {pred_file.name}")
    for p in preds:
        logger.info(f"  {p.stock} ({p.option_type}) | {p.timestamp} | Grade {p.grade} | TN {p.tn_ratio}")

    key_map = load_instrument_keys()
    logger.info(f"Loaded {len(key_map)} instrument key mappings from TickerFlow API")

    candles = await fetch_all_candles(preds, key_map)

    logger.info("=" * 60)
    logger.info("Simulating trades...")
    logger.info("=" * 60)
    signals = run_trade_simulation(preds, candles)

    if not signals:
        logger.error("No trade signals generated")
        return

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
    logger.info("=" * 60)
    logger.info(f"  Total trades  : {total}")
    logger.info(f"  Target hit (W): {wins}")
    logger.info(f"  SL hit (L)    : {losses}")
    logger.info(f"  Time exit (T) : {time_exits}")
    logger.info(f"  Win rate      : {wins/total*100:.1f}%" if total else "  Win rate      : N/A")
    logger.info(f"  Total PnL     : {total_pnl:+.2f} pts")
    logger.info(f"  Avg PnL/trade : {total_pnl/total:+.2f} pts" if total else "  Avg PnL/trade : N/A")
    logger.info(f"  R:R ratio     : 1:{strategy_config.REWARD_RATIO}")
    logger.info(f"  SL method     : {strategy_config.SL_METHOD}")

    logger.info("")
    logger.info("Trade details:")
    for s in signals:
        vwap_str = f"VWAP={s.vwap_at_signal}" if s.vwap_at_signal else "VWAP=N/A"
        logger.info(
            f"  {s.stock:12s} | {s.option_type:4s} | {s.signal_time.strftime('%Y-%m-%d %H:%M')} | "
            f"{vwap_str:>12s} | Entry {s.entry_price:8.2f} | SL {s.stop_loss:8.2f} | "
            f"Target {s.target:8.2f} | {s.outcome:10s} | "
            f"Exit {s.exit_price:8.2f} | PnL {s.pnl_points:+7.2f} | "
            f"{s.holding_candles} candles"
        )

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
        "vwap_sl_buffer_pct": strategy_config.VWAP_SL_BUFFER_PCT,
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
            }
            for s in signals
        ],
    }

    def _serialize(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, pd.Timestamp):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    with open(run_dir / "phase2_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=_serialize)

    logger.info(f"\nResults saved to {run_dir}")
    elapsed = time.time() - start
    logger.info(f"Phase 2 completed in {elapsed:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
