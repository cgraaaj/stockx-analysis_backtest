"""Phase 3a: F&O Flat Per-Trade Validation.

Simulates option buying (CE for calls, PE for puts) with fixed
capital per trade.  Uses ATM strike premiums via TickerFlow API.

Optimised: pre-fetches all instrument + tick data concurrently before
running the sequential simulation loop.
"""

import asyncio
import json
import logging
import math
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.backtest.config.schema import BacktestConfig
from src.backtest.core.analysis import pick_expiry
from src.backtest.core.simulation import (
    get_premium_rsi_at_signal,
    simulate_option_trade,
    simulate_staged_entry_trade,
)
from src.backtest.data import tickerflow as tf

logger = logging.getLogger("Phase3a-FnO")


def _load_phase2_trades(run_dir: Path) -> list[dict]:
    p2 = run_dir / "phase2_summary.json"
    if not p2.exists():
        logger.error(f"phase2_summary.json not found in {run_dir}. Run Phase 2 first.")
        return []
    with open(p2) as f:
        data = json.load(f)
    return data["trades"]


async def _prefetch_trade_data(
    trades: list[dict], expiry_dates: list[str],
) -> dict[int, dict]:
    """Bulk-fetch ATM instruments in one call, then fetch ticks concurrently.

    Returns {trade_index: {"inst": ..., "candles": DataFrame}} for trades
    that have valid data.
    """
    # --- Step 1: Group trades by expiry and do one bulk ATM call per group ---
    expiry_groups: dict[str, list[dict]] = {}
    for idx, trade in enumerate(trades):
        signal_time = datetime.strptime(trade["signal_time"], "%Y-%m-%d %H:%M:%S")
        trade_date = signal_time.strftime("%Y-%m-%d")
        expiry = pick_expiry(trade_date, expiry_dates)
        expiry_groups.setdefault(expiry, []).append({
            "idx": idx,
            "stock_name": trade["stock"],
            "option_type": trade["option_type"],
            "spot_price": trade["entry_price"],
        })

    bulk_tasks = [
        tf.aget_atm_instruments_bulk(reqs, expiry=exp)
        for exp, reqs in expiry_groups.items()
    ]
    bulk_results = await asyncio.gather(*bulk_tasks, return_exceptions=True)

    inst_map: dict[int, dict] = {}
    for res in bulk_results:
        if isinstance(res, Exception):
            logger.warning(f"  Bulk ATM error: {res}")
            continue
        inst_map.update(res)

    logger.info(f"  Bulk ATM resolved {len(inst_map)}/{len(trades)} instruments")

    # --- Step 2: Fetch tick data concurrently for resolved instruments ---
    async def _fetch_ticks(idx: int, inst: dict, trade: dict):
        signal_time = datetime.strptime(trade["signal_time"], "%Y-%m-%d %H:%M:%S")
        trade_date = signal_time.strftime("%Y-%m-%d")
        candles = await tf.aget_ticks(
            instrument_id=inst["instrument_seq"],
            start=f"{trade_date}T09:15:00",
            end=f"{trade_date}T15:30:00",
        )
        if candles.empty:
            return idx, None
        return idx, {"inst": inst, "candles": candles}

    tick_tasks = [
        _fetch_ticks(idx, inst_map[idx], trades[idx])
        for idx in inst_map
    ]
    tick_results = await asyncio.gather(*tick_tasks, return_exceptions=True)

    data_map: dict[int, dict] = {}
    for item in tick_results:
        if isinstance(item, Exception):
            logger.warning(f"  Tick prefetch error: {item}")
            continue
        idx, data = item
        if data is not None:
            data_map[idx] = data
    return data_map


async def run(config: BacktestConfig, run_dir: Path, *, phase2_trades: list[dict] | None = None):
    """Execute Phase 3a and return the results list.

    If *phase2_trades* is provided (in-memory from pipeline), use it
    directly instead of loading from disk.
    """
    start = time.time()

    if phase2_trades is None:
        phase2_trades = _load_phase2_trades(run_dir)
    if not phase2_trades:
        logger.error("No Phase 2 trades found. Cannot run Phase 3a.")
        return []

    entry_mode = config.fno_entry_mode
    sl_pct = config.fno_sl_pct
    target_pct = config.fno_target_pct
    capital_per_trade = config.fno_capital_per_trade
    entry_offset = config.fno_entry_candle_offset
    max_exit_time = config.max_exit_time
    use_rsi = config.fno_use_rsi_filter
    rsi_overbought = config.fno_rsi_overbought

    logger.info("=" * 60)
    logger.info("Phase 3a: F&O Flat Per-Trade Validation")
    logger.info("=" * 60)
    logger.info(f"Capital/trade   = Rs {capital_per_trade:,}")
    logger.info(f"SL on premium   = {sl_pct}%")
    logger.info(f"Target premium  = {target_pct}%")
    logger.info(f"Entry mode      = {entry_mode}")
    logger.info(f"Trades loaded   = {len(phase2_trades)}")

    expiry_dates = await tf.aget_expiries()

    logger.info("Prefetching instrument + tick data concurrently...")
    prefetch_start = time.time()
    trade_data = await _prefetch_trade_data(phase2_trades, expiry_dates)
    logger.info(
        f"Prefetch done: {len(trade_data)}/{len(phase2_trades)} trades "
        f"in {time.time() - prefetch_start:.1f}s"
    )

    results = []
    skipped = 0
    total_capital_deployed = 0

    for trade_idx, trade in enumerate(phase2_trades):
        stock = trade["stock"]
        opt_type = trade["option_type"]
        signal_time_str = trade["signal_time"]
        signal_time = datetime.strptime(signal_time_str, "%Y-%m-%d %H:%M:%S")
        trade_date = signal_time.strftime("%Y-%m-%d")
        spot_price = trade["entry_price"]

        fetched = trade_data.get(trade_idx)
        if fetched is None:
            skipped += 1
            continue
        inst = fetched["inst"]
        candles = fetched["candles"]

        premium_rsi = get_premium_rsi_at_signal(candles, signal_time, 14)

        if entry_mode == "staged":
            sim = simulate_staged_entry_trade(
                candles, signal_time,
                sl_pct=sl_pct, target_pct=target_pct,
                max_exit_time=max_exit_time,
            )
            if sim is None or sim.get("outcome") == "no_entry":
                skipped += 1
                continue
            entry_premium = sim["entry_premium"]
        else:
            if use_rsi and premium_rsi is not None and premium_rsi > rsi_overbought:
                logger.info(f"  {stock:15s} | {opt_type:4s} | RSI {premium_rsi:.1f} > {rsi_overbought} -> SKIPPED")
                skipped += 1
                continue

            sim = simulate_option_trade(
                candles, signal_time, entry_offset,
                sl_pct, target_pct, max_exit_time,
            )
            if sim is None:
                skipped += 1
                continue

            signal_idx = candles[candles["time_stamp"] <= signal_time].index
            entry_premium = float(candles.iloc[signal_idx[-1] + entry_offset]["open"])

        lot_size = inst["lot_size"]
        cost_per_lot = entry_premium * lot_size
        if cost_per_lot <= 0:
            skipped += 1
            continue

        num_lots = max(1, math.floor(capital_per_trade / cost_per_lot))
        capital_used = num_lots * cost_per_lot

        exit_premium = sim["exit_premium"]
        pnl_per_unit = exit_premium - entry_premium
        rupee_pnl = pnl_per_unit * lot_size * num_lots
        pnl_pct = (pnl_per_unit / entry_premium) * 100 if entry_premium > 0 else 0

        total_capital_deployed += capital_used

        result = {
            "stock": stock,
            "option_type": opt_type,
            "trade_date": trade_date,
            "signal_time": signal_time_str,
            "spot_price": spot_price,
            "strike_price": inst["strike_price"],
            "trading_symbol": inst["trading_symbol"],
            "lot_size": lot_size,
            "num_lots": num_lots,
            "capital_used": round(capital_used, 2),
            "entry_premium": round(entry_premium, 2),
            "sl_premium": round(entry_premium * (1 - sl_pct / 100), 2),
            "target_premium": round(entry_premium * (1 + target_pct / 100), 2),
            "exit_premium": round(exit_premium, 2),
            "exit_time": sim["exit_time"],
            "outcome": sim["outcome"],
            "pnl_per_unit": round(pnl_per_unit, 2),
            "pnl_pct": round(pnl_pct, 2),
            "rupee_pnl": round(rupee_pnl, 2),
            "holding_candles": sim["holding_candles"],
            "grade": trade.get("grade", ""),
            "rsi_at_signal": round(premium_rsi, 1) if premium_rsi is not None else None,
            "entry_mode": entry_mode,
            "entry_reason": sim.get("entry_reason"),
            "entry_time": sim.get("entry_time", signal_time_str),
            "candles_waited": sim.get("candles_waited", 0),
        }
        results.append(result)

        logger.info(
            f"  {stock:15s} | {opt_type:4s} | {trade_date} | "
            f"Strike {inst['strike_price']:>8.0f} | "
            f"Prem {entry_premium:>7.2f} -> {exit_premium:>7.2f} | "
            f"{sim['outcome']:10s} | "
            f"Lots {num_lots} x {lot_size} | "
            f"Rs {rupee_pnl:>+10,.0f}"
        )

    wins = [r for r in results if r["outcome"] == "target_hit"]
    losses = [r for r in results if r["outcome"] == "sl_hit"]
    time_exits = [r for r in results if r["outcome"] == "time_exit"]
    total_pnl = sum(r["rupee_pnl"] for r in results)
    win_rate = (len(wins) / len(results) * 100) if results else 0

    logger.info("=" * 60)
    logger.info("PHASE 3a F&O RESULTS")
    logger.info(f"  Total trades    : {len(results)}")
    logger.info(f"  Skipped         : {skipped}")
    logger.info(f"  Win rate        : {win_rate:.1f}%")
    logger.info(f"  Total PnL       : Rs {total_pnl:>+,.0f}")
    logger.info(f"  Capital deployed: Rs {total_capital_deployed:>,.0f}")
    logger.info("=" * 60)

    def _serialize(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    summary = {
        "config": {
            "capital_per_trade": capital_per_trade,
            "sl_pct": sl_pct,
            "target_pct": target_pct,
            "entry_mode": entry_mode,
        },
        "total_trades": len(results),
        "skipped": skipped,
        "wins": len(wins),
        "losses": len(losses),
        "time_exits": len(time_exits),
        "win_rate_pct": round(win_rate, 1),
        "total_rupee_pnl": round(total_pnl, 2),
        "avg_pnl_per_trade": round(total_pnl / len(results), 2) if results else 0,
        "total_capital_deployed": round(total_capital_deployed, 2),
        "trades": results,
    }

    with open(run_dir / "phase3_fno_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=_serialize)

    logger.info(f"Phase 3a completed in {time.time() - start:.1f}s")
    return results
