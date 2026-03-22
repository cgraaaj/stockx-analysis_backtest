"""Phase 3b: Grade A+B Capital Simulation (compounding).

Filters Phase 2 trades to accepted grades, applies RSI filter,
then simulates with a compounding portfolio (concurrent position tracking).

Optimised: pre-fetches all instrument + tick data concurrently before
the sequential capital simulation loop.
"""

import asyncio
import csv
import json
import logging
import math
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.backtest.config.schema import BacktestConfig
from src.backtest.core.analysis import pick_expiry
from src.backtest.core.capital import PortfolioTracker, compute_lot_count
from src.backtest.core.simulation import (
    get_premium_rsi_at_signal,
    simulate_option_trade,
)
from src.backtest.data import tickerflow as tf

logger = logging.getLogger("Phase3b-Capital")


def _load_phase2_trades(run_dir: Path) -> list[dict]:
    p2 = run_dir / "phase2_summary.json"
    if not p2.exists():
        logger.error(f"phase2_summary.json not found in {run_dir}. Run Phase 2 first.")
        return []
    with open(p2) as f:
        data = json.load(f)
    return data["trades"]


async def _prefetch_grade_data(
    trades: list[dict],
    accepted_grades: list[str],
    expiry_dates: list[str],
) -> tuple[list[dict], int]:
    """Bulk-fetch ATM instruments, then fetch ticks concurrently.

    Returns (prepared_list, skipped_count).
    """
    # --- Step 1: Grade-filter and group by expiry for bulk ATM lookup ---
    eligible: list[tuple[int, dict]] = []
    for idx, trade in enumerate(trades):
        if trade.get("grade", "D") in accepted_grades:
            eligible.append((idx, trade))

    expiry_groups: dict[str, list[dict]] = {}
    trade_meta: dict[int, dict] = {}
    for idx, trade in eligible:
        signal_time = datetime.strptime(trade["signal_time"], "%Y-%m-%d %H:%M:%S")
        trade_date = signal_time.strftime("%Y-%m-%d")
        expiry = pick_expiry(trade_date, expiry_dates)
        expiry_groups.setdefault(expiry, []).append({
            "idx": idx,
            "stock_name": trade["stock"],
            "option_type": trade["option_type"],
            "spot_price": trade["entry_price"],
        })
        trade_meta[idx] = {
            "trade": trade, "signal_time": signal_time,
            "trade_date": trade_date, "expiry": expiry,
        }

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

    logger.info(f"  Bulk ATM resolved {len(inst_map)}/{len(eligible)} instruments")

    # --- Step 2: Fetch tick data concurrently for resolved instruments ---
    async def _fetch_ticks(idx: int):
        meta = trade_meta[idx]
        inst = inst_map[idx]
        td = meta["trade_date"]
        candles = await tf.aget_ticks(
            instrument_id=inst["instrument_seq"],
            start=f"{td}T09:15:00", end=f"{td}T15:30:00",
        )
        if candles.empty:
            return idx, None
        trade = meta["trade"]
        rsi_val = get_premium_rsi_at_signal(candles, meta["signal_time"], 14)
        return idx, {
            "stock": trade["stock"],
            "option_type": trade["option_type"],
            "trade_date": td,
            "signal_time": meta["signal_time"],
            "spot_price": trade["entry_price"],
            "lot_size": inst["lot_size"],
            "strike_price": inst["strike_price"],
            "trading_symbol": inst["trading_symbol"],
            "expiry": meta["expiry"],
            "candles": candles,
            "rsi": rsi_val,
            "grade": trade.get("grade", "D"),
            "market_trend": trade.get("market_trend", ""),
            "trend_alignment": trade.get("trend_alignment", ""),
        }

    tick_results = await asyncio.gather(
        *[_fetch_ticks(idx) for idx in inst_map], return_exceptions=True,
    )

    prepared = []
    skipped = 0
    for r in tick_results:
        if isinstance(r, Exception):
            logger.warning(f"  Prefetch error: {r}")
            skipped += 1
            continue
        idx, data = r
        if data is None:
            skipped += 1
        else:
            prepared.append(data)

    skipped += len(eligible) - len(inst_map)
    prepared.sort(key=lambda t: t["signal_time"])
    return prepared, skipped


async def run(config: BacktestConfig, run_dir: Path, *, phase2_trades: list[dict] | None = None):
    """Execute Phase 3b and return the trade rows list.

    If *phase2_trades* is provided (in-memory from pipeline), use it
    directly instead of loading from disk.
    """
    start = time.time()

    if phase2_trades is None:
        phase2_trades = _load_phase2_trades(run_dir)
    if not phase2_trades:
        logger.error("No Phase 2 trades found. Cannot run Phase 3b.")
        return []

    accepted_grades = config.accepted_grades
    sl_pct = config.grade_sl_pct
    target_pct = config.grade_target_pct
    entry_offset = config.grade_entry_offset
    rsi_overbought = config.grade_rsi_overbought
    max_exit_time = config.max_exit_time
    starting_capital = config.starting_capital
    max_per_trade = config.max_capital_per_trade

    logger.info("=" * 60)
    logger.info("Phase 3b: Grade A+B Capital Simulation")
    logger.info("=" * 60)
    logger.info(f"Accepted grades = {accepted_grades}")
    logger.info(f"Starting capital= Rs {starting_capital:,}")
    logger.info(f"Max per trade   = Rs {max_per_trade:,}")
    logger.info(f"SL / Target     = {sl_pct}% / {target_pct}%")
    logger.info(f"Entry offset    = {entry_offset} candles")

    expiry_dates = await tf.aget_expiries()

    logger.info("Prefetching instrument + tick data concurrently...")
    prefetch_start = time.time()
    prepared, skipped = await _prefetch_grade_data(
        phase2_trades, accepted_grades, expiry_dates,
    )
    logger.info(
        f"Prefetch done: {len(prepared)} trades, skipped {skipped} "
        f"in {time.time() - prefetch_start:.1f}s"
    )
    logger.info(f"Grade {accepted_grades} trades: {len(prepared)} (skipped {skipped})")

    # -- Compounding capital simulation --
    portfolio = PortfolioTracker(starting_capital, max_per_trade)
    rows: list[dict] = []

    for t in prepared:
        if t["rsi"] is not None and t["rsi"] > rsi_overbought:
            continue

        sim = simulate_option_trade(
            t["candles"], t["signal_time"], entry_offset,
            sl_pct, target_pct, max_exit_time,
        )
        if sim is None:
            continue
        entry_premium = sim["entry_premium"]
        if entry_premium <= 0:
            continue
        cost_per_lot = entry_premium * t["lot_size"]
        if cost_per_lot <= 0:
            continue

        entry_time = sim["entry_time"]
        portfolio.settle_closed(entry_time)

        available = portfolio.available_capital
        if available <= 0:
            logger.info(
                f"  SKIP {t['trade_date']} | {t['stock']:15s} | "
                f"No capital (avail Rs {available:,.0f})"
            )
            continue

        num_lots = compute_lot_count(entry_premium, t["lot_size"], available, max_per_trade)
        capital_used = num_lots * cost_per_lot

        pnl_per_unit = sim["exit_premium"] - entry_premium
        rupee_pnl = pnl_per_unit * t["lot_size"] * num_lots
        pnl_pct = (pnl_per_unit / entry_premium) * 100

        portfolio.open_position(capital_used, rupee_pnl, sim["exit_time"])
        capital_after = portfolio.total_capital

        opt_label = "CE" if t["option_type"] == "call" else "PE"

        row = {
            "trade_no": len(rows) + 1,
            "trade_date": t["trade_date"],
            "stock": t["stock"],
            "direction": t["option_type"].upper(),
            "grade": t["grade"],
            "spot_price": round(t["spot_price"], 2),
            "strike_price": int(t["strike_price"]),
            "option_type": opt_label,
            "expiry": t["expiry"],
            "trading_symbol": t["trading_symbol"],
            "lot_size": t["lot_size"],
            "num_lots": num_lots,
            "entry_premium": round(entry_premium, 2),
            "sl_premium": round(sim["sl_premium"], 2),
            "target_premium": round(sim["target_premium"], 2),
            "exit_premium": round(sim["exit_premium"], 2),
            "buy_time": str(sim["entry_time"]),
            "sell_time": str(sim["exit_time"]),
            "holding_candles": sim["holding_candles"],
            "outcome": sim["outcome"],
            "pnl_per_unit": round(pnl_per_unit, 2),
            "pnl_pct": round(pnl_pct, 2),
            "rupee_pnl": round(rupee_pnl, 2),
            "capital_before": round(available, 2),
            "capital_used": round(capital_used, 2),
            "market_trend": t.get("market_trend", ""),
            "trend_alignment": t.get("trend_alignment", ""),
            "capital_after": round(capital_after, 2),
        }
        rows.append(row)

        tag = "WIN" if sim["outcome"] == "target_hit" else ("LOSS" if sim["outcome"] == "sl_hit" else "TIME")
        logger.info(
            f"  #{row['trade_no']:>2} {row['trade_date']} | {row['stock']:15s} | "
            f"{row['strike_price']:>8} {opt_label} | "
            f"Prem {entry_premium:>7.2f} -> {sim['exit_premium']:>7.2f} | "
            f"{tag:4s} | Lots {num_lots:>3} x {t['lot_size']} | "
            f"Rs {rupee_pnl:>+12,.0f} | Total Rs {capital_after:>12,.0f}"
        )

    # -- Summary --
    total_trades = len(rows)
    wins = sum(1 for r in rows if r["outcome"] == "target_hit")
    losses = sum(1 for r in rows if r["outcome"] == "sl_hit")
    time_exits = sum(1 for r in rows if r["outcome"] == "time_exit")
    total_pnl = sum(r["rupee_pnl"] for r in rows)
    final_capital = starting_capital + total_pnl

    logger.info("=" * 60)
    logger.info(f"  GRADE {accepted_grades} | {sl_pct}% SL / {target_pct}% Target | {starting_capital / 1_00_000:.0f}L Capital")
    logger.info(f"  Starting Capital : Rs {starting_capital:>12,}")
    logger.info(f"  Final Capital    : Rs {final_capital:>12,.0f}")
    logger.info(f"  Total P&L        : Rs {total_pnl:>+12,.0f}")
    logger.info(f"  ROI              : {(total_pnl / starting_capital) * 100:.2f}%")
    logger.info(f"  Trades           : {total_trades} (W:{wins}  L:{losses}  T:{time_exits})")
    if total_trades:
        logger.info(f"  Win Rate         : {wins / total_trades * 100:.1f}%")
    logger.info("=" * 60)

    # -- Write CSV --
    max_l = max_per_trade // 100000
    csv_path = run_dir / f"gradeAB_{starting_capital // 1_00_000}l_max{max_l}l_{entry_offset}minbuf_trades.csv"
    if rows:
        fieldnames = list(rows[0].keys())
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"CSV saved to: {csv_path}")

    # -- Write JSON summary --
    def _default(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    summary = {
        "strategy": f"Grade {accepted_grades} | {sl_pct}% SL / {target_pct}% Target",
        "starting_capital": starting_capital,
        "final_capital": round(final_capital, 2),
        "total_pnl": round(total_pnl, 2),
        "roi_pct": round((total_pnl / starting_capital) * 100, 2),
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "time_exits": time_exits,
        "win_rate_pct": round(wins / total_trades * 100, 1) if total_trades else 0,
        "trades": rows,
    }

    json_path = run_dir / f"gradeAB_{starting_capital // 1_00_000}l_max{max_l}l_{entry_offset}minbuf_summary.json"
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2, default=_default)

    elapsed = time.time() - start
    logger.info(f"Phase 3b completed in {elapsed:.1f}s")

    return rows
