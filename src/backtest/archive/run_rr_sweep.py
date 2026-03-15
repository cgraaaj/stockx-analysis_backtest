"""
R:R Sweep: Grade A+B | 10% SL | 10L Capital | Max 5L/trade
============================================================
Tests reward ratios from 1:2 to 1:5 (in 0.5 steps).

Usage:
  python -m src.backtest.run_rr_sweep [results_dir]
"""

import csv
import json
import math
import os
import sys
import time
from datetime import datetime
from itertools import groupby
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

import pandas as pd

from src.backtest import tickerflow_client as tf
from src.backtest.run_phase3_fno import (
    find_results_dir,
    load_phase2_trades,
    pick_expiry_for_date,
    get_premium_rsi_at_signal,
)

# ── Config ────────────────────────────────────────────────────────────
STARTING_CAPITAL = 10_00_000
MAX_CAPITAL_PER_TRADE = 5_00_000
SL_PCT = 10.0
ACCEPTED_GRADES = ["A", "B"]
RSI_OVERBOUGHT = 80.0
ENTRY_OFFSET = 1
MAX_EXIT_TIME = "15:15:00"

RR_RATIOS = [2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
# ──────────────────────────────────────────────────────────────────────


def simulate(candles, signal_time, entry_offset, sl_pct, target_pct, max_exit_time):
    max_exit_ts = datetime.strptime(
        f"{signal_time.strftime('%Y-%m-%d')} {max_exit_time}", "%Y-%m-%d %H:%M:%S"
    )
    signal_idx = candles[candles["time_stamp"] <= signal_time].index
    if signal_idx.empty:
        return None
    entry_idx = signal_idx[-1] + entry_offset
    if entry_idx >= len(candles):
        return None

    entry_row = candles.iloc[entry_idx]
    entry_premium = float(entry_row["open"])
    entry_time = entry_row["time_stamp"]
    if entry_premium <= 0:
        return None

    sl_premium = entry_premium * (1 - sl_pct / 100.0)
    target_premium = entry_premium * (1 + target_pct / 100.0)

    for i in range(entry_idx + 1, len(candles)):
        row = candles.iloc[i]
        ts = row["time_stamp"]
        high, low, close = float(row["high"]), float(row["low"]), float(row["close"])

        if ts >= max_exit_ts:
            return dict(outcome="time_exit", entry_premium=entry_premium,
                        exit_premium=close, entry_time=entry_time, exit_time=ts,
                        sl_premium=sl_premium, target_premium=target_premium,
                        holding_candles=i - entry_idx)
        if low <= sl_premium:
            return dict(outcome="sl_hit", entry_premium=entry_premium,
                        exit_premium=sl_premium, entry_time=entry_time, exit_time=ts,
                        sl_premium=sl_premium, target_premium=target_premium,
                        holding_candles=i - entry_idx)
        if high >= target_premium:
            return dict(outcome="target_hit", entry_premium=entry_premium,
                        exit_premium=target_premium, entry_time=entry_time, exit_time=ts,
                        sl_premium=sl_premium, target_premium=target_premium,
                        holding_candles=i - entry_idx)

    last = candles.iloc[-1]
    return dict(outcome="time_exit", entry_premium=entry_premium,
                exit_premium=float(last["close"]), entry_time=entry_time,
                exit_time=last["time_stamp"], sl_premium=sl_premium,
                target_premium=target_premium,
                holding_candles=len(candles) - 1 - entry_idx)


def run_with_rr(prepared, rr_ratio):
    """Run full simulation for a given R:R ratio, return summary + trade rows."""
    target_pct = SL_PCT * rr_ratio
    capital = float(STARTING_CAPITAL)
    rows = []

    # Pre-simulate
    trade_queue = []
    for t in prepared:
        if t["rsi"] is not None and t["rsi"] > RSI_OVERBOUGHT:
            continue
        sim = simulate(t["candles"], t["signal_time"], ENTRY_OFFSET,
                       SL_PCT, target_pct, MAX_EXIT_TIME)
        if sim is None:
            continue
        entry_premium = sim["entry_premium"]
        if entry_premium <= 0:
            continue
        cost_per_lot = entry_premium * t["lot_size"]
        if cost_per_lot <= 0:
            continue
        trade_queue.append({"trade": t, "sim": sim, "cost_per_lot": cost_per_lot})

    # Same-day capital management
    for _, day_group in groupby(trade_queue, key=lambda x: x["trade"]["trade_date"]):
        day_trades = list(day_group)
        day_capital_pool = capital
        day_locked = 0.0
        day_results = []

        for item in day_trades:
            t = item["trade"]
            sim = item["sim"]
            cost_per_lot = item["cost_per_lot"]
            entry_premium = sim["entry_premium"]
            lot_size = t["lot_size"]

            available = day_capital_pool - day_locked
            if available <= 0:
                continue

            deploy = min(available, MAX_CAPITAL_PER_TRADE)
            num_lots = max(1, math.floor(deploy / cost_per_lot))
            capital_used = num_lots * cost_per_lot
            day_locked += capital_used

            pnl_per_unit = sim["exit_premium"] - entry_premium
            rupee_pnl = pnl_per_unit * lot_size * num_lots
            pnl_pct = (pnl_per_unit / entry_premium) * 100

            opt_label = "CE" if t["option_type"] == "call" else "PE"

            row = {
                "trade_no": len(rows) + len(day_results) + 1,
                "trade_date": t["trade_date"],
                "stock": t["stock"],
                "direction": t["option_type"].upper(),
                "grade": t["grade"],
                "spot_price": round(t["spot_price"], 2),
                "strike_price": int(t["strike_price"]),
                "option_type": opt_label,
                "expiry": t["expiry"],
                "trading_symbol": t["trading_symbol"],
                "lot_size": lot_size,
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
                "capital_before": round(day_capital_pool, 2),
                "capital_used": round(capital_used, 2),
            }
            day_results.append((row, rupee_pnl))

        for row, rupee_pnl in day_results:
            capital += rupee_pnl
            row["capital_after"] = round(capital, 2)
            rows.append(row)

    total_trades = len(rows)
    wins = sum(1 for r in rows if r["outcome"] == "target_hit")
    losses = sum(1 for r in rows if r["outcome"] == "sl_hit")
    time_exits = sum(1 for r in rows if r["outcome"] == "time_exit")
    total_pnl = sum(r["rupee_pnl"] for r in rows)

    return {
        "rr_ratio": rr_ratio,
        "sl_pct": SL_PCT,
        "target_pct": target_pct,
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "time_exits": time_exits,
        "win_rate": round(wins / total_trades * 100, 1) if total_trades else 0,
        "total_pnl": round(total_pnl, 0),
        "roi_pct": round((total_pnl / STARTING_CAPITAL) * 100, 2),
        "final_capital": round(STARTING_CAPITAL + total_pnl, 0),
        "avg_pnl": round(total_pnl / total_trades, 0) if total_trades else 0,
    }, rows


def main():
    start = time.time()
    run_dir = find_results_dir()
    print(f"Results dir: {run_dir}")

    phase2_trades = load_phase2_trades(run_dir)
    expiry_dates = tf.get_expiries()

    # Fetch data once
    print("Fetching data via TickerFlow API...")
    prepared = []
    skipped = 0

    for trade in phase2_trades:
        grade = trade.get("grade", "D")
        if grade not in ACCEPTED_GRADES:
            continue

        stock = trade["stock"]
        opt_type = trade["option_type"]
        signal_time = datetime.strptime(trade["signal_time"], "%Y-%m-%d %H:%M:%S")
        trade_date = signal_time.strftime("%Y-%m-%d")
        spot_price = trade["entry_price"]
        expiry = pick_expiry_for_date(trade_date, expiry_dates)

        inst = tf.find_atm_instrument(stock, opt_type, spot_price, expiry)
        if inst is None:
            skipped += 1
            continue

        candles = tf.get_ticks(
            instrument_id=inst["instrument_seq"],
            start=f"{trade_date}T09:15:00",
            end=f"{trade_date}T15:30:00",
        )
        if candles.empty:
            skipped += 1
            continue

        rsi_val = get_premium_rsi_at_signal(candles, signal_time, 14)

        prepared.append({
            "stock": stock,
            "option_type": opt_type,
            "trade_date": trade_date,
            "signal_time": signal_time,
            "spot_price": spot_price,
            "lot_size": inst["lot_size"],
            "strike_price": inst["strike_price"],
            "trading_symbol": inst["trading_symbol"],
            "expiry": expiry,
            "candles": candles,
            "rsi": rsi_val,
            "grade": grade,
        })

    prepared.sort(key=lambda t: t["signal_time"])
    print(f"Grade A+B trades: {len(prepared)} ({skipped} skipped)")

    # Run each R:R ratio
    print(f"\nRunning R:R sweep: {RR_RATIOS}")
    all_summaries = []
    best_rows = None
    best_roi = -999

    for rr in RR_RATIOS:
        summary, rows = run_with_rr(prepared, rr)
        all_summaries.append(summary)
        if summary["roi_pct"] > best_roi:
            best_roi = summary["roi_pct"]
            best_rows = rows
            best_rr = rr

    # Print comparison table
    print(f"\n{'=' * 120}")
    print(f"  R:R SWEEP | Grade A+B | 10% SL | 10L Capital | Max 5L/trade")
    print(f"{'=' * 120}")
    hdr = (
        f"{'R:R':>5} │ {'SL%':>4} {'TGT%':>5} │ "
        f"{'Trades':>6} {'W':>3} {'L':>3} {'T':>3} │ "
        f"{'WinRate':>7} │ {'Total PnL':>12} {'Avg PnL':>10} │ "
        f"{'ROI%':>8} │ {'Final Capital':>14}"
    )
    sep = "─" * len(hdr)
    print(sep)
    print(hdr)
    print(sep)

    for s in all_summaries:
        marker = " ★ BEST" if s["rr_ratio"] == best_rr else ""
        print(
            f" 1:{s['rr_ratio']:<3.1f} │ {s['sl_pct']:>4.0f} {s['target_pct']:>5.0f} │ "
            f"{s['total_trades']:>6} {s['wins']:>3} {s['losses']:>3} {s['time_exits']:>3} │ "
            f"{s['win_rate']:>6.1f}% │ "
            f"{s['total_pnl']:>+12,.0f} {s['avg_pnl']:>+10,.0f} │ "
            f"{s['roi_pct']:>7.2f}% │ "
            f"Rs {s['final_capital']:>12,.0f}{marker}"
        )
    print(sep)

    print(f"\n  >>> BEST R:R = 1:{best_rr:.1f} (SL {SL_PCT}% / Target {SL_PCT * best_rr:.0f}%) → ROI {best_roi:.2f}%")

    # Save best CSV
    csv_path = run_dir / "gradeAB_rr_sweep_best_trades.csv"
    if best_rows:
        fieldnames = list(best_rows[0].keys())
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(best_rows)
        print(f"\nBest R:R trades CSV: {csv_path}")

    # Save all CSVs per R:R
    for rr in RR_RATIOS:
        _, rows = run_with_rr(prepared, rr)
        rr_csv = run_dir / f"gradeAB_rr{rr:.1f}_trades.csv"
        if rows:
            with open(rr_csv, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)

    # Save summary JSON
    def _default(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    json_path = run_dir / "gradeAB_rr_sweep_summary.json"
    with open(json_path, "w") as f:
        json.dump({
            "config": {"sl_pct": SL_PCT, "grades": ACCEPTED_GRADES,
                       "capital": STARTING_CAPITAL, "max_per_trade": MAX_CAPITAL_PER_TRADE},
            "results": all_summaries,
            "best_rr": best_rr,
            "best_roi": best_roi,
        }, f, indent=2, default=_default)

    elapsed = time.time() - start
    print(f"\nCompleted in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
