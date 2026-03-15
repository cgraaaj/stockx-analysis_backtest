"""
Grade A | 10% SL / 30% Target | 10L Capital (compounding)
==========================================================
Runs the best-performing strategy with real capital tracking.
Outputs a CSV trade log with full details.

Usage:
  python -m src.backtest.2025.run_gradeA_10l [results_dir]
"""

import csv
import json
import math
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# analysis_backtest root so that "from src.analysis" / "from src.backtest" resolve
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

import pandas as pd

from src.backtest import tickerflow_client as tf
from src.backtest.run_phase3_fno import (
    find_results_dir,
    load_phase2_trades,
    pick_expiry_for_date,
    compute_rsi,
    get_premium_rsi_at_signal,
)

# ── Config ────────────────────────────────────────────────────────────
STARTING_CAPITAL = 10_00_000          # 10 Lakhs
MAX_CAPITAL_PER_TRADE = 2_00_000      # Max 2L per trade
SL_PCT = 10.0                         # 10% SL on premium
TARGET_PCT = 30.0                     # 30% target on premium (1:3 R:R)
ACCEPTED_GRADES = ["A", "B"]
RSI_OVERBOUGHT = 80.0
ENTRY_OFFSET = 5                     # 5-min buffer for signal processing
MAX_EXIT_TIME = "15:15:00"
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


def main():
    import pickle as _pkl

    start = time.time()
    run_dir = find_results_dir()
    print(f"Results dir: {run_dir}")

    cache_path = run_dir / "prepared_gradeAB_cache.pkl"

    if cache_path.exists():
        print(f"Loading cached data from {cache_path.name}...")
        with open(cache_path, "rb") as f:
            prepared = _pkl.load(f)
        print(f"Grade A+B trades: {len(prepared)} (from cache)")
    else:
        phase2_trades = load_phase2_trades(run_dir)
        if not phase2_trades:
            print("ERROR: No Phase 2 trades found. Run Phase 2 first.")
            return
        expiry_dates = tf.get_expiries()

        # ── Fetch + filter ────────────────────────────────────────────
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
                "market_trend": trade.get("market_trend", ""),
                "trend_alignment": trade.get("trend_alignment", ""),
            })

        prepared.sort(key=lambda t: t["signal_time"])
        print(f"Grade A+B trades: {len(prepared)} (skipped {skipped})")

        # Save cache
        with open(cache_path, "wb") as f:
            _pkl.dump(prepared, f)
        print(f"Cached to {cache_path.name}")

    # ── Concurrent capital simulation ─────────────────────────────────
    # Tracks active (open) positions by entry/exit time.
    # On each new entry: settle positions whose exit_time <= entry_time,
    # then capital_before = settled_capital - locked_in_active_trades.
    # capital_after = cumulative running total (STARTING + all realised PnL).
    settled_capital = float(STARTING_CAPITAL)
    cumulative_pnl = 0.0
    active_positions = []
    rows = []

    for t in prepared:
        if t["rsi"] is not None and t["rsi"] > RSI_OVERBOUGHT:
            continue
        sim = simulate(t["candles"], t["signal_time"], ENTRY_OFFSET,
                       SL_PCT, TARGET_PCT, MAX_EXIT_TIME)
        if sim is None:
            continue
        entry_premium = sim["entry_premium"]
        if entry_premium <= 0:
            continue
        cost_per_lot = entry_premium * t["lot_size"]
        if cost_per_lot <= 0:
            continue

        entry_time = sim["entry_time"]

        still_active = []
        for pos in active_positions:
            if pos["exit_time"] <= entry_time:
                settled_capital += pos["rupee_pnl"]
            else:
                still_active.append(pos)
        active_positions = still_active

        locked = sum(p["capital_used"] for p in active_positions)
        available = settled_capital - locked

        if available <= 0:
            print(f"  SKIP {t['trade_date']} | {t['stock']:15s} | "
                  f"No capital (settled Rs {settled_capital:,.0f}, locked Rs {locked:,.0f})")
            continue

        deploy = min(available, MAX_CAPITAL_PER_TRADE)
        num_lots = max(1, math.floor(deploy / cost_per_lot))
        capital_used = num_lots * cost_per_lot

        pnl_per_unit = sim["exit_premium"] - entry_premium
        rupee_pnl = pnl_per_unit * t["lot_size"] * num_lots
        pnl_pct = (pnl_per_unit / entry_premium) * 100

        cumulative_pnl += rupee_pnl
        capital_after = float(STARTING_CAPITAL) + cumulative_pnl

        active_positions.append({
            "exit_time": sim["exit_time"],
            "capital_used": capital_used,
            "rupee_pnl": rupee_pnl,
        })

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
        print(
            f"  #{row['trade_no']:>2} {row['trade_date']} | {row['stock']:15s} | "
            f"{row['strike_price']:>8} {row['option_type']} | "
            f"Prem {entry_premium:>7.2f} → {sim['exit_premium']:>7.2f} | "
            f"{tag:4s} | Lots {num_lots:>3} x {t['lot_size']} | "
            f"Rs {rupee_pnl:>+12,.0f} | Avail Rs {available:>12,.0f} | "
            f"Total Rs {capital_after:>12,.0f}"
        )

    # ── Summary ───────────────────────────────────────────────────────
    total_trades = len(rows)
    wins = sum(1 for r in rows if r["outcome"] == "target_hit")
    losses = sum(1 for r in rows if r["outcome"] == "sl_hit")
    time_exits = sum(1 for r in rows if r["outcome"] == "time_exit")
    total_pnl = sum(r["rupee_pnl"] for r in rows)
    final_capital = STARTING_CAPITAL + total_pnl

    print(f"\n{'=' * 80}")
    print(f"  GRADE A+B | 10% SL / 30% Target | 10L Capital | Max {MAX_CAPITAL_PER_TRADE//100000}L/trade | 5min buffer")
    print(f"{'=' * 80}")
    print(f"  Starting Capital : Rs {STARTING_CAPITAL:>12,}")
    print(f"  Final Capital    : Rs {final_capital:>12,.0f}")
    print(f"  Total P&L        : Rs {total_pnl:>+12,.0f}")
    print(f"  ROI              : {(total_pnl / STARTING_CAPITAL) * 100:.2f}%")
    print(f"  Trades           : {total_trades} (W:{wins}  L:{losses}  T:{time_exits})")
    print(f"  Win Rate         : {wins / total_trades * 100:.1f}%" if total_trades else "")
    print(f"{'=' * 80}")

    # ── Write CSV ─────────────────────────────────────────────────────
    max_l = MAX_CAPITAL_PER_TRADE // 100000
    csv_path = run_dir / f"gradeAB_10l_max{max_l}l_{ENTRY_OFFSET}minbuf_trades.csv"
    if rows:
        fieldnames = list(rows[0].keys())
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nCSV saved to: {csv_path}")

    # ── Write JSON summary ────────────────────────────────────────────
    summary = {
        "strategy": "Grade A | 10% SL / 30% Target",
        "starting_capital": STARTING_CAPITAL,
        "final_capital": round(final_capital, 2),
        "total_pnl": round(total_pnl, 2),
        "roi_pct": round((total_pnl / STARTING_CAPITAL) * 100, 2),
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "time_exits": time_exits,
        "win_rate_pct": round(wins / total_trades * 100, 1) if total_trades else 0,
        "trades": rows,
    }
    def _default(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    json_path = run_dir / f"gradeAB_10l_max{max_l}l_{ENTRY_OFFSET}minbuf_summary.json"
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2, default=_default)

    elapsed = time.time() - start
    print(f"Completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
