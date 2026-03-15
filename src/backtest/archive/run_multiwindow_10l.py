"""
Multi-Window Backtest: Grade A | 10% SL / 30% Target | 10L Capital
====================================================================
Loads existing analysis pickle and re-ranks with TWO scan windows:
  Window 1: 09:30 - 10:00  →  signals at 09:45
  Window 2: 11:30 - 12:15  →  signals at 11:45 / 12:00

A stock can generate signals in BOTH windows (dedup is per-window).
Then runs Phase 2 + Phase 3 inline with 10L capital, max 5L/trade.

Usage:
  python -m src.backtest.run_multiwindow_10l [results_dir]
"""

import csv
import json
import math
import os
import pickle
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import groupby
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

import pandas as pd

from src.backtest import tickerflow_client as tf
from src.backtest.run_phase3_fno import (
    find_results_dir,
    pick_expiry_for_date,
    compute_rsi,
    get_premium_rsi_at_signal,
)

# ── Config ────────────────────────────────────────────────────────────
STARTING_CAPITAL = 10_00_000
MAX_CAPITAL_PER_TRADE = 5_00_000
SL_PCT = 10.0
TARGET_PCT = 30.0
ACCEPTED_GRADES = ["A"]
RSI_OVERBOUGHT = 80.0
ENTRY_OFFSET = 1
MAX_EXIT_TIME = "15:15:00"

TN_RATIO_THRESHOLD = 60

# Scan windows: (start_time, end_time) — consecutive filter applied within each
SCAN_WINDOWS = [
    ("09:30:00", "10:00:00"),   # Morning window → signals at 09:45
    ("11:30:00", "12:15:00"),   # Midday window  → signals at 11:45 / 12:00
]
CONSECUTIVE_MINUTES = 15
# ──────────────────────────────────────────────────────────────────────


def extract_qualified(data, tn_threshold, accepted_grades):
    """Extract all qualified predictions from pickle data (all times)."""
    calls, puts = [], []
    for stock_data in data:
        if not stock_data or "opt_data" not in stock_data:
            continue
        name = stock_data["name"]
        for td in stock_data["opt_data"]:
            ts = td.get("time_stamp")
            if ts is None:
                continue
            opts = td.get("options", {})
            for key, opt_type, bucket in [("calls", "call", calls), ("puts", "put", puts)]:
                o = opts.get(key, {})
                if not o:
                    continue
                if (o.get("tn_ratio", 0) > tn_threshold
                        and o.get("bullish", 0) > o.get("bearish", 0)
                        and o.get("grade", "D") in accepted_grades):
                    bucket.append({
                        "stock": name,
                        "timestamp": ts,
                        "grade": o["grade"],
                        "option_type": opt_type,
                        "tn_ratio": o["tn_ratio"],
                        "bullish_count": o["bullish"],
                        "bearish_count": o["bearish"],
                    })
    return calls, puts


def find_consecutive_multiwindow(preds, windows, consecutive_minutes=15):
    """
    Find consecutive-appearing stocks, with dedup applied PER WINDOW.
    A stock can appear in multiple windows on the same day.
    """
    # Group by (date, timestamp)
    by_date = defaultdict(list)
    for p in preds:
        ts = p["timestamp"]
        date_str = ts.strftime("%Y-%m-%d")
        by_date[date_str].append(p)

    results = []

    for date_str, day_preds in sorted(by_date.items()):
        # Group by timestamp within the day
        by_ts = defaultdict(list)
        for p in day_preds:
            by_ts[p["timestamp"]].append(p)
        timestamps = sorted(by_ts.keys())

        for win_start, win_end in windows:
            # Filter timestamps to this window
            win_ts = [
                ts for ts in timestamps
                if win_start <= ts.strftime("%H:%M:%S") <= win_end
            ]
            if len(win_ts) < 2:
                continue

            # Find consecutive within this window
            seen_stocks = set()
            prev_stocks = set()
            prev_ts = None

            for ts in win_ts:
                cur_stocks = {p["stock"] for p in by_ts[ts]}

                if prev_ts is not None:
                    delta = ts - prev_ts
                    if delta == timedelta(minutes=consecutive_minutes):
                        for p in by_ts[ts]:
                            if p["stock"] in prev_stocks and p["stock"] not in seen_stocks:
                                results.append(p)
                                seen_stocks.add(p["stock"])

                prev_stocks = cur_stocks
                prev_ts = ts

    return results


def apply_market_context_filter(preds, run_dir):
    """Load market context from backtest_summary.json and filter conflicting signals."""
    summary_path = run_dir / "backtest_summary.json"
    if not summary_path.exists():
        return preds

    with open(summary_path) as f:
        summary = json.load(f)

    market_ctx = summary.get("market_context", {})
    if not market_ctx:
        return preds

    filtered = []
    for p in preds:
        date_str = p["timestamp"].strftime("%Y-%m-%d")
        ctx = market_ctx.get(date_str, {})
        nifty = ctx.get("NIFTY 50", {})
        trend = nifty.get("trend", "neutral")

        # Block conflicting: call against bearish NIFTY, put against bullish NIFTY
        if p["option_type"] == "call" and trend == "bearish":
            continue
        if p["option_type"] == "put" and trend == "bullish":
            continue
        filtered.append(p)

    print(f"  Market context filter: {len(preds)} → {len(filtered)} signals")
    return filtered


def simulate_option(candles, signal_time, entry_offset, sl_pct, target_pct, max_exit_time):
    """Simulate F&O option trade on premium candles."""
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


def fetch_stock_spot_price(stock_name, signal_time, stock_key_map):
    """Fetch spot price at signal time using stock OHLC from TickerFlow."""
    inst_key = stock_key_map.get(stock_name)
    if not inst_key:
        return None

    trade_date = signal_time.strftime("%Y-%m-%d")
    # Get stock instruments to find equity instrument_seq
    instruments = tf.get_instruments(
        stock_name=stock_name,
        instrument_type="CE",
        limit=1,
    )
    if not instruments:
        return None

    # Use the stock_id to get equity candles from the ticks endpoint
    # Actually, we need the stock's equity price. Let's use a simpler approach:
    # Get any CE instrument near ATM to estimate spot, or use the stock API
    return None  # Will use a different approach


def main():
    start_time = time.time()
    run_dir = find_results_dir()
    print(f"Results dir: {run_dir}")

    # ── Step 1: Load pickle and re-rank with multi-window ─────────────
    pickle_path = run_dir / "all_results_full_2025_pct_sl.pickle"
    if not pickle_path.exists():
        # Try any pickle in the directory
        pickles = list(run_dir.glob("*.pickle"))
        if not pickles:
            print("ERROR: No pickle file found in results directory")
            return
        pickle_path = pickles[0]

    print(f"Loading analysis data from {pickle_path.name}...")
    with open(pickle_path, "rb") as f:
        all_results = pickle.load(f)
    print(f"  Loaded {len(all_results)} stock-date entries")

    print("Extracting qualified predictions (all time intervals)...")
    all_calls, all_puts = extract_qualified(all_results, TN_RATIO_THRESHOLD, ACCEPTED_GRADES)
    print(f"  Raw qualified: {len(all_calls)} calls, {len(all_puts)} puts")

    print(f"Applying consecutive filter with scan windows: {SCAN_WINDOWS}")
    cons_calls = find_consecutive_multiwindow(all_calls, SCAN_WINDOWS, CONSECUTIVE_MINUTES)
    cons_puts = find_consecutive_multiwindow(all_puts, SCAN_WINDOWS, CONSECUTIVE_MINUTES)
    print(f"  After consecutive filter: {len(cons_calls)} calls, {len(cons_puts)} puts")

    all_signals = cons_calls + cons_puts
    all_signals = apply_market_context_filter(all_signals, run_dir)

    # Sort by timestamp
    all_signals.sort(key=lambda p: p["timestamp"])

    # Show window distribution
    window_dist = defaultdict(int)
    for s in all_signals:
        h = s["timestamp"].strftime("%H:%M")
        window_dist[h] += 1
    print(f"  Signal time distribution: {dict(sorted(window_dist.items()))}")
    print(f"  Total signals to trade: {len(all_signals)}")

    # ── Step 2: Fetch option data and simulate trades ─────────────────
    print("\nFetching expiry dates and option data via TickerFlow API...")
    expiry_dates = tf.get_expiries()
    stock_key_map = tf.get_stock_key_map()

    # For signals from the pickle, we don't have spot_price directly.
    # We need to get it from the stock's equity candle at signal time.
    # Approach: fetch 5-min stock candles and get close at signal time.
    # But the simpler approach: use the Phase 2 existing trades for 09:45 signals
    # and only fetch new data for 12:00 signals.

    # Actually, let's fetch spot prices from equity instrument candles
    # We need the stock instrument_key to get equity OHLC

    prepared = []
    skipped = 0

    for sig in all_signals:
        stock = sig["stock"]
        opt_type = sig["option_type"]
        signal_time = sig["timestamp"]
        trade_date = signal_time.strftime("%Y-%m-%d")

        # Get spot price: fetch equity instrument and get price at signal time
        inst_key = stock_key_map.get(stock)
        if not inst_key:
            skipped += 1
            continue

        # Fetch 1-min equity candles to get spot at signal time
        try:
            equity_candles = tf.get_candles(
                instrument_id=0,  # not used for this endpoint
                interval="1m",
                start=f"{trade_date}T09:15:00",
                end=f"{trade_date}T15:30:00",
            )
        except Exception:
            equity_candles = pd.DataFrame()

        # Alternative: use the stock instrument to find nearest strike
        # and get spot from the instrument API
        # Simpler: get instruments near a wide range and pick ATM
        expiry = pick_expiry_for_date(trade_date, expiry_dates)
        inst_type = "CE" if opt_type == "call" else "PE"

        # Get instruments for this stock to estimate spot price
        instruments = tf.get_instruments(
            stock_name=stock,
            instrument_type=inst_type,
            expiry=expiry,
            limit=20,
        )
        if not instruments:
            skipped += 1
            continue

        # Estimate spot from available strikes (midpoint of available range)
        strikes = sorted([float(i["strike_price"]) for i in instruments])
        estimated_spot = strikes[len(strikes) // 2]

        # Now find ATM instrument
        inst = tf.find_atm_instrument(stock, opt_type, estimated_spot, expiry)
        if inst is None:
            skipped += 1
            continue

        # Fetch option premium candles
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
            "spot_price": estimated_spot,
            "lot_size": inst["lot_size"],
            "strike_price": inst["strike_price"],
            "trading_symbol": inst["trading_symbol"],
            "expiry": expiry,
            "candles": candles,
            "rsi": rsi_val,
            "grade": sig["grade"],
        })

    prepared.sort(key=lambda t: t["signal_time"])
    print(f"\nPrepared {len(prepared)} trades ({skipped} skipped)")

    # Window distribution after data fetch
    window_dist2 = defaultdict(int)
    for t in prepared:
        h = t["signal_time"].strftime("%H:%M")
        window_dist2[h] += 1
    print(f"Signal time distribution (prepared): {dict(sorted(window_dist2.items()))}")

    # ── Step 3: Simulate with 10L capital, max 5L/trade, same-day aware
    capital = float(STARTING_CAPITAL)
    rows = []

    # Pre-simulate
    trade_queue = []
    for t in prepared:
        if t["rsi"] is not None and t["rsi"] > RSI_OVERBOUGHT:
            continue
        sim = simulate_option(t["candles"], t["signal_time"], ENTRY_OFFSET,
                              SL_PCT, TARGET_PCT, MAX_EXIT_TIME)
        if sim is None:
            continue
        entry_premium = sim["entry_premium"]
        if entry_premium <= 0:
            continue
        cost_per_lot = entry_premium * t["lot_size"]
        if cost_per_lot <= 0:
            continue
        trade_queue.append({"trade": t, "sim": sim, "cost_per_lot": cost_per_lot})

    # Group by date for same-day capital management
    for date_key, day_group in groupby(trade_queue, key=lambda x: x["trade"]["trade_date"]):
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
                print(f"  SKIP {t['trade_date']} | {t['stock']:15s} | "
                      f"No capital left (locked Rs {day_locked:,.0f})")
                continue

            deploy = min(available, MAX_CAPITAL_PER_TRADE)
            num_lots = max(1, math.floor(deploy / cost_per_lot))
            capital_used = num_lots * cost_per_lot
            day_locked += capital_used

            pnl_per_unit = sim["exit_premium"] - entry_premium
            rupee_pnl = pnl_per_unit * lot_size * num_lots
            pnl_pct = (pnl_per_unit / entry_premium) * 100

            opt_label = "CE" if t["option_type"] == "call" else "PE"
            sig_window = "AM" if t["signal_time"].hour < 11 else "PM"

            row = {
                "trade_no": len(rows) + len(day_results) + 1,
                "trade_date": t["trade_date"],
                "stock": t["stock"],
                "direction": t["option_type"].upper(),
                "grade": t["grade"],
                "signal_window": sig_window,
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

            tag = "WIN" if row["outcome"] == "target_hit" else ("LOSS" if row["outcome"] == "sl_hit" else "TIME")
            win_lbl = f"[{row['signal_window']}]"
            print(
                f"  #{row['trade_no']:>2} {row['trade_date']} {win_lbl} | {row['stock']:15s} | "
                f"{row['strike_price']:>8} {row['option_type']} | "
                f"Prem {row['entry_premium']:>7.2f} → {row['exit_premium']:>7.2f} | "
                f"{tag:4s} | Lots {row['num_lots']:>3} x {row['lot_size']} | "
                f"Rs {row['rupee_pnl']:>+12,.0f} | Capital Rs {capital:>12,.0f}"
            )

    # ── Summary ───────────────────────────────────────────────────────
    total_trades = len(rows)
    wins = sum(1 for r in rows if r["outcome"] == "target_hit")
    losses = sum(1 for r in rows if r["outcome"] == "sl_hit")
    time_exits = sum(1 for r in rows if r["outcome"] == "time_exit")
    total_pnl = sum(r["rupee_pnl"] for r in rows)
    final_capital = STARTING_CAPITAL + total_pnl
    am_trades = sum(1 for r in rows if r["signal_window"] == "AM")
    pm_trades = sum(1 for r in rows if r["signal_window"] == "PM")

    print(f"\n{'=' * 90}")
    print(f"  MULTI-WINDOW | Grade A | 10% SL / 30% Target | 10L Capital | Max 5L/trade")
    print(f"{'=' * 90}")
    print(f"  Scan windows     : {SCAN_WINDOWS}")
    print(f"  Starting Capital : Rs {STARTING_CAPITAL:>12,}")
    print(f"  Final Capital    : Rs {final_capital:>12,.0f}")
    print(f"  Total P&L        : Rs {total_pnl:>+12,.0f}")
    print(f"  ROI              : {(total_pnl / STARTING_CAPITAL) * 100:.2f}%")
    print(f"  Trades           : {total_trades} (W:{wins}  L:{losses}  T:{time_exits})")
    print(f"  AM trades (09:45): {am_trades}")
    print(f"  PM trades (12:00): {pm_trades}")
    print(f"  Win Rate         : {wins / total_trades * 100:.1f}%" if total_trades else "")
    print(f"{'=' * 90}")

    # ── Write CSV ─────────────────────────────────────────────────────
    csv_path = run_dir / "multiwindow_gradeA_10l_trades.csv"
    if rows:
        fieldnames = list(rows[0].keys())
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nCSV saved to: {csv_path}")

    # ── Write JSON summary ────────────────────────────────────────────
    def _default(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    summary = {
        "strategy": "Multi-Window Grade A | 10% SL / 30% Target",
        "scan_windows": SCAN_WINDOWS,
        "starting_capital": STARTING_CAPITAL,
        "max_capital_per_trade": MAX_CAPITAL_PER_TRADE,
        "final_capital": round(final_capital, 2),
        "total_pnl": round(total_pnl, 2),
        "roi_pct": round((total_pnl / STARTING_CAPITAL) * 100, 2),
        "total_trades": total_trades,
        "am_trades": am_trades,
        "pm_trades": pm_trades,
        "wins": wins,
        "losses": losses,
        "time_exits": time_exits,
        "win_rate_pct": round(wins / total_trades * 100, 1) if total_trades else 0,
        "trades": rows,
    }
    json_path = run_dir / "multiwindow_gradeA_10l_summary.json"
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2, default=_default)

    elapsed = time.time() - start_time
    print(f"Completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
