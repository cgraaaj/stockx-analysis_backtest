"""
Stock R:R Sweep: Grade A+B | Underlying Stock Trades | 10L Capital
===================================================================
Same OI signals, but trades the underlying stock (not options).
SL/Target as percentage of stock buy price.
Sweeps R:R from 1:2 to 1:5.

Usage:
  python -m src.backtest.run_stock_rr_sweep [results_dir]
"""

import asyncio
import csv
import json
import math
import os
import sys
import time
import urllib.parse
from datetime import datetime
from itertools import groupby
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

import aiohttp
import pandas as pd

from src.backtest import tickerflow_client as tf
from src.backtest.run_phase3_fno import find_results_dir, load_phase2_trades

# ── Config ────────────────────────────────────────────────────────────
STARTING_CAPITAL = 10_00_000
MAX_CAPITAL_PER_TRADE = 5_00_000
SL_PCT = 1.0                       # 1% SL on stock price
ACCEPTED_GRADES = ["A", "B"]
ENTRY_OFFSET = 1                    # enter 1 candle after signal
MAX_EXIT_TIME = "15:15:00"
CANDLE_INTERVAL = "5min"

UPSTOX_BASE = "https://api.upstox.com/v2/historical-candle"

RR_RATIOS = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
# ──────────────────────────────────────────────────────────────────────


async def fetch_stock_candles(session, instrument_key, date, stock_name):
    """Fetch 1-min candles from Upstox public API, resample to 5-min."""
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
                            df.sort_values("timestamp", inplace=True)
                            df.reset_index(drop=True, inplace=True)

                            # Resample to 5-min
                            df5 = df.set_index("timestamp").resample(
                                "5min", label="left", closed="left"
                            ).agg({
                                "open": "first", "high": "max",
                                "low": "min", "close": "last", "volume": "sum",
                            }).dropna().reset_index()
                            return df5
        except Exception:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    return pd.DataFrame()


def simulate_stock_trade(candles, signal_time, entry_offset, sl_pct, target_pct,
                         max_exit_time, direction):
    """Simulate a stock trade with percentage SL/Target."""
    max_exit_ts = datetime.strptime(
        f"{signal_time.strftime('%Y-%m-%d')} {max_exit_time}", "%Y-%m-%d %H:%M:%S"
    )

    signal_idx = candles[candles["timestamp"] <= signal_time].index
    if signal_idx.empty:
        return None
    entry_idx = signal_idx[-1] + entry_offset
    if entry_idx >= len(candles):
        return None

    entry_row = candles.iloc[entry_idx]
    entry_price = float(entry_row["open"])
    entry_time = entry_row["timestamp"]
    if entry_price <= 0:
        return None

    if direction == "call":
        sl_price = entry_price * (1 - sl_pct / 100.0)
        target_price = entry_price * (1 + target_pct / 100.0)
    else:  # put
        sl_price = entry_price * (1 + sl_pct / 100.0)
        target_price = entry_price * (1 - target_pct / 100.0)

    for i in range(entry_idx + 1, len(candles)):
        row = candles.iloc[i]
        ts = row["timestamp"]
        high, low, close = float(row["high"]), float(row["low"]), float(row["close"])

        if ts >= max_exit_ts:
            pnl = (close - entry_price) if direction == "call" else (entry_price - close)
            return dict(outcome="time_exit", entry_price=entry_price,
                        exit_price=close, entry_time=entry_time, exit_time=ts,
                        sl_price=sl_price, target_price=target_price,
                        pnl_points=pnl, holding_candles=i - entry_idx)

        if direction == "call":
            if low <= sl_price:
                return dict(outcome="sl_hit", entry_price=entry_price,
                            exit_price=sl_price, entry_time=entry_time, exit_time=ts,
                            sl_price=sl_price, target_price=target_price,
                            pnl_points=sl_price - entry_price, holding_candles=i - entry_idx)
            if high >= target_price:
                return dict(outcome="target_hit", entry_price=entry_price,
                            exit_price=target_price, entry_time=entry_time, exit_time=ts,
                            sl_price=sl_price, target_price=target_price,
                            pnl_points=target_price - entry_price, holding_candles=i - entry_idx)
        else:  # put
            if high >= sl_price:
                return dict(outcome="sl_hit", entry_price=entry_price,
                            exit_price=sl_price, entry_time=entry_time, exit_time=ts,
                            sl_price=sl_price, target_price=target_price,
                            pnl_points=entry_price - sl_price, holding_candles=i - entry_idx)
            if low <= target_price:
                return dict(outcome="target_hit", entry_price=entry_price,
                            exit_price=target_price, entry_time=entry_time, exit_time=ts,
                            sl_price=sl_price, target_price=target_price,
                            pnl_points=entry_price - target_price, holding_candles=i - entry_idx)

    last = candles.iloc[-1]
    pnl = (float(last["close"]) - entry_price) if direction == "call" else (entry_price - float(last["close"]))
    return dict(outcome="time_exit", entry_price=entry_price,
                exit_price=float(last["close"]), entry_time=entry_time,
                exit_time=last["timestamp"], sl_price=sl_price, target_price=target_price,
                pnl_points=pnl, holding_candles=len(candles) - 1 - entry_idx)


def run_with_rr(prepared, rr_ratio, sl_pct):
    """Run simulation for a given R:R ratio, return summary + trade rows."""
    target_pct = sl_pct * rr_ratio
    capital = float(STARTING_CAPITAL)
    rows = []

    # Pre-simulate
    trade_queue = []
    for t in prepared:
        sim = simulate_stock_trade(
            t["candles"], t["signal_time"], ENTRY_OFFSET,
            sl_pct, target_pct, MAX_EXIT_TIME, t["option_type"],
        )
        if sim is None:
            continue
        trade_queue.append({"trade": t, "sim": sim})

    # Same-day capital management
    for _, day_group in groupby(trade_queue, key=lambda x: x["trade"]["trade_date"]):
        day_trades = list(day_group)
        day_capital_pool = capital
        day_locked = 0.0
        day_results = []

        for item in day_trades:
            t = item["trade"]
            sim = item["sim"]
            entry_price = sim["entry_price"]

            available = day_capital_pool - day_locked
            if available <= 0:
                continue

            deploy = min(available, MAX_CAPITAL_PER_TRADE)
            qty = max(1, math.floor(deploy / entry_price))
            capital_used = qty * entry_price
            day_locked += capital_used

            rupee_pnl = sim["pnl_points"] * qty
            pnl_pct = (sim["pnl_points"] / entry_price) * 100

            opt_label = "CE" if t["option_type"] == "call" else "PE"

            row = {
                "trade_no": len(rows) + len(day_results) + 1,
                "trade_date": t["trade_date"],
                "stock": t["stock"],
                "direction": t["option_type"].upper(),
                "grade": t["grade"],
                "entry_price": round(entry_price, 2),
                "sl_price": round(sim["sl_price"], 2),
                "target_price": round(sim["target_price"], 2),
                "exit_price": round(sim["exit_price"], 2),
                "buy_time": str(sim["entry_time"]),
                "sell_time": str(sim["exit_time"]),
                "holding_candles": sim["holding_candles"],
                "outcome": sim["outcome"],
                "pnl_points": round(sim["pnl_points"], 2),
                "pnl_pct": round(pnl_pct, 2),
                "qty": qty,
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
        "sl_pct": sl_pct,
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


async def main():
    start = time.time()
    run_dir = find_results_dir()
    print(f"Results dir: {run_dir}")

    phase2_trades = load_phase2_trades(run_dir)
    print(f"Loaded {len(phase2_trades)} Phase 2 trades")

    # Filter to Grade A+B
    filtered = [t for t in phase2_trades if t.get("grade", "D") in ACCEPTED_GRADES]
    print(f"Grade A+B: {len(filtered)} trades")

    # Get instrument keys for stock equity candles
    stock_key_map = tf.get_stock_key_map()

    # Fetch stock candles from Upstox
    print("Fetching stock candles from Upstox...")
    prepared = []
    skipped = 0

    async with aiohttp.ClientSession() as session:
        for trade in filtered:
            stock = trade["stock"]
            opt_type = trade["option_type"]
            signal_time = datetime.strptime(trade["signal_time"], "%Y-%m-%d %H:%M:%S")
            trade_date = signal_time.strftime("%Y-%m-%d")

            inst_key = stock_key_map.get(stock)
            if not inst_key:
                skipped += 1
                continue

            candles = await fetch_stock_candles(session, inst_key, trade_date, stock)
            if candles.empty:
                skipped += 1
                continue

            prepared.append({
                "stock": stock,
                "option_type": opt_type,
                "trade_date": trade_date,
                "signal_time": signal_time,
                "spot_price": trade["entry_price"],
                "candles": candles,
                "grade": trade.get("grade", "D"),
            })

    prepared.sort(key=lambda t: t["signal_time"])
    print(f"Prepared {len(prepared)} stock trades ({skipped} skipped)")

    # ── Run R:R sweep ─────────────────────────────────────────────────
    print(f"\nSweeping R:R ratios: {RR_RATIOS} (SL = {SL_PCT}%)")
    all_summaries = []
    best_rows = None
    best_roi = -999
    best_rr = RR_RATIOS[0]

    for rr in RR_RATIOS:
        summary, rows = run_with_rr(prepared, rr, SL_PCT)
        all_summaries.append(summary)
        if summary["roi_pct"] > best_roi:
            best_roi = summary["roi_pct"]
            best_rows = rows
            best_rr = rr

    # ── Print comparison table ────────────────────────────────────────
    print(f"\n{'=' * 120}")
    print(f"  STOCK R:R SWEEP | Grade A+B | {SL_PCT}% SL | 10L Capital | Max 5L/trade")
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
            f" 1:{s['rr_ratio']:<3.1f} │ {s['sl_pct']:>4.1f} {s['target_pct']:>5.1f} │ "
            f"{s['total_trades']:>6} {s['wins']:>3} {s['losses']:>3} {s['time_exits']:>3} │ "
            f"{s['win_rate']:>6.1f}% │ "
            f"{s['total_pnl']:>+12,.0f} {s['avg_pnl']:>+10,.0f} │ "
            f"{s['roi_pct']:>7.2f}% │ "
            f"Rs {s['final_capital']:>12,.0f}{marker}"
        )
    print(sep)

    print(f"\n  >>> BEST R:R = 1:{best_rr:.1f} (SL {SL_PCT}% / Target {SL_PCT * best_rr:.1f}%) → ROI {best_roi:.2f}%")

    # ── Print best R:R trade details ──────────────────────────────────
    if best_rows:
        print(f"\n--- Best R:R 1:{best_rr} Trade Details ---")
        for r in best_rows:
            tag = "WIN" if r["outcome"] == "target_hit" else ("LOSS" if r["outcome"] == "sl_hit" else "TIME")
            print(
                f"  #{r['trade_no']:>2} {r['trade_date']} | {r['stock']:15s} | "
                f"{r['direction']:4s} | "
                f"Buy {r['entry_price']:>8.2f} → Sell {r['exit_price']:>8.2f} | "
                f"{tag:4s} | Qty {r['qty']:>4} | "
                f"Rs {r['rupee_pnl']:>+10,.0f} | Capital Rs {r['capital_after']:>12,.0f}"
            )

    # ── Save CSVs ─────────────────────────────────────────────────────
    # Save 1:2 specifically (user's primary ask)
    _, rows_1to2 = run_with_rr(prepared, 2.0, SL_PCT)
    csv_1to2 = run_dir / "stock_gradeAB_rr2_trades.csv"
    if rows_1to2:
        with open(csv_1to2, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows_1to2[0].keys()))
            writer.writeheader()
            writer.writerows(rows_1to2)
        print(f"\n1:2 trades CSV: {csv_1to2}")

    # Save best R:R CSV
    csv_best = run_dir / f"stock_gradeAB_rr{best_rr:.1f}_best_trades.csv"
    if best_rows:
        with open(csv_best, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(best_rows[0].keys()))
            writer.writeheader()
            writer.writerows(best_rows)
        print(f"Best R:R trades CSV: {csv_best}")

    # Save all R:R CSVs
    for rr in RR_RATIOS:
        _, rr_rows = run_with_rr(prepared, rr, SL_PCT)
        rr_csv = run_dir / f"stock_gradeAB_rr{rr:.1f}_trades.csv"
        if rr_rows:
            with open(rr_csv, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rr_rows[0].keys()))
                writer.writeheader()
                writer.writerows(rr_rows)

    # ── Save summary JSON ─────────────────────────────────────────────
    def _default(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    json_path = run_dir / "stock_gradeAB_rr_sweep_summary.json"
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
    asyncio.run(main())
