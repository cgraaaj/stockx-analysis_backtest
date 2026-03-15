"""
Phase 3 Sweep: Run F&O simulation across SL/Target combos + entry modes.
Fetches data once via TickerFlow API, then re-simulates with each parameter set.

Compares: Immediate entry vs Staged entry (RSI pullback + volume).

Usage:  python src/backtest/run_phase3_sweep.py

Data source: TickerFlow API via tickerflow_client (no direct DB access).
"""

import json
import math
import os
import sys
import time
from datetime import datetime
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
    simulate_option_trade,
    simulate_staged_entry_trade,
    compute_rsi,
    get_premium_rsi_at_signal,
)

CAPITAL_PER_TRADE = 100_000
MAX_EXIT_TIME = "15:15:00"
ENTRY_OFFSET = 1
RSI_PERIOD = 14

SL_TARGET_COMBOS = [
    (1.0, 2.0),
    (2.0, 4.0),
    (3.0, 6.0),
    (4.0, 8.0),
    (5.0, 10.0),
    (6.0, 12.0),
    (7.0, 14.0),
    (8.0, 16.0),
    (9.0, 18.0),
    (10.0, 20.0),
]

STAGED_CONFIGS = [
    {"rsi_threshold": 60, "vol_mult": 1.2, "max_wait": 60, "deadline": "14:00:00"},
    {"rsi_threshold": 55, "vol_mult": 1.2, "max_wait": 60, "deadline": "14:00:00"},
    {"rsi_threshold": 50, "vol_mult": 1.0, "max_wait": 30, "deadline": "13:00:00"},
]


def run_immediate(prepared: list[dict], sl_pct: float, target_pct: float) -> dict:
    wins = losses = time_exits = 0
    total_pnl = total_capital = 0.0
    win_pnl = loss_pnl = te_pnl = 0.0

    for t in prepared:
        sim = simulate_option_trade(
            t["candles"], t["signal_time"], ENTRY_OFFSET,
            sl_pct, target_pct, MAX_EXIT_TIME,
        )
        if sim is None:
            continue

        entry_premium = t["entry_premium"]
        lot_size = t["lot_size"]
        cost_per_lot = entry_premium * lot_size
        num_lots = max(1, math.floor(CAPITAL_PER_TRADE / cost_per_lot))
        capital_used = num_lots * cost_per_lot
        total_capital += capital_used

        pnl = (sim["exit_premium"] - entry_premium) * lot_size * num_lots
        total_pnl += pnl

        if sim["outcome"] == "target_hit":
            wins += 1; win_pnl += pnl
        elif sim["outcome"] == "sl_hit":
            losses += 1; loss_pnl += pnl
        else:
            time_exits += 1; te_pnl += pnl

    total = wins + losses + time_exits
    return _build_result("immediate", sl_pct, target_pct, total, wins, losses,
                         time_exits, total_pnl, total_capital, win_pnl, loss_pnl, te_pnl)


def run_staged(
    prepared: list[dict], sl_pct: float, target_pct: float, scfg: dict,
) -> dict:
    wins = losses = time_exits = no_entries = 0
    total_pnl = total_capital = 0.0
    win_pnl = loss_pnl = te_pnl = 0.0
    total_waited = 0

    for t in prepared:
        sim = simulate_staged_entry_trade(
            t["candles"], t["signal_time"],
            sl_pct=sl_pct, target_pct=target_pct,
            max_exit_time=MAX_EXIT_TIME,
            rsi_period=RSI_PERIOD,
            rsi_entry_threshold=scfg["rsi_threshold"],
            vol_multiplier=scfg["vol_mult"],
            vol_lookback=10,
            max_wait_candles=scfg["max_wait"],
            entry_deadline=scfg["deadline"],
        )
        if sim is None or sim.get("outcome") == "no_entry":
            no_entries += 1
            continue

        entry_premium = sim["entry_premium"]
        total_waited += sim.get("candles_waited", 0)
        lot_size = t["lot_size"]
        cost_per_lot = entry_premium * lot_size
        if cost_per_lot <= 0:
            no_entries += 1
            continue

        num_lots = max(1, math.floor(CAPITAL_PER_TRADE / cost_per_lot))
        capital_used = num_lots * cost_per_lot
        total_capital += capital_used

        pnl = (sim["exit_premium"] - entry_premium) * lot_size * num_lots
        total_pnl += pnl

        if sim["outcome"] == "target_hit":
            wins += 1; win_pnl += pnl
        elif sim["outcome"] == "sl_hit":
            losses += 1; loss_pnl += pnl
        else:
            time_exits += 1; te_pnl += pnl

    total = wins + losses + time_exits
    label = f"staged(RSI<{scfg['rsi_threshold']},vol{scfg['vol_mult']}x,wait{scfg['max_wait']})"
    res = _build_result(label, sl_pct, target_pct, total, wins, losses,
                        time_exits, total_pnl, total_capital, win_pnl, loss_pnl, te_pnl)
    res["no_entries"] = no_entries
    res["avg_wait"] = round(total_waited / total, 1) if total else 0
    return res


def _build_result(mode, sl, tgt, total, w, l, t, pnl, cap, wp, lp, tp):
    return {
        "mode": mode, "sl_pct": sl, "target_pct": tgt,
        "total": total, "wins": w, "losses": l, "time_exits": t,
        "win_rate": round(w / total * 100, 1) if total else 0,
        "total_pnl": round(pnl, 0),
        "avg_pnl": round(pnl / total, 0) if total else 0,
        "capital_deployed": round(cap, 0),
        "roi_pct": round(pnl / cap * 100, 2) if cap else 0,
        "win_pnl": round(wp, 0), "loss_pnl": round(lp, 0), "te_pnl": round(tp, 0),
    }


def print_table(title: str, results: list[dict], show_extra: bool = False):
    print(f"\n{'=' * 130}")
    print(f"  {title}")
    print(f"{'=' * 130}")

    extra = "{'Skip':>4} {'AvgW':>5} │ " if show_extra else ""
    hdr = (
        f"{'SL%':>5} {'TGT%':>5} │ {'Trades':>6} {'W':>4} {'L':>4} {'T':>4} │ "
        f"{extra}"
        f"{'WinRate':>7} │ {'Total PnL':>12} {'Avg PnL':>10} │ {'ROI%':>7} │ "
        f"{'WinPnL':>10} {'LossPnL':>10} {'TimePnL':>10}"
    )
    sep = "─" * len(hdr)
    print(sep)
    print(hdr)
    print(sep)

    best = max(results, key=lambda r: r["total_pnl"])

    for r in results:
        marker = " ★" if r is best else ""
        extra_cols = ""
        if show_extra:
            extra_cols = f"{r.get('no_entries', 0):>4} {r.get('avg_wait', 0):>5.1f} │ "
        print(
            f"{r['sl_pct']:>5.1f} {r['target_pct']:>5.1f} │ "
            f"{r['total']:>6} {r['wins']:>4} {r['losses']:>4} {r['time_exits']:>4} │ "
            f"{extra_cols}"
            f"{r['win_rate']:>6.1f}% │ "
            f"{r['total_pnl']:>+12,.0f} {r['avg_pnl']:>+10,.0f} │ "
            f"{r['roi_pct']:>6.2f}% │ "
            f"{r['win_pnl']:>+10,.0f} {r['loss_pnl']:>+10,.0f} {r['te_pnl']:>+10,.0f}"
            f"{marker}"
        )
    print(sep)
    return best


def main():
    start = time.time()
    run_dir = find_results_dir()
    print(f"Results dir: {run_dir}")

    phase2_trades = load_phase2_trades(run_dir)
    print(f"Loaded {len(phase2_trades)} Phase 2 trades")

    expiry_dates = tf.get_expiries()

    print("Fetching instruments and premium candles (one-time) via TickerFlow API...")
    prepared = []
    skipped = 0

    for trade in phase2_trades:
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

        signal_idx = candles[candles["time_stamp"] <= signal_time].index
        if signal_idx.empty:
            skipped += 1
            continue
        entry_idx = signal_idx[-1] + ENTRY_OFFSET
        if entry_idx >= len(candles):
            skipped += 1
            continue

        entry_premium = float(candles.iloc[entry_idx]["open"])
        if entry_premium <= 0:
            skipped += 1
            continue

        rsi_val = get_premium_rsi_at_signal(candles, signal_time, RSI_PERIOD)

        prepared.append({
            "stock": stock,
            "option_type": opt_type,
            "trade_date": trade_date,
            "signal_time": signal_time,
            "lot_size": inst["lot_size"],
            "entry_premium": entry_premium,
            "candles": candles,
            "strike": inst["strike_price"],
            "symbol": inst["trading_symbol"],
            "rsi": rsi_val,
        })

    print(f"Prepared {len(prepared)} trades ({skipped} skipped)")

    rsi_values = [t["rsi"] for t in prepared if t["rsi"] is not None]
    if rsi_values:
        print(f"RSI stats: min={min(rsi_values):.1f}  max={max(rsi_values):.1f}  "
              f"mean={sum(rsi_values)/len(rsi_values):.1f}\n")

    all_bests = []

    imm_results = [run_immediate(prepared, sl, tgt) for sl, tgt in SL_TARGET_COMBOS]
    best_imm = print_table("IMMEDIATE ENTRY (signal + 1 candle)", imm_results)
    all_bests.append(("Immediate", best_imm))

    for scfg in STAGED_CONFIGS:
        label = f"STAGED ENTRY (RSI<={scfg['rsi_threshold']}, vol>={scfg['vol_mult']}x, max_wait={scfg['max_wait']}, deadline={scfg['deadline']})"
        stg_results = [run_staged(prepared, sl, tgt, scfg) for sl, tgt in SL_TARGET_COMBOS]
        best_stg = print_table(label, stg_results, show_extra=True)
        all_bests.append((label, best_stg))

    print(f"\n{'#' * 130}")
    print("  HEAD-TO-HEAD: Best from each entry mode")
    print(f"{'#' * 130}")
    overall_best = None
    for mode_name, best in all_bests:
        print(f"  {mode_name[:60]:60s} │ {best['sl_pct']}%/{best['target_pct']}% │ "
              f"PnL Rs {best['total_pnl']:>+10,.0f} │ ROI {best['roi_pct']:>6.2f}% │ "
              f"Win {best['win_rate']}% │ Trades {best['total']}")
        if overall_best is None or best["total_pnl"] > overall_best[1]["total_pnl"]:
            overall_best = (mode_name, best)

    print(f"\n  >>> OVERALL WINNER: {overall_best[0][:60]}")
    print(f"      {overall_best[1]['sl_pct']}% SL / {overall_best[1]['target_pct']}% Target → "
          f"Rs {overall_best[1]['total_pnl']:>+,.0f}  |  ROI {overall_best[1]['roi_pct']}%")
    print(f"{'#' * 130}")

    sweep_out = {
        "immediate": [r for r in imm_results],
        "staged_configs": STAGED_CONFIGS,
        "staged_results": {},
        "best_per_mode": {name: {k: v for k, v in b.items()} for name, b in all_bests},
        "overall_winner": {
            "mode": overall_best[0],
            **{k: v for k, v in overall_best[1].items()},
        },
        "prepared_trades": len(prepared),
        "skipped": skipped,
    }
    out_path = run_dir / "phase3_sweep_results.json"
    with open(out_path, "w") as f:
        json.dump(sweep_out, f, indent=2)
    print(f"\nSaved to {out_path}")

    elapsed = time.time() - start
    print(f"Sweep completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
