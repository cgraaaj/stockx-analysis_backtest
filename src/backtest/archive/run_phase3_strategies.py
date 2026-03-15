"""
Phase 3 Strategy Sweep: Advanced Strategy Combinations
=======================================================
Tests multiple strategy enhancements to push F&O backtesting ROI toward 10%+.

Strategies tested:
  1. Trailing stop (let winners run instead of fixed target)
  2. Grade filtering (A-only, A+B only)
  3. Time-of-day filter (skip noisy early signals)
  4. Partial profit booking (book 50% at fixed target, trail rest)
  5. Asymmetric R:R (wider targets 1:3, 1:4)
  6. Combined best-of-all

Fetches data once, then re-simulates all combinations.

Usage:
  python -m src.backtest.run_phase3_strategies [results_dir]
"""

import json
import math
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict

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

# ======================================================================
# Strategy configuration dataclass
# ======================================================================

@dataclass
class StrategyConfig:
    name: str = "baseline"

    # SL / Target
    sl_pct: float = 7.0
    target_pct: float = 14.0

    # Trailing stop
    use_trailing_stop: bool = False
    trail_activation_pct: float = 5.0       # premium must move +X% before trailing kicks in
    trail_distance_pct: float = 5.0          # trail X% below peak premium

    # Partial exit
    use_partial_exit: bool = False
    partial_book_pct: float = 50.0           # % of position to book at first target
    partial_target_pct: float = 10.0         # first target for partial booking
    partial_trail_pct: float = 5.0           # trailing stop for remaining position

    # Grade filter
    accepted_grades: list = field(default_factory=lambda: ["A", "B", "C", "D"])

    # Time-of-day filter
    min_signal_time: str = "09:30:00"        # earliest signal to accept
    max_signal_time: str = "14:00:00"        # latest signal to accept

    # RSI filter
    use_rsi_filter: bool = True
    rsi_overbought: float = 80.0

    # Entry
    entry_candle_offset: int = 1
    max_exit_time: str = "15:15:00"
    capital_per_trade: float = 100_000


# ======================================================================
# Simulation engines
# ======================================================================

def simulate_trailing_stop(
    candles: pd.DataFrame,
    signal_time: datetime,
    entry_offset: int,
    sl_pct: float,
    trail_activation_pct: float,
    trail_distance_pct: float,
    max_exit_time: str,
    target_pct: float = 0.0,    # optional hard target cap (0 = no cap)
) -> dict | None:
    """
    Trailing stop simulation:
    - Start with a fixed SL at entry_premium * (1 - sl_pct%)
    - Once premium rises by trail_activation_pct%, switch to trailing mode
    - In trailing mode, SL follows at trail_distance_pct% below peak
    - Optionally hit a hard target cap if target_pct > 0
    """
    max_exit_ts = datetime.strptime(
        f"{signal_time.strftime('%Y-%m-%d')} {max_exit_time}", "%Y-%m-%d %H:%M:%S"
    )

    signal_idx = candles[candles["time_stamp"] <= signal_time].index
    if signal_idx.empty:
        return None
    entry_idx = signal_idx[-1] + entry_offset
    if entry_idx >= len(candles):
        return None

    entry_premium = float(candles.iloc[entry_idx]["open"])
    if entry_premium <= 0:
        return None

    fixed_sl = entry_premium * (1 - sl_pct / 100.0)
    activation_price = entry_premium * (1 + trail_activation_pct / 100.0)
    hard_target = entry_premium * (1 + target_pct / 100.0) if target_pct > 0 else float("inf")

    peak_premium = entry_premium
    trailing_active = False
    current_sl = fixed_sl

    for i in range(entry_idx + 1, len(candles)):
        row = candles.iloc[i]
        ts = row["time_stamp"]
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])

        if ts >= max_exit_ts:
            return {
                "outcome": "time_exit",
                "exit_premium": close,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
                "peak_premium": peak_premium,
                "trailing_active": trailing_active,
            }

        # Check SL first
        if low <= current_sl:
            exit_price = current_sl if not trailing_active else current_sl
            outcome = "trail_stop" if trailing_active else "sl_hit"
            return {
                "outcome": outcome,
                "exit_premium": exit_price,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
                "peak_premium": peak_premium,
                "trailing_active": trailing_active,
            }

        # Check hard target cap
        if high >= hard_target:
            return {
                "outcome": "target_hit",
                "exit_premium": hard_target,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
                "peak_premium": max(peak_premium, high),
                "trailing_active": trailing_active,
            }

        # Update peak and trailing SL
        if high > peak_premium:
            peak_premium = high

        if not trailing_active and peak_premium >= activation_price:
            trailing_active = True

        if trailing_active:
            new_trail_sl = peak_premium * (1 - trail_distance_pct / 100.0)
            current_sl = max(current_sl, new_trail_sl)

    last = candles.iloc[-1]
    return {
        "outcome": "time_exit",
        "exit_premium": float(last["close"]),
        "exit_time": str(last["time_stamp"]),
        "holding_candles": len(candles) - 1 - entry_idx,
        "peak_premium": peak_premium,
        "trailing_active": trailing_active,
    }


def simulate_partial_exit(
    candles: pd.DataFrame,
    signal_time: datetime,
    entry_offset: int,
    sl_pct: float,
    partial_target_pct: float,
    partial_book_pct: float,
    trail_distance_pct: float,
    max_exit_time: str,
) -> dict | None:
    """
    Partial exit simulation:
    - Enter full position
    - When premium hits partial_target_pct%, book partial_book_pct% of position
    - Trail remaining position with trail_distance_pct% from peak
    - Fixed SL applies to full position until partial target is hit
    """
    max_exit_ts = datetime.strptime(
        f"{signal_time.strftime('%Y-%m-%d')} {max_exit_time}", "%Y-%m-%d %H:%M:%S"
    )

    signal_idx = candles[candles["time_stamp"] <= signal_time].index
    if signal_idx.empty:
        return None
    entry_idx = signal_idx[-1] + entry_offset
    if entry_idx >= len(candles):
        return None

    entry_premium = float(candles.iloc[entry_idx]["open"])
    if entry_premium <= 0:
        return None

    fixed_sl = entry_premium * (1 - sl_pct / 100.0)
    partial_target = entry_premium * (1 + partial_target_pct / 100.0)

    partial_booked = False
    partial_exit_premium = 0.0
    peak_premium = entry_premium
    current_sl = fixed_sl
    book_fraction = partial_book_pct / 100.0
    remain_fraction = 1.0 - book_fraction

    for i in range(entry_idx + 1, len(candles)):
        row = candles.iloc[i]
        ts = row["time_stamp"]
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])

        if ts >= max_exit_ts:
            if partial_booked:
                # Remaining portion exits at close
                return {
                    "outcome": "partial_time_exit",
                    "exit_premium": close,
                    "exit_time": str(ts),
                    "holding_candles": i - entry_idx,
                    "partial_booked": True,
                    "partial_exit_premium": partial_exit_premium,
                    "book_fraction": book_fraction,
                    "remain_fraction": remain_fraction,
                    "peak_premium": peak_premium,
                }
            return {
                "outcome": "time_exit",
                "exit_premium": close,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
                "partial_booked": False,
                "peak_premium": peak_premium,
            }

        # SL check
        if low <= current_sl:
            if partial_booked:
                return {
                    "outcome": "partial_trail_stop",
                    "exit_premium": current_sl,
                    "exit_time": str(ts),
                    "holding_candles": i - entry_idx,
                    "partial_booked": True,
                    "partial_exit_premium": partial_exit_premium,
                    "book_fraction": book_fraction,
                    "remain_fraction": remain_fraction,
                    "peak_premium": peak_premium,
                }
            return {
                "outcome": "sl_hit",
                "exit_premium": current_sl,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
                "partial_booked": False,
                "peak_premium": peak_premium,
            }

        # Partial booking check
        if not partial_booked and high >= partial_target:
            partial_booked = True
            partial_exit_premium = partial_target
            # Move SL to breakeven + small buffer for remaining position
            current_sl = entry_premium * 1.002  # breakeven + 0.2%

        # Update peak and trailing SL for remaining position
        if high > peak_premium:
            peak_premium = high

        if partial_booked:
            new_trail_sl = peak_premium * (1 - trail_distance_pct / 100.0)
            current_sl = max(current_sl, new_trail_sl)

    last = candles.iloc[-1]
    if partial_booked:
        return {
            "outcome": "partial_time_exit",
            "exit_premium": float(last["close"]),
            "exit_time": str(last["time_stamp"]),
            "holding_candles": len(candles) - 1 - entry_idx,
            "partial_booked": True,
            "partial_exit_premium": partial_exit_premium,
            "book_fraction": book_fraction,
            "remain_fraction": remain_fraction,
            "peak_premium": peak_premium,
        }
    return {
        "outcome": "time_exit",
        "exit_premium": float(last["close"]),
        "exit_time": str(last["time_stamp"]),
        "holding_candles": len(candles) - 1 - entry_idx,
        "partial_booked": False,
        "peak_premium": peak_premium,
    }


def simulate_fixed_target(
    candles: pd.DataFrame,
    signal_time: datetime,
    entry_offset: int,
    sl_pct: float,
    target_pct: float,
    max_exit_time: str,
) -> dict | None:
    """Standard fixed SL/Target simulation (same as original but inlined for consistency)."""
    max_exit_ts = datetime.strptime(
        f"{signal_time.strftime('%Y-%m-%d')} {max_exit_time}", "%Y-%m-%d %H:%M:%S"
    )

    signal_idx = candles[candles["time_stamp"] <= signal_time].index
    if signal_idx.empty:
        return None
    entry_idx = signal_idx[-1] + entry_offset
    if entry_idx >= len(candles):
        return None

    entry_premium = float(candles.iloc[entry_idx]["open"])
    if entry_premium <= 0:
        return None

    sl_premium = entry_premium * (1 - sl_pct / 100.0)
    target_premium = entry_premium * (1 + target_pct / 100.0)

    for i in range(entry_idx + 1, len(candles)):
        row = candles.iloc[i]
        ts = row["time_stamp"]
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])

        if ts >= max_exit_ts:
            return {"outcome": "time_exit", "exit_premium": close,
                    "exit_time": str(ts), "holding_candles": i - entry_idx}
        if low <= sl_premium:
            return {"outcome": "sl_hit", "exit_premium": sl_premium,
                    "exit_time": str(ts), "holding_candles": i - entry_idx}
        if high >= target_premium:
            return {"outcome": "target_hit", "exit_premium": target_premium,
                    "exit_time": str(ts), "holding_candles": i - entry_idx}

    last = candles.iloc[-1]
    return {"outcome": "time_exit", "exit_premium": float(last["close"]),
            "exit_time": str(last["time_stamp"]),
            "holding_candles": len(candles) - 1 - entry_idx}


# ======================================================================
# Strategy runner
# ======================================================================

def run_strategy(prepared: list[dict], cfg: StrategyConfig) -> dict:
    """Run a single strategy config across all prepared trades."""
    wins = losses = time_exits = trail_stops = partial_wins = 0
    total_pnl = total_capital = 0.0
    skipped_grade = skipped_time = skipped_rsi = 0
    trades_taken = 0

    for t in prepared:
        # Grade filter
        grade = t.get("grade", "D")
        if grade not in cfg.accepted_grades:
            skipped_grade += 1
            continue

        # Time-of-day filter
        sig_time_str = t["signal_time"].strftime("%H:%M:%S")
        if sig_time_str < cfg.min_signal_time or sig_time_str > cfg.max_signal_time:
            skipped_time += 1
            continue

        # RSI filter
        if cfg.use_rsi_filter and t.get("rsi") is not None:
            if t["rsi"] > cfg.rsi_overbought:
                skipped_rsi += 1
                continue

        trades_taken += 1
        entry_premium = t["entry_premium"]
        lot_size = t["lot_size"]
        cost_per_lot = entry_premium * lot_size
        if cost_per_lot <= 0:
            continue
        num_lots = max(1, math.floor(cfg.capital_per_trade / cost_per_lot))
        capital_used = num_lots * cost_per_lot
        total_capital += capital_used

        # Choose simulation engine
        if cfg.use_partial_exit:
            sim = simulate_partial_exit(
                t["candles"], t["signal_time"], cfg.entry_candle_offset,
                cfg.sl_pct, cfg.partial_target_pct, cfg.partial_book_pct,
                cfg.partial_trail_pct, cfg.max_exit_time,
            )
        elif cfg.use_trailing_stop:
            sim = simulate_trailing_stop(
                t["candles"], t["signal_time"], cfg.entry_candle_offset,
                cfg.sl_pct, cfg.trail_activation_pct, cfg.trail_distance_pct,
                cfg.max_exit_time, target_pct=cfg.target_pct,
            )
        else:
            sim = simulate_fixed_target(
                t["candles"], t["signal_time"], cfg.entry_candle_offset,
                cfg.sl_pct, cfg.target_pct, cfg.max_exit_time,
            )

        if sim is None:
            continue

        # Calculate PnL
        if cfg.use_partial_exit and sim.get("partial_booked"):
            # Partial: book_fraction at partial_exit_premium + remain_fraction at exit_premium
            book_frac = sim["book_fraction"]
            remain_frac = sim["remain_fraction"]
            partial_pnl = (sim["partial_exit_premium"] - entry_premium) * book_frac
            remain_pnl = (sim["exit_premium"] - entry_premium) * remain_frac
            pnl_per_unit = partial_pnl + remain_pnl
        else:
            pnl_per_unit = sim["exit_premium"] - entry_premium

        rupee_pnl = pnl_per_unit * lot_size * num_lots
        total_pnl += rupee_pnl

        outcome = sim["outcome"]
        if outcome in ("target_hit",):
            wins += 1
        elif outcome in ("trail_stop", "partial_trail_stop"):
            trail_stops += 1
        elif outcome in ("partial_time_exit",):
            partial_wins += 1
        elif outcome == "sl_hit":
            losses += 1
        else:
            time_exits += 1

    total = wins + losses + time_exits + trail_stops + partial_wins
    effective_wins = wins + trail_stops + partial_wins  # all forms of profitable exit

    return {
        "name": cfg.name,
        "sl_pct": cfg.sl_pct,
        "target_pct": cfg.target_pct,
        "total": total,
        "wins": wins,
        "losses": losses,
        "time_exits": time_exits,
        "trail_stops": trail_stops,
        "partial_wins": partial_wins,
        "effective_wins": effective_wins,
        "win_rate": round(effective_wins / total * 100, 1) if total else 0,
        "total_pnl": round(total_pnl, 0),
        "avg_pnl": round(total_pnl / total, 0) if total else 0,
        "capital_deployed": round(total_capital, 0),
        "roi_pct": round(total_pnl / total_capital * 100, 2) if total_capital else 0,
        "skipped_grade": skipped_grade,
        "skipped_time": skipped_time,
        "skipped_rsi": skipped_rsi,
        "trades_taken": trades_taken,
    }


# ======================================================================
# Strategy definitions
# ======================================================================

def build_strategies() -> list[StrategyConfig]:
    strategies = []

    # --- GROUP 1: Baseline fixed targets (current best configs) ---
    for sl, tgt in [(7, 14), (10, 20), (8, 16), (10, 25), (10, 30)]:
        strategies.append(StrategyConfig(
            name=f"fixed_{sl}sl_{tgt}tgt",
            sl_pct=sl, target_pct=tgt,
        ))

    # --- GROUP 2: Asymmetric R:R (wider targets) ---
    for sl, tgt in [(5, 15), (5, 20), (5, 25), (7, 21), (7, 28), (7, 35)]:
        strategies.append(StrategyConfig(
            name=f"asym_{sl}sl_{tgt}tgt",
            sl_pct=sl, target_pct=tgt,
        ))

    # --- GROUP 3: Trailing stop variants ---
    for sl in [5, 7, 10]:
        for activation in [3, 5, 7]:
            for trail in [3, 5, 7]:
                strategies.append(StrategyConfig(
                    name=f"trail_{sl}sl_act{activation}_dist{trail}",
                    sl_pct=sl,
                    use_trailing_stop=True,
                    trail_activation_pct=activation,
                    trail_distance_pct=trail,
                    target_pct=0,  # no hard cap
                ))

    # --- GROUP 4: Trailing stop with hard target cap ---
    for sl, cap in [(7, 30), (7, 40), (7, 50), (10, 30), (10, 40), (10, 50)]:
        for activation, trail in [(5, 5), (5, 7), (7, 5)]:
            strategies.append(StrategyConfig(
                name=f"trailcap_{sl}sl_act{activation}_dist{trail}_cap{cap}",
                sl_pct=sl,
                use_trailing_stop=True,
                trail_activation_pct=activation,
                trail_distance_pct=trail,
                target_pct=cap,
            ))

    # --- GROUP 5: Partial exit strategies ---
    for sl in [5, 7, 10]:
        for partial_tgt in [7, 10, 15]:
            for trail in [3, 5, 7]:
                strategies.append(StrategyConfig(
                    name=f"partial_{sl}sl_book{partial_tgt}_trail{trail}",
                    sl_pct=sl,
                    use_partial_exit=True,
                    partial_target_pct=partial_tgt,
                    partial_book_pct=50.0,
                    partial_trail_pct=trail,
                ))

    # --- GROUP 6: Grade A+B only (applied on top of best configs) ---
    for sl, tgt in [(7, 14), (10, 20), (10, 30)]:
        strategies.append(StrategyConfig(
            name=f"gradeAB_fixed_{sl}sl_{tgt}tgt",
            sl_pct=sl, target_pct=tgt,
            accepted_grades=["A", "B"],
        ))
    for sl, activation, trail in [(7, 5, 5), (10, 5, 5), (10, 7, 5)]:
        strategies.append(StrategyConfig(
            name=f"gradeAB_trail_{sl}sl_act{activation}_dist{trail}",
            sl_pct=sl,
            use_trailing_stop=True,
            trail_activation_pct=activation,
            trail_distance_pct=trail,
            target_pct=0,
            accepted_grades=["A", "B"],
        ))

    # --- GROUP 7: Grade A only ---
    for sl, tgt in [(7, 14), (10, 20), (10, 30)]:
        strategies.append(StrategyConfig(
            name=f"gradeA_fixed_{sl}sl_{tgt}tgt",
            sl_pct=sl, target_pct=tgt,
            accepted_grades=["A"],
        ))

    # --- GROUP 8: Time-of-day filter (skip first 45 min) ---
    for sl, tgt in [(7, 14), (10, 20), (10, 30)]:
        strategies.append(StrategyConfig(
            name=f"time10_fixed_{sl}sl_{tgt}tgt",
            sl_pct=sl, target_pct=tgt,
            min_signal_time="10:00:00",
        ))
    for sl, activation, trail in [(7, 5, 5), (10, 5, 5)]:
        strategies.append(StrategyConfig(
            name=f"time10_trail_{sl}sl_act{activation}_dist{trail}",
            sl_pct=sl,
            use_trailing_stop=True,
            trail_activation_pct=activation,
            trail_distance_pct=trail,
            target_pct=0,
            min_signal_time="10:00:00",
        ))

    # --- GROUP 9: Combined best (Grade A+B + time filter + trailing) ---
    for sl in [5, 7, 10]:
        for activation, trail in [(3, 3), (3, 5), (5, 5), (5, 7), (7, 5)]:
            strategies.append(StrategyConfig(
                name=f"combo_AB_t10_trail_{sl}sl_act{activation}_dist{trail}",
                sl_pct=sl,
                use_trailing_stop=True,
                trail_activation_pct=activation,
                trail_distance_pct=trail,
                target_pct=0,
                accepted_grades=["A", "B"],
                min_signal_time="10:00:00",
            ))

    # --- GROUP 10: Combined with partial exit ---
    for sl in [5, 7, 10]:
        for partial_tgt in [7, 10]:
            for trail in [3, 5]:
                strategies.append(StrategyConfig(
                    name=f"combo_AB_t10_partial_{sl}sl_book{partial_tgt}_trail{trail}",
                    sl_pct=sl,
                    use_partial_exit=True,
                    partial_target_pct=partial_tgt,
                    partial_book_pct=50.0,
                    partial_trail_pct=trail,
                    accepted_grades=["A", "B"],
                    min_signal_time="10:00:00",
                ))

    # --- GROUP 11: Aggressive RSI filter (lower threshold) ---
    for sl, tgt in [(7, 14), (10, 20)]:
        strategies.append(StrategyConfig(
            name=f"rsi70_fixed_{sl}sl_{tgt}tgt",
            sl_pct=sl, target_pct=tgt,
            use_rsi_filter=True, rsi_overbought=70.0,
        ))

    # --- GROUP 12: No RSI filter (more trades) ---
    for sl in [7, 10]:
        for activation, trail in [(5, 5), (7, 5)]:
            strategies.append(StrategyConfig(
                name=f"norsi_trail_{sl}sl_act{activation}_dist{trail}",
                sl_pct=sl,
                use_trailing_stop=True,
                trail_activation_pct=activation,
                trail_distance_pct=trail,
                target_pct=0,
                use_rsi_filter=False,
            ))

    return strategies


# ======================================================================
# Output formatting
# ======================================================================

def print_results_table(results: list[dict], title: str = ""):
    if not results:
        print("  No results to display.")
        return

    print(f"\n{'=' * 150}")
    if title:
        print(f"  {title}")
        print(f"{'=' * 150}")

    hdr = (
        f"{'#':>3} {'Strategy':40s} │ "
        f"{'Trades':>6} {'W':>3} {'L':>3} {'T':>3} {'TS':>3} {'PW':>3} │ "
        f"{'WinRate':>7} │ {'Total PnL':>12} {'Avg PnL':>10} │ {'ROI%':>7} │ "
        f"{'SkipG':>5} {'SkipT':>5} {'SkipR':>5}"
    )
    sep = "─" * len(hdr)
    print(sep)
    print(hdr)
    print(sep)

    for i, r in enumerate(results, 1):
        roi_marker = " ★" if r["roi_pct"] >= 10.0 else ""
        print(
            f"{i:>3} {r['name']:40s} │ "
            f"{r['total']:>6} {r['wins']:>3} {r['losses']:>3} "
            f"{r['time_exits']:>3} {r['trail_stops']:>3} {r['partial_wins']:>3} │ "
            f"{r['win_rate']:>6.1f}% │ "
            f"{r['total_pnl']:>+12,.0f} {r['avg_pnl']:>+10,.0f} │ "
            f"{r['roi_pct']:>6.2f}%{roi_marker} │ "
            f"{r['skipped_grade']:>5} {r['skipped_time']:>5} {r['skipped_rsi']:>5}"
        )

    print(sep)


# ======================================================================
# Main
# ======================================================================

def main():
    start = time.time()
    run_dir = find_results_dir()
    print(f"Results dir: {run_dir}")

    phase2_trades = load_phase2_trades(run_dir)
    print(f"Loaded {len(phase2_trades)} Phase 2 trades")

    expiry_dates = tf.get_expiries()
    print(f"Found {len(expiry_dates)} expiry dates")

    # -- Fetch data once --
    print("\nFetching instruments and premium candles (one-time) via TickerFlow API...")
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
        entry_idx = signal_idx[-1] + 1
        if entry_idx >= len(candles):
            skipped += 1
            continue

        entry_premium = float(candles.iloc[entry_idx]["open"])
        if entry_premium <= 0:
            skipped += 1
            continue

        rsi_val = get_premium_rsi_at_signal(candles, signal_time, 14)

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
            "grade": trade.get("grade", "D"),
        })

    print(f"Prepared {len(prepared)} trades ({skipped} skipped)")

    # Grade distribution
    grade_counts = {}
    for t in prepared:
        g = t.get("grade", "?")
        grade_counts[g] = grade_counts.get(g, 0) + 1
    print(f"Grade distribution: {grade_counts}")

    # Time distribution
    time_dist = {}
    for t in prepared:
        h = t["signal_time"].strftime("%H:00")
        time_dist[h] = time_dist.get(h, 0) + 1
    print(f"Signal time distribution: {time_dist}")

    rsi_values = [t["rsi"] for t in prepared if t["rsi"] is not None]
    if rsi_values:
        print(f"RSI stats: min={min(rsi_values):.1f}  max={max(rsi_values):.1f}  "
              f"mean={sum(rsi_values)/len(rsi_values):.1f}")

    # -- Build and run strategies --
    strategies = build_strategies()
    print(f"\nRunning {len(strategies)} strategy combinations...")

    all_results = []
    for i, cfg in enumerate(strategies):
        result = run_strategy(prepared, cfg)
        all_results.append(result)
        if (i + 1) % 50 == 0:
            print(f"  ...completed {i + 1}/{len(strategies)}")

    print(f"Completed all {len(strategies)} strategies")

    # -- Sort by ROI and display --
    all_results.sort(key=lambda r: r["roi_pct"], reverse=True)

    # Top 30
    print_results_table(all_results[:30], "TOP 30 STRATEGIES BY ROI")

    # Strategies hitting 10%+ ROI
    ten_pct = [r for r in all_results if r["roi_pct"] >= 10.0]
    if ten_pct:
        print_results_table(ten_pct, f"STRATEGIES WITH 10%+ ROI ({len(ten_pct)} found)")
    else:
        print(f"\n  *** No strategies reached 10% ROI. Best: {all_results[0]['roi_pct']:.2f}% ({all_results[0]['name']}) ***")

    # Best per category
    categories = {
        "Fixed Target": [r for r in all_results if "fixed_" in r["name"] and "grade" not in r["name"] and "time" not in r["name"]],
        "Asymmetric R:R": [r for r in all_results if "asym_" in r["name"]],
        "Trailing Stop": [r for r in all_results if "trail_" in r["name"] and "partial" not in r["name"] and "combo" not in r["name"] and "grade" not in r["name"] and "time" not in r["name"] and "trailcap" not in r["name"]],
        "Trail + Cap": [r for r in all_results if "trailcap_" in r["name"]],
        "Partial Exit": [r for r in all_results if "partial_" in r["name"] and "combo" not in r["name"]],
        "Grade A+B Only": [r for r in all_results if "gradeAB_" in r["name"]],
        "Grade A Only": [r for r in all_results if "gradeA_" in r["name"]],
        "Time Filter 10:00": [r for r in all_results if "time10_" in r["name"]],
        "Combined (AB+Time+Trail)": [r for r in all_results if "combo_" in r["name"] and "partial" not in r["name"]],
        "Combined (AB+Time+Partial)": [r for r in all_results if "combo_" in r["name"] and "partial" in r["name"]],
        "No RSI Filter": [r for r in all_results if "norsi_" in r["name"]],
        "RSI 70 Filter": [r for r in all_results if "rsi70_" in r["name"]],
    }

    print(f"\n{'#' * 150}")
    print("  BEST STRATEGY PER CATEGORY")
    print(f"{'#' * 150}")
    for cat_name, cat_results in categories.items():
        if not cat_results:
            continue
        best = max(cat_results, key=lambda r: r["roi_pct"])
        print(
            f"  {cat_name:35s} │ {best['name']:40s} │ "
            f"ROI {best['roi_pct']:>7.2f}% │ PnL Rs {best['total_pnl']:>+10,.0f} │ "
            f"Trades {best['total']:>3} │ WR {best['win_rate']:>5.1f}%"
        )
    print(f"{'#' * 150}")

    # Overall best
    best = all_results[0]
    print(f"\n  >>> OVERALL BEST: {best['name']}")
    print(f"      ROI: {best['roi_pct']:.2f}% | PnL: Rs {best['total_pnl']:>+,.0f} | "
          f"Trades: {best['total']} | Win Rate: {best['win_rate']}%")

    # -- Save results --
    output = {
        "total_strategies_tested": len(strategies),
        "prepared_trades": len(prepared),
        "skipped": skipped,
        "grade_distribution": grade_counts,
        "signal_time_distribution": time_dist,
        "top_30": all_results[:30],
        "ten_pct_plus": ten_pct,
        "best_per_category": {
            cat: max(res, key=lambda r: r["roi_pct"])
            for cat, res in categories.items() if res
        },
        "overall_best": best,
        "all_results": all_results,
    }

    out_path = run_dir / "phase3_strategies_results.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {out_path}")

    elapsed = time.time() - start
    print(f"Strategy sweep completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
