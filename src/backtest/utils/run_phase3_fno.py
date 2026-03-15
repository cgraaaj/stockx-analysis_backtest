"""
Phase 3: F&O Options Trading Simulation
========================================
Simulates actual option buying (CE for calls, PE for puts) with fixed
capital per trade (default 1L).  Uses ATM strike premiums fetched via
the TickerFlow API and tracks SL / target on the option premium.

Usage:
  python -m src.backtest.run_phase3_fno [results_dir]

  If results_dir is omitted, uses the most recent run in src/backtest/results/.

Data source: TickerFlow API via tickerflow_client (no direct DB access).
"""

import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# analysis_backtest root so that "from src.backtest.utils" etc. resolve
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

import pandas as pd

from src.backtest.utils import tickerflow_client as tf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("Phase3-FnO")

# ======================================================================
# Configuration
# ======================================================================

FNO_CONFIG = {
    "capital_per_trade": 100_000,
    "sl_pct_on_premium": 7.0,
    "target_pct_on_premium": 14.0,
    "max_exit_time": "15:15:00",
    "entry_candle_offset": 1,
    "use_rsi_filter": True,
    "rsi_period": 14,
    "rsi_overbought": 80,
    "entry_mode": "immediate",
}

# ======================================================================
# Helpers
# ======================================================================


def find_results_dir() -> Path:
    if len(sys.argv) > 1:
        return Path(sys.argv[1])
    # Results live under backtest/2025/results and backtest/2026/results (this file is in utils/)
    backtest_root = Path(__file__).resolve().parents[1]
    candidate_bases = [backtest_root / "2025" / "results", backtest_root / "2026" / "results"]
    all_dirs = []
    for base in candidate_bases:
        if base.exists():
            for d in base.iterdir():
                if d.is_dir():
                    all_dirs.append(d)
    if not all_dirs:
        raise FileNotFoundError(
            "No results directory found under src/backtest/2025/results or 2026/results. Run Phase 1 first."
        )
    return max(all_dirs, key=lambda d: d.stat().st_mtime)


def load_phase2_trades(run_dir: Path) -> list[dict]:
    p2 = run_dir / "phase2_summary.json"
    if not p2.exists():
        logger.error(
            f"phase2_summary.json not found in {run_dir}. "
            "Run Phase 2 first (python -m src.backtest.2025.run_phase2)."
        )
        return []
    with open(p2) as f:
        data = json.load(f)
    return data["trades"]


def pick_expiry_for_date(trade_date: str, expiry_dates: list[str]) -> str:
    from datetime import datetime as _dt
    trade_dt = _dt.strptime(trade_date, "%Y-%m-%d")
    ym = trade_dt.strftime("%Y-%m")
    for exp in expiry_dates:
        exp_dt = _dt.strptime(exp, "%Y-%m-%d")
        if exp_dt.strftime("%Y-%m") == ym and exp_dt >= trade_dt:
            return exp
    for exp in expiry_dates:
        if _dt.strptime(exp, "%Y-%m-%d") >= trade_dt:
            return exp
    return expiry_dates[-1] if expiry_dates else trade_date


# ======================================================================
# RSI computation
# ======================================================================

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def get_premium_rsi_at_signal(
    candles: pd.DataFrame,
    signal_time,
    period: int = 14,
) -> float | None:
    mask = candles["time_stamp"] <= signal_time
    pre = candles.loc[mask]
    if len(pre) < period + 1:
        return None
    rsi_series = compute_rsi(pre["close"], period)
    return float(rsi_series.iloc[-1]) if not pd.isna(rsi_series.iloc[-1]) else None


# ======================================================================
# Trade simulation on option premium
# ======================================================================

def simulate_option_trade(
    candles: pd.DataFrame,
    signal_time: datetime,
    entry_offset: int,
    sl_pct: float,
    target_pct: float,
    max_exit_time: str,
) -> dict | None:
    max_exit_ts = datetime.strptime(
        f"{signal_time.strftime('%Y-%m-%d')} {max_exit_time}",
        "%Y-%m-%d %H:%M:%S",
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
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])

        if ts >= max_exit_ts:
            return {
                "outcome": "time_exit",
                "exit_premium": close,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
            }

        if low <= sl_premium:
            return {
                "outcome": "sl_hit",
                "exit_premium": sl_premium,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
            }

        if high >= target_premium:
            return {
                "outcome": "target_hit",
                "exit_premium": target_premium,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
            }

    last = candles.iloc[-1]
    return {
        "outcome": "time_exit",
        "exit_premium": float(last["close"]),
        "exit_time": str(last["time_stamp"]),
        "holding_candles": len(candles) - 1 - entry_idx,
    }


def simulate_staged_entry_trade(
    candles: pd.DataFrame,
    signal_time: datetime,
    sl_pct: float,
    target_pct: float,
    max_exit_time: str,
    rsi_period: int = 14,
    rsi_entry_threshold: float = 60,
    vol_multiplier: float = 1.2,
    vol_lookback: int = 10,
    max_wait_candles: int = 60,
    entry_deadline: str = "14:00:00",
) -> dict | None:
    """
    Staged entry: instead of entering at signal+1, scan forward from signal
    and wait for optimal conditions:
      1) Premium RSI drops to <= rsi_entry_threshold (cooldown from initial move)
      2) Volume on current candle >= vol_multiplier * rolling avg
      3) Fallback: enter after max_wait_candles even if conditions aren't perfect
      4) Hard deadline: skip entirely if no entry by entry_deadline
    After entry, standard SL/target/time-exit logic applies.
    """
    date_str = signal_time.strftime("%Y-%m-%d")
    max_exit_ts = datetime.strptime(f"{date_str} {max_exit_time}", "%Y-%m-%d %H:%M:%S")
    deadline_ts = datetime.strptime(f"{date_str} {entry_deadline}", "%Y-%m-%d %H:%M:%S")

    signal_idx = candles[candles["time_stamp"] <= signal_time].index
    if signal_idx.empty:
        return None
    scan_start = signal_idx[-1] + 1
    if scan_start >= len(candles):
        return None

    rsi_full = compute_rsi(candles["close"], rsi_period)
    vol_series = candles["volume"].astype(float)

    entry_idx = None
    entry_reason = None
    candles_waited = 0

    for i in range(scan_start, len(candles)):
        ts = candles.iloc[i]["time_stamp"]

        if ts >= deadline_ts:
            return {
                "outcome": "no_entry",
                "exit_premium": 0,
                "exit_time": str(ts),
                "holding_candles": 0,
                "entry_premium": 0,
                "entry_time": str(ts),
                "entry_reason": "deadline_reached",
                "candles_waited": candles_waited,
            }

        candles_waited += 1
        current_rsi = float(rsi_full.iloc[i]) if not pd.isna(rsi_full.iloc[i]) else 50.0

        lookback_start = max(0, i - vol_lookback)
        avg_vol = vol_series.iloc[lookback_start:i].mean() if i > lookback_start else 0
        current_vol = float(vol_series.iloc[i])
        vol_ok = avg_vol > 0 and current_vol >= vol_multiplier * avg_vol

        rsi_ok = current_rsi <= rsi_entry_threshold

        if rsi_ok and vol_ok:
            entry_idx = i
            entry_reason = "rsi_vol_confirmed"
            break

        if rsi_ok and candles_waited >= 5:
            entry_idx = i
            entry_reason = "rsi_confirmed"
            break

        if candles_waited >= max_wait_candles:
            entry_idx = i
            entry_reason = "max_wait_fallback"
            break

    if entry_idx is None:
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
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])

        if ts >= max_exit_ts:
            return {
                "outcome": "time_exit",
                "exit_premium": close,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
                "entry_premium": entry_premium,
                "entry_time": str(entry_time),
                "entry_reason": entry_reason,
                "candles_waited": candles_waited,
            }

        if low <= sl_premium:
            return {
                "outcome": "sl_hit",
                "exit_premium": sl_premium,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
                "entry_premium": entry_premium,
                "entry_time": str(entry_time),
                "entry_reason": entry_reason,
                "candles_waited": candles_waited,
            }

        if high >= target_premium:
            return {
                "outcome": "target_hit",
                "exit_premium": target_premium,
                "exit_time": str(ts),
                "holding_candles": i - entry_idx,
                "entry_premium": entry_premium,
                "entry_time": str(entry_time),
                "entry_reason": entry_reason,
                "candles_waited": candles_waited,
            }

    last = candles.iloc[-1]
    return {
        "outcome": "time_exit",
        "exit_premium": float(last["close"]),
        "exit_time": str(last["time_stamp"]),
        "holding_candles": len(candles) - 1 - entry_idx,
        "entry_premium": entry_premium,
        "entry_time": str(entry_time),
        "entry_reason": entry_reason,
        "candles_waited": candles_waited,
    }


# ======================================================================
# Main
# ======================================================================

def main():
    start = time.time()

    run_dir = find_results_dir()
    logger.info(f"Using results from: {run_dir}")

    cfg = dict(FNO_CONFIG)
    cfg_file = run_dir / "phase3_fno_config.json"
    if cfg_file.exists():
        with open(cfg_file) as f:
            cfg.update(json.load(f))

    entry_mode = cfg.get("entry_mode", "immediate")
    if entry_mode == "staged":
        cfg.setdefault("staged_rsi_entry_threshold", 60)
        cfg.setdefault("staged_vol_multiplier", 1.2)
        cfg.setdefault("staged_vol_lookback", 10)
        cfg.setdefault("staged_max_wait_candles", 60)
        cfg.setdefault("staged_entry_deadline", "14:00:00")

    logger.info("=" * 60)
    logger.info("Phase 3: F&O Options Trading Simulation")
    logger.info("=" * 60)
    logger.info(f"Capital/trade   = Rs {cfg['capital_per_trade']:,}")
    logger.info(f"SL on premium   = {cfg['sl_pct_on_premium']}%")
    logger.info(f"Target premium  = {cfg['target_pct_on_premium']}%")
    logger.info(f"Hard exit       = {cfg['max_exit_time']}")
    logger.info(f"Entry mode      = {entry_mode}")
    if entry_mode == "staged":
        logger.info(f"  RSI threshold = {cfg['staged_rsi_entry_threshold']}")
        logger.info(f"  Vol multiplier= {cfg['staged_vol_multiplier']}x")
        logger.info(f"  Max wait      = {cfg['staged_max_wait_candles']} candles")
        logger.info(f"  Deadline      = {cfg['staged_entry_deadline']}")
    else:
        logger.info(f"Entry offset    = {cfg['entry_candle_offset']} candle(s)")

    phase2_trades = load_phase2_trades(run_dir)
    if not phase2_trades:
        logger.error("No Phase 2 trades found. Cannot run Phase 3.")
        return
    logger.info(f"Loaded {len(phase2_trades)} trades from Phase 2")
    logger.info(f"TickerFlow API  = {tf._BASE_URL}")

    expiry_dates = tf.get_expiries()
    logger.info(f"Found {len(expiry_dates)} expiry dates")

    results = []
    skipped = 0
    total_capital_deployed = 0

    for trade in phase2_trades:
        stock = trade["stock"]
        opt_type = trade["option_type"]
        signal_time_str = trade["signal_time"]
        signal_time = datetime.strptime(signal_time_str, "%Y-%m-%d %H:%M:%S")
        trade_date = signal_time.strftime("%Y-%m-%d")
        spot_price = trade["entry_price"]

        expiry = pick_expiry_for_date(trade_date, expiry_dates)

        inst = tf.find_atm_instrument(stock, opt_type, spot_price, expiry)
        if inst is None:
            logger.warning(f"  {stock} ({trade_date}): no ATM {opt_type.upper()} instrument found")
            skipped += 1
            continue

        candles = tf.get_ticks(
            instrument_id=inst["instrument_seq"],
            start=f"{trade_date}T09:15:00",
            end=f"{trade_date}T15:30:00",
        )
        if candles.empty:
            logger.warning(
                f"  {stock} ({trade_date}): no premium candles for "
                f"{inst['trading_symbol']}"
            )
            skipped += 1
            continue

        premium_rsi = get_premium_rsi_at_signal(
            candles, signal_time, cfg.get("rsi_period", 14)
        )

        if entry_mode == "staged":
            sim = simulate_staged_entry_trade(
                candles, signal_time,
                sl_pct=cfg["sl_pct_on_premium"],
                target_pct=cfg["target_pct_on_premium"],
                max_exit_time=cfg["max_exit_time"],
                rsi_period=cfg.get("rsi_period", 14),
                rsi_entry_threshold=cfg["staged_rsi_entry_threshold"],
                vol_multiplier=cfg["staged_vol_multiplier"],
                vol_lookback=cfg["staged_vol_lookback"],
                max_wait_candles=cfg["staged_max_wait_candles"],
                entry_deadline=cfg["staged_entry_deadline"],
            )
            if sim is None or sim.get("outcome") == "no_entry":
                reason = sim["entry_reason"] if sim else "no_candles"
                logger.info(
                    f"  {stock:15s} | {opt_type:4s} | {trade_date} | "
                    f"NO ENTRY ({reason})"
                )
                skipped += 1
                continue
            entry_premium = sim["entry_premium"]
        else:
            if cfg.get("use_rsi_filter", False) and premium_rsi is not None:
                if premium_rsi > cfg.get("rsi_overbought", 80):
                    logger.info(
                        f"  {stock:15s} | {opt_type:4s} | {trade_date} | "
                        f"RSI {premium_rsi:.1f} > {cfg['rsi_overbought']} → SKIPPED"
                    )
                    skipped += 1
                    continue

            sim = simulate_option_trade(
                candles, signal_time,
                cfg["entry_candle_offset"],
                cfg["sl_pct_on_premium"],
                cfg["target_pct_on_premium"],
                cfg["max_exit_time"],
            )
            if sim is None:
                logger.warning(f"  {stock} ({trade_date}): simulation failed")
                skipped += 1
                continue

            signal_idx = candles[candles["time_stamp"] <= signal_time].index
            entry_premium = float(candles.iloc[signal_idx[-1] + cfg["entry_candle_offset"]]["open"])

        lot_size = inst["lot_size"]
        cost_per_lot = entry_premium * lot_size
        if cost_per_lot <= 0:
            skipped += 1
            continue

        num_lots = max(1, math.floor(cfg["capital_per_trade"] / cost_per_lot))
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
            "sl_premium": round(entry_premium * (1 - cfg["sl_pct_on_premium"] / 100), 2),
            "target_premium": round(entry_premium * (1 + cfg["target_pct_on_premium"] / 100), 2),
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

        rsi_str = f"RSI {premium_rsi:>5.1f}" if premium_rsi is not None else "RSI   N/A"
        waited_str = f"W{sim.get('candles_waited', 0):>3d}" if entry_mode == "staged" else ""
        reason_str = f"({sim.get('entry_reason', '')})" if entry_mode == "staged" else ""
        logger.info(
            f"  {stock:15s} | {opt_type:4s} | {trade_date} | "
            f"Strike {inst['strike_price']:>8.0f} | "
            f"Prem {entry_premium:>7.2f} → {exit_premium:>7.2f} | "
            f"{rsi_str} | "
            f"{sim['outcome']:10s} | "
            f"Lots {num_lots} x {lot_size} | "
            f"Rs {rupee_pnl:>+10,.0f} {waited_str} {reason_str}"
        )

    wins = [r for r in results if r["outcome"] == "target_hit"]
    losses = [r for r in results if r["outcome"] == "sl_hit"]
    time_exits = [r for r in results if r["outcome"] == "time_exit"]
    total_pnl = sum(r["rupee_pnl"] for r in results)
    win_rate = (len(wins) / len(results) * 100) if results else 0

    logger.info("=" * 60)
    logger.info("PHASE 3 F&O RESULTS")
    logger.info("=" * 60)
    logger.info(f"  Entry mode      : {entry_mode}")
    if entry_mode == "staged":
        reasons = {}
        for r in results:
            reason = r.get("entry_reason", "unknown")
            reasons[reason] = reasons.get(reason, 0) + 1
        avg_wait = sum(r.get("candles_waited", 0) for r in results) / len(results) if results else 0
        logger.info(f"  Avg wait        : {avg_wait:.1f} candles")
        logger.info(f"  Entry reasons   : {reasons}")
    elif cfg.get("use_rsi_filter"):
        logger.info(f"  RSI filter      : ON (period={cfg['rsi_period']}, overbought>{cfg['rsi_overbought']})")
    logger.info(f"  Total trades    : {len(results)}")
    logger.info(f"  Skipped         : {skipped}")
    logger.info(f"  Target hit (W)  : {len(wins)}")
    logger.info(f"  SL hit (L)      : {len(losses)}")
    logger.info(f"  Time exit (T)   : {len(time_exits)}")
    logger.info(f"  Win rate        : {win_rate:.1f}%")
    logger.info(f"  Total PnL       : Rs {total_pnl:>+,.0f}")
    logger.info(f"  Avg PnL/trade   : Rs {total_pnl / len(results):>+,.0f}" if results else "")
    logger.info(f"  Capital deployed: Rs {total_capital_deployed:>,.0f}")
    logger.info(f"  ROI             : {(total_pnl / total_capital_deployed * 100):.2f}%" if total_capital_deployed > 0 else "")

    win_pnl = sum(r["rupee_pnl"] for r in wins)
    loss_pnl = sum(r["rupee_pnl"] for r in losses)
    te_pnl = sum(r["rupee_pnl"] for r in time_exits)
    logger.info(f"  Win total       : Rs {win_pnl:>+,.0f}")
    logger.info(f"  Loss total      : Rs {loss_pnl:>+,.0f}")
    logger.info(f"  Time exit total : Rs {te_pnl:>+,.0f}")

    def _serialize(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    summary = {
        "config": cfg,
        "total_trades": len(results),
        "skipped": skipped,
        "wins": len(wins),
        "losses": len(losses),
        "time_exits": len(time_exits),
        "win_rate_pct": round(win_rate, 1),
        "total_rupee_pnl": round(total_pnl, 2),
        "avg_pnl_per_trade": round(total_pnl / len(results), 2) if results else 0,
        "total_capital_deployed": round(total_capital_deployed, 2),
        "roi_pct": round(total_pnl / total_capital_deployed * 100, 2) if total_capital_deployed > 0 else 0,
        "win_pnl": round(win_pnl, 2),
        "loss_pnl": round(loss_pnl, 2),
        "time_exit_pnl": round(te_pnl, 2),
        "trades": results,
    }

    with open(run_dir / "phase3_fno_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=_serialize)

    logger.info(f"\nResults saved to {run_dir / 'phase3_fno_summary.json'}")
    elapsed = time.time() - start
    logger.info(f"Phase 3 completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
