"""Shared option trade simulation and RSI computation.

Simulation loops use pre-extracted NumPy arrays instead of row-by-row
.iloc access, giving ~5-10x speedup on the CPU-bound portion.
"""

from datetime import datetime

import numpy as np
import pandas as pd


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


def simulate_option_trade(
    candles: pd.DataFrame,
    signal_time: datetime,
    entry_offset: int,
    sl_pct: float,
    target_pct: float,
    max_exit_time: str,
) -> dict | None:
    """Simulate a single option trade on premium candles.

    Uses pre-extracted NumPy arrays for the hot loop.
    """
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

    entry_premium = float(candles.iat[entry_idx, candles.columns.get_loc("open")])
    entry_time = candles.iat[entry_idx, candles.columns.get_loc("time_stamp")]

    if entry_premium <= 0:
        return None

    sl_premium = entry_premium * (1 - sl_pct / 100.0)
    target_premium = entry_premium * (1 + target_pct / 100.0)

    ts_arr = candles["time_stamp"].values[entry_idx + 1:]
    high_arr = candles["high"].values[entry_idx + 1:].astype(np.float64)
    low_arr = candles["low"].values[entry_idx + 1:].astype(np.float64)
    close_arr = candles["close"].values[entry_idx + 1:].astype(np.float64)

    max_exit_np = np.datetime64(max_exit_ts)

    for j in range(len(ts_arr)):
        ts = ts_arr[j]
        if ts >= max_exit_np:
            return dict(
                outcome="time_exit", entry_premium=entry_premium,
                exit_premium=float(close_arr[j]),
                entry_time=entry_time, exit_time=pd.Timestamp(ts),
                sl_premium=sl_premium, target_premium=target_premium,
                holding_candles=j + 1,
            )
        if low_arr[j] <= sl_premium:
            return dict(
                outcome="sl_hit", entry_premium=entry_premium,
                exit_premium=sl_premium,
                entry_time=entry_time, exit_time=pd.Timestamp(ts),
                sl_premium=sl_premium, target_premium=target_premium,
                holding_candles=j + 1,
            )
        if high_arr[j] >= target_premium:
            return dict(
                outcome="target_hit", entry_premium=entry_premium,
                exit_premium=target_premium,
                entry_time=entry_time, exit_time=pd.Timestamp(ts),
                sl_premium=sl_premium, target_premium=target_premium,
                holding_candles=j + 1,
            )

    if len(ts_arr) == 0:
        return dict(
            outcome="time_exit", entry_premium=entry_premium,
            exit_premium=entry_premium, entry_time=entry_time,
            exit_time=entry_time, sl_premium=sl_premium,
            target_premium=target_premium, holding_candles=0,
        )
    return dict(
        outcome="time_exit", entry_premium=entry_premium,
        exit_premium=float(close_arr[-1]),
        entry_time=entry_time, exit_time=pd.Timestamp(ts_arr[-1]),
        sl_premium=sl_premium, target_premium=target_premium,
        holding_candles=len(ts_arr),
    )


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
    """Staged entry: scan forward from signal for RSI/volume confirmation.

    Uses NumPy arrays for the hot simulation loop.
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
    rsi_vals = rsi_full.values
    vol_arr = candles["volume"].values.astype(np.float64)
    ts_all = candles["time_stamp"].values
    deadline_np = np.datetime64(deadline_ts)

    entry_idx = None
    entry_reason = None
    candles_waited = 0

    for i in range(scan_start, len(candles)):
        ts = ts_all[i]

        if ts >= deadline_np:
            return dict(
                outcome="no_entry", exit_premium=0, exit_time=str(pd.Timestamp(ts)),
                holding_candles=0, entry_premium=0, entry_time=str(pd.Timestamp(ts)),
                entry_reason="deadline_reached", candles_waited=candles_waited,
            )

        candles_waited += 1
        current_rsi = float(rsi_vals[i]) if not np.isnan(rsi_vals[i]) else 50.0

        lookback_start = max(0, i - vol_lookback)
        avg_vol = float(vol_arr[lookback_start:i].mean()) if i > lookback_start else 0.0
        current_vol = float(vol_arr[i])
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

    entry_premium = float(candles.iat[entry_idx, candles.columns.get_loc("open")])
    entry_time = candles.iat[entry_idx, candles.columns.get_loc("time_stamp")]

    if entry_premium <= 0:
        return None

    sl_premium = entry_premium * (1 - sl_pct / 100.0)
    target_premium = entry_premium * (1 + target_pct / 100.0)

    ts_arr = candles["time_stamp"].values[entry_idx + 1:]
    high_arr = candles["high"].values[entry_idx + 1:].astype(np.float64)
    low_arr = candles["low"].values[entry_idx + 1:].astype(np.float64)
    close_arr = candles["close"].values[entry_idx + 1:].astype(np.float64)
    max_exit_np = np.datetime64(max_exit_ts)

    for j in range(len(ts_arr)):
        ts = ts_arr[j]
        if ts >= max_exit_np:
            return dict(
                outcome="time_exit", exit_premium=float(close_arr[j]),
                exit_time=str(pd.Timestamp(ts)),
                holding_candles=j + 1, entry_premium=entry_premium,
                entry_time=str(entry_time), entry_reason=entry_reason,
                candles_waited=candles_waited,
            )
        if low_arr[j] <= sl_premium:
            return dict(
                outcome="sl_hit", exit_premium=sl_premium,
                exit_time=str(pd.Timestamp(ts)),
                holding_candles=j + 1, entry_premium=entry_premium,
                entry_time=str(entry_time), entry_reason=entry_reason,
                candles_waited=candles_waited,
            )
        if high_arr[j] >= target_premium:
            return dict(
                outcome="target_hit", exit_premium=target_premium,
                exit_time=str(pd.Timestamp(ts)),
                holding_candles=j + 1, entry_premium=entry_premium,
                entry_time=str(entry_time), entry_reason=entry_reason,
                candles_waited=candles_waited,
            )

    if len(ts_arr) == 0:
        return dict(
            outcome="time_exit", exit_premium=entry_premium,
            exit_time=str(entry_time),
            holding_candles=0, entry_premium=entry_premium,
            entry_time=str(entry_time), entry_reason=entry_reason,
            candles_waited=candles_waited,
        )
    return dict(
        outcome="time_exit", exit_premium=float(close_arr[-1]),
        exit_time=str(pd.Timestamp(ts_arr[-1])),
        holding_candles=len(ts_arr),
        entry_premium=entry_premium, entry_time=str(entry_time),
        entry_reason=entry_reason, candles_waited=candles_waited,
    )
