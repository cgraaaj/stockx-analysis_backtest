"""
Trade Execution Service
======================
Computes entry, stop-loss, target based on configurable R:R and SL methods,
then simulates the trade forward on OHLC candles to determine outcome.

SL Methods
----------
- vwap        : SL = VWAP at signal time (+/- buffer). Primary method.
- candle_low  : SL = signal candle low (calls) or signal candle high (puts)
- percentage  : SL = entry +/- (entry * SL_PERCENTAGE / 100)
- atr         : SL = entry +/- ATR(period) * multiplier

VWAP Bias
---------
When USE_VWAP_BIAS is True, the trade is only taken if price is on the
correct side of VWAP (above for calls, below for puts).

Intraday Hard Exit
------------------
MAX_EXIT_TIME forces an exit before market close regardless of target/SL.

All parameters are in StrategyConfig (config/settings.py).
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from ..config.settings import strategy_config
from ..models.entities import StockPrediction, TradeSignal


class TradeExecutionService:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cfg = strategy_config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate_trades(
        self,
        predictions: List[StockPrediction],
        ohlc_by_stock: Dict[str, pd.DataFrame],
    ) -> List[TradeSignal]:
        """For each prediction, compute entry/SL/target and simulate outcome."""
        signals: List[TradeSignal] = []
        for pred in predictions:
            ohlc = ohlc_by_stock.get(pred.stock)
            if ohlc is None or ohlc.empty:
                continue

            signal = self._build_trade_signal(pred, ohlc)
            if signal is not None:
                signals.append(signal)
        self.logger.info(f"Evaluated {len(signals)}/{len(predictions)} trade signals")
        return signals

    # ------------------------------------------------------------------
    # VWAP
    # ------------------------------------------------------------------

    @staticmethod
    def compute_vwap(ohlc: pd.DataFrame, up_to_ts: pd.Timestamp = None) -> float:
        """Cumulative VWAP = sum(typical_price * volume) / sum(volume).
        Uses candles from market open up to *up_to_ts* (inclusive).
        """
        df = ohlc.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        if up_to_ts is not None:
            df = df[df["timestamp"] <= up_to_ts]
        if df.empty:
            return 0.0

        tp = (df["high"] + df["low"] + df["close"]) / 3.0
        vol = df["volume"].astype(float)
        total_vol = vol.sum()
        if total_vol == 0:
            return float(tp.iloc[-1])
        return float((tp * vol).sum() / total_vol)

    def check_vwap_bias(
        self, pred: StockPrediction, ohlc: pd.DataFrame
    ) -> Optional[float]:
        """Return VWAP if bias confirms the signal direction, else None."""
        signal_ts = pd.Timestamp(pred.timestamp)
        vwap = self.compute_vwap(ohlc, up_to_ts=signal_ts)
        if vwap <= 0:
            return None

        mask = ohlc["timestamp"] <= signal_ts
        if not mask.any():
            return None
        price_at_signal = float(ohlc.loc[mask, "close"].iloc[-1])

        if pred.option_type == "call" and price_at_signal > vwap:
            return vwap
        if pred.option_type == "put" and price_at_signal < vwap:
            return vwap

        self.logger.debug(
            f"VWAP bias rejected {pred.stock} ({pred.option_type}): "
            f"price={price_at_signal:.2f}, VWAP={vwap:.2f}"
        )
        return None

    # ------------------------------------------------------------------
    # Trade signal builder
    # ------------------------------------------------------------------

    def _build_trade_signal(
        self, pred: StockPrediction, ohlc: pd.DataFrame, vwap: float = None,
    ) -> Optional[TradeSignal]:
        try:
            ohlc = ohlc.copy()
            ohlc["timestamp"] = pd.to_datetime(ohlc["timestamp"])
            ohlc.sort_values("timestamp", inplace=True)
            ohlc.reset_index(drop=True, inplace=True)

            # VWAP bias gate
            if self.cfg.USE_VWAP_BIAS:
                if vwap is None:
                    vwap = self.check_vwap_bias(pred, ohlc)
                if vwap is None:
                    return None

            entry_idx = self._find_entry_index(pred, ohlc)
            if entry_idx is None:
                return None

            entry_row = ohlc.iloc[entry_idx]
            entry_price = float(entry_row["close"])

            sl = self._compute_stop_loss(pred, ohlc, entry_idx, entry_price, vwap)
            risk = abs(entry_price - sl)
            if risk < self.cfg.MIN_RISK_POINTS:
                return None

            reward = risk * self.cfg.REWARD_RATIO
            if pred.option_type == "call":
                target = entry_price + reward
            else:
                target = entry_price - reward

            signal = TradeSignal(
                stock=pred.stock,
                signal_time=pred.timestamp,
                option_type=pred.option_type,
                grade=pred.grade,
                tn_ratio=pred.tn_ratio,
                entry_price=round(entry_price, 2),
                stop_loss=round(sl, 2),
                target=round(target, 2),
                risk=round(risk, 2),
                reward=round(reward, 2),
                instrument_key=None,
                market_trend=pred.market_trend,
                trend_alignment=pred.trend_alignment,
                vwap_at_signal=round(vwap, 2) if vwap else None,
                sl_method_used=self.cfg.SL_METHOD,
            )

            self._simulate_outcome(signal, ohlc, entry_idx)
            return signal
        except Exception as e:
            self.logger.error(f"Error building trade signal for {pred.stock}: {e}")
            return None

    def _find_entry_index(
        self, pred: StockPrediction, ohlc: pd.DataFrame
    ) -> Optional[int]:
        signal_ts = pd.Timestamp(pred.timestamp)
        mask = ohlc["timestamp"] >= signal_ts
        matching = ohlc.index[mask]
        if matching.empty:
            return None
        first = matching[0]
        target_idx = first + self.cfg.ENTRY_CANDLE_OFFSET
        if target_idx >= len(ohlc):
            return None
        return target_idx

    # ------------------------------------------------------------------
    # Stop-loss computation
    # ------------------------------------------------------------------

    def _compute_stop_loss(
        self,
        pred: StockPrediction,
        ohlc: pd.DataFrame,
        entry_idx: int,
        entry_price: float,
        vwap: float = None,
    ) -> float:
        method = self.cfg.SL_METHOD

        if method == "vwap" and vwap is not None and vwap > 0:
            buf = vwap * (self.cfg.VWAP_SL_BUFFER_PCT / 100.0)
            if pred.option_type == "call":
                return vwap - buf
            return vwap + buf

        if method == "candle_low":
            row = ohlc.iloc[entry_idx]
            if pred.option_type == "call":
                return float(row["low"])
            return float(row["high"])

        if method == "percentage":
            delta = entry_price * (self.cfg.SL_PERCENTAGE / 100.0)
            if pred.option_type == "call":
                return entry_price - delta
            return entry_price + delta

        if method == "atr":
            atr = self._calculate_atr(ohlc, entry_idx)
            delta = atr * self.cfg.ATR_MULTIPLIER
            if pred.option_type == "call":
                return entry_price - delta
            return entry_price + delta

        return entry_price * 0.99 if pred.option_type == "call" else entry_price * 1.01

    def _calculate_atr(self, ohlc: pd.DataFrame, up_to_idx: int) -> float:
        period = self.cfg.ATR_PERIOD
        start = max(0, up_to_idx - period)
        subset = ohlc.iloc[start:up_to_idx + 1].copy()
        if subset.empty:
            return 0.0

        subset["prev_close"] = subset["close"].shift(1)
        subset["tr"] = np.maximum(
            subset["high"] - subset["low"],
            np.maximum(
                (subset["high"] - subset["prev_close"]).abs(),
                (subset["low"] - subset["prev_close"]).abs(),
            ),
        )
        return float(subset["tr"].mean())

    # ------------------------------------------------------------------
    # Trade simulation
    # ------------------------------------------------------------------

    def _simulate_outcome(
        self, signal: TradeSignal, ohlc: pd.DataFrame, entry_idx: int
    ) -> None:
        """Walk forward from entry to determine target hit, SL hit, or time exit.
        Respects MAX_EXIT_TIME for intraday hard cutoff.
        """
        max_candles = self.cfg.MAX_HOLDING_CANDLES
        end_idx = min(entry_idx + max_candles + 1, len(ohlc))
        max_exit_time = self._parse_exit_time(signal.signal_time)

        for i in range(entry_idx + 1, end_idx):
            row = ohlc.iloc[i]
            candle_ts = pd.Timestamp(row["timestamp"])
            candle_high = float(row["high"])
            candle_low = float(row["low"])

            if max_exit_time and candle_ts >= max_exit_time:
                self._set_exit(signal, row, i - entry_idx, "time_exit")
                return

            if signal.option_type == "call":
                if candle_low <= signal.stop_loss:
                    self._set_exit(signal, row, i - entry_idx, "sl_hit",
                                   forced_price=signal.stop_loss)
                    return
                if candle_high >= signal.target:
                    self._set_exit(signal, row, i - entry_idx, "target_hit",
                                   forced_price=signal.target)
                    return
            else:
                if candle_high >= signal.stop_loss:
                    self._set_exit(signal, row, i - entry_idx, "sl_hit",
                                   forced_price=signal.stop_loss)
                    return
                if candle_low <= signal.target:
                    self._set_exit(signal, row, i - entry_idx, "target_hit",
                                   forced_price=signal.target)
                    return

        last = ohlc.iloc[min(end_idx - 1, len(ohlc) - 1)]
        self._set_exit(signal, last, end_idx - entry_idx - 1, "time_exit")

    def _set_exit(
        self, signal: TradeSignal, row, holding: int, outcome: str,
        forced_price: float = None,
    ) -> None:
        signal.outcome = outcome
        exit_p = forced_price if forced_price is not None else float(row["close"])
        signal.exit_price = round(exit_p, 2)
        signal.exit_time = row["timestamp"]
        signal.holding_candles = holding
        if signal.option_type == "call":
            signal.pnl_points = round(exit_p - signal.entry_price, 2)
        else:
            signal.pnl_points = round(signal.entry_price - exit_p, 2)

    def _parse_exit_time(self, signal_time) -> Optional[pd.Timestamp]:
        """Build a hard-exit timestamp from MAX_EXIT_TIME on the signal's date."""
        exit_str = self.cfg.MAX_EXIT_TIME
        if not exit_str:
            return None
        try:
            dt = pd.Timestamp(signal_time)
            parts = exit_str.split(":")
            return dt.replace(
                hour=int(parts[0]), minute=int(parts[1]),
                second=int(parts[2]) if len(parts) > 2 else 0,
            )
        except Exception:
            return None
