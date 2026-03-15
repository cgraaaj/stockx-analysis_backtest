"""
Market Context Service
=====================
Evaluates broader market index trends (NIFTY, BANKNIFTY, NIFTY IT, etc.)
to filter or downgrade individual stock option signals that go against the
market direction.

Trend Methods
-------------
- "vwap"      : price vs index VWAP at signal time (no lookahead).
- "price_sma" : SMA crossover on index candles up to cutoff time.

When TRUNCATE_TO_SIGNAL_TIME is True, candles are clipped to the cutoff
timestamp, eliminating end-of-day lookahead bias in backtesting.

All parameters (indices, trend method, filter mode) are configurable via
MarketContextConfig in config/settings.py.
"""

import asyncio
import logging
import urllib.parse
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp
import pandas as pd

from ..config.settings import (
    api_config,
    market_context_config,
    processing_config,
    Grade,
)
from ..models.entities import IndexContext, StockPrediction


class MarketContextService:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cfg = market_context_config
        self._index_contexts: Dict[str, IndexContext] = {}

    @property
    def is_enabled(self) -> bool:
        return self.cfg.ENABLED

    @property
    def contexts(self) -> Dict[str, IndexContext]:
        return self._index_contexts

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def evaluate_market_context(
        self,
        trade_date: str,
        cutoff_time: str = None,
    ) -> Dict[str, IndexContext]:
        """Fetch index candles and compute trend for every configured index.

        Parameters
        ----------
        trade_date : str  "YYYY-MM-DD"
        cutoff_time : str, optional
            "HH:MM:SS" — if provided (and TRUNCATE_TO_SIGNAL_TIME is True),
            candles after this time are discarded before computing the trend.
            This prevents lookahead bias in backtesting.
        """
        if not self.is_enabled:
            return {}

        self.logger.info(
            f"Evaluating market context for {trade_date} "
            f"({len(self.cfg.INDICES)} indices"
            f"{', cutoff=' + cutoff_time if cutoff_time else ''})"
        )

        cutoff_ts = None
        if cutoff_time and self.cfg.TRUNCATE_TO_SIGNAL_TIME:
            cutoff_ts = pd.Timestamp(f"{trade_date} {cutoff_time}")

        async with aiohttp.ClientSession() as session:
            tasks = {
                name: self._fetch_and_evaluate(session, name, key, trade_date, cutoff_ts)
                for name, key in self.cfg.INDICES.items()
            }
            results = await asyncio.gather(*tasks.values(), return_exceptions=True)

            for (name, _), result in zip(tasks.items(), results):
                if isinstance(result, IndexContext):
                    self._index_contexts[name] = result
                    self.logger.info(
                        f"  {name}: {result.trend} "
                        f"(price={result.current_price:.2f}"
                        f"{', vwap=' + f'{result.sma_fast:.2f}' if self.cfg.INDEX_TREND_METHOD == 'vwap' else ''}"
                        f"{', fast=' + f'{result.sma_fast:.2f}, slow={result.sma_slow:.2f}' if self.cfg.INDEX_TREND_METHOD == 'price_sma' else ''})"
                    )
                elif isinstance(result, Exception):
                    self.logger.warning(f"  {name}: evaluation failed – {result}")

        return self._index_contexts

    def get_index_for_stock(self, stock_name: str) -> str:
        return self.cfg.SECTOR_INDEX_MAP.get(stock_name, self.cfg.PRIMARY_INDEX)

    def get_trend(self, index_name: str) -> Optional[str]:
        ctx = self._index_contexts.get(index_name)
        return ctx.trend if ctx else None

    # ------------------------------------------------------------------
    # Signal filtering
    # ------------------------------------------------------------------

    def filter_predictions(
        self, predictions: List[StockPrediction]
    ) -> List[StockPrediction]:
        if not self.is_enabled or not self._index_contexts:
            return predictions

        filtered: List[StockPrediction] = []
        grade_order = [g.value for g in Grade]

        for pred in predictions:
            idx_name = self.get_index_for_stock(pred.stock)
            trend = self.get_trend(idx_name)
            if trend is None:
                filtered.append(pred)
                continue

            conflict = self._is_conflicting(pred, trend)
            if not conflict:
                filtered.append(pred)
                continue

            if self.cfg.FILTER_MODE == "block":
                self.logger.debug(f"Blocked {pred.stock} ({pred.option_type}) – "
                                  f"conflicts with {idx_name} trend ({trend})")
                continue

            try:
                idx = grade_order.index(pred.grade)
                if idx < len(grade_order) - 1:
                    pred.grade = grade_order[idx + 1]
            except ValueError:
                pass
            filtered.append(pred)

        return filtered

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _fetch_and_evaluate(
        self,
        session: aiohttp.ClientSession,
        name: str,
        instrument_key: str,
        trade_date: str,
        cutoff_ts: pd.Timestamp = None,
    ) -> IndexContext:
        candles = await self._fetch_index_candles(session, instrument_key, trade_date)
        if candles.empty:
            return IndexContext(
                name=name, trend="neutral",
                current_price=0.0, sma_fast=0.0, sma_slow=0.0,
            )

        if cutoff_ts is not None:
            ts_col = candles["timestamp"]
            if ts_col.dt.tz is not None:
                candles = candles.copy()
                candles["timestamp"] = ts_col.dt.tz_localize(None)
            candles = candles[candles["timestamp"] <= cutoff_ts]
            if candles.empty:
                return IndexContext(
                    name=name, trend="neutral",
                    current_price=0.0, sma_fast=0.0, sma_slow=0.0,
                )

        method = self.cfg.INDEX_TREND_METHOD
        if method == "vwap":
            return self._compute_vwap_trend(name, candles)
        return self._compute_sma_trend(name, candles)

    async def _fetch_index_candles(
        self,
        session: aiohttp.ClientSession,
        instrument_key: str,
        trade_date: str,
    ) -> pd.DataFrame:
        encoded = urllib.parse.quote(instrument_key, safe="")
        url = (
            f"{api_config.UPSTOX_BASE_URL}/{encoded}"
            f"/1minute/{trade_date}/{trade_date}"
        )

        for attempt in range(processing_config.MAX_RETRIES):
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self._parse_candles(data)
                    self.logger.warning(f"HTTP {resp.status} for index candle {instrument_key}")
            except Exception as e:
                self.logger.error(f"Attempt {attempt+1} for {instrument_key}: {e}")
                if attempt < processing_config.MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** attempt)
        return pd.DataFrame()

    @staticmethod
    def _parse_candles(api_data: dict) -> pd.DataFrame:
        if api_data.get("status") != "success":
            return pd.DataFrame()
        candles = api_data.get("data", {}).get("candles", [])
        if not candles:
            return pd.DataFrame()
        df = pd.DataFrame(
            candles,
            columns=["timestamp", "open", "high", "low", "close", "volume", "oi"],
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
        df.sort_values("timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    # -- VWAP trend ---------------------------------------------------

    def _compute_vwap_trend(self, name: str, candles: pd.DataFrame) -> IndexContext:
        tp = (candles["high"] + candles["low"] + candles["close"]) / 3.0
        vol = candles["volume"].astype(float)
        total_vol = vol.sum()
        vwap = float((tp * vol).sum() / total_vol) if total_vol > 0 else float(tp.iloc[-1])

        last = candles.iloc[-1]
        price = float(last["close"])

        if price > vwap:
            trend = "bullish"
        elif price < vwap:
            trend = "bearish"
        else:
            trend = "neutral"

        spread = abs(price - vwap)
        confidence = min(spread / max(vwap, 1.0), 1.0)

        return IndexContext(
            name=name,
            trend=trend,
            current_price=price,
            sma_fast=vwap,
            sma_slow=vwap,
            timestamp=last["timestamp"],
            confidence=round(confidence, 4),
        )

    # -- SMA trend (legacy) -------------------------------------------

    def _compute_sma_trend(self, name: str, candles: pd.DataFrame) -> IndexContext:
        fast = self.cfg.SMA_FAST_PERIOD
        slow = self.cfg.SMA_SLOW_PERIOD

        candles = candles.copy()
        candles["sma_fast"] = candles["close"].rolling(window=fast, min_periods=1).mean()
        candles["sma_slow"] = candles["close"].rolling(window=slow, min_periods=1).mean()

        last = candles.iloc[-1]
        sma_f = float(last["sma_fast"])
        sma_s = float(last["sma_slow"])
        price = float(last["close"])

        if sma_f > sma_s:
            trend = "bullish"
        elif sma_f < sma_s:
            trend = "bearish"
        else:
            trend = "neutral"

        spread = abs(sma_f - sma_s)
        confidence = min(spread / max(sma_s, 1.0), 1.0)

        return IndexContext(
            name=name,
            trend=trend,
            current_price=price,
            sma_fast=sma_f,
            sma_slow=sma_s,
            timestamp=last["timestamp"],
            confidence=round(confidence, 4),
        )

    @staticmethod
    def _is_conflicting(pred: StockPrediction, index_trend: str) -> bool:
        """Call signals conflict with a bearish market; put signals with bullish."""
        if index_trend == "neutral":
            return False
        if pred.option_type == "call" and index_trend == "bearish":
            return True
        if pred.option_type == "put" and index_trend == "bullish":
            return True
        return False
