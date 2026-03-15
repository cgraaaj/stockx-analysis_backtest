"""
Analysis Service
===============
Core option analysis business logic: OI action classification, trend analysis,
grading, and prediction ranking.

All vectorized operations are preserved from the original optimized code.
The only structural change is that CE/PE pairs are now identified by
instrument_seq (integer) instead of UUID, matching the ticker_ts schema.
"""

import pandas as pd
import numpy as np
import math
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from ..config.settings import (
    analysis_config, trading_config, market_actions,
    processing_config, Grade,
)
from ..models.entities import (
    StockPrediction, OptionAnalysis, TimeAnalysisData,
    TrendAnalysisResult, CEPEPair, PredictionResult,
)

_USE_OI_WEIGHT = analysis_config.USE_OI_MAGNITUDE_WEIGHTING
_OI_BLEND = analysis_config.OI_WEIGHT_BLEND


class AnalysisService:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # OI action classification (vectorized)
    # ------------------------------------------------------------------

    def calculate_oi_actions_vectorized(self, df: pd.DataFrame, suffix: str) -> pd.Series:
        price_col = f"ltp_change_{suffix}"
        oi_col = f"open_interest_change_{suffix}"

        if price_col not in df.columns or oi_col not in df.columns:
            self.logger.warning(f"Missing columns: {price_col}, {oi_col}")
            return pd.Series([""] * len(df), index=df.index)

        conditions = [
            (df[price_col] > 0) & (df[oi_col] > 0),
            (df[price_col] > 0) & (df[oi_col] < 0),
            (df[price_col] < 0) & (df[oi_col] > 0),
            (df[price_col] < 0) & (df[oi_col] < 0),
        ]
        choices = ["Long Buildup", "Short Cover", "Short Buildup", "Long Unwind"]
        return np.select(conditions, choices, default="")

    # ------------------------------------------------------------------
    # Trend analysis (vectorized)
    # ------------------------------------------------------------------

    def analyze_trend_vectorized(self, ticker_cepe_df: pd.DataFrame) -> pd.DataFrame:
        if ticker_cepe_df.empty:
            return ticker_cepe_df
        try:
            ticker_cepe_df["oi_action_x"] = self.calculate_oi_actions_vectorized(ticker_cepe_df, "x")
            ticker_cepe_df["oi_action_y"] = self.calculate_oi_actions_vectorized(ticker_cepe_df, "y")

            ticker_cepe_df["trend_x"] = np.where(
                ticker_cepe_df["oi_action_x"].isin(market_actions.BULLISH), "Bullish",
                np.where(ticker_cepe_df["oi_action_x"].isin(market_actions.BEARISH), "Bearish", None),
            )
            ticker_cepe_df["trend_y"] = np.where(
                ticker_cepe_df["oi_action_y"].isin(market_actions.BULLISH), "Bullish",
                np.where(ticker_cepe_df["oi_action_y"].isin(market_actions.BEARISH), "Bearish", None),
            )
            return ticker_cepe_df
        except Exception as e:
            self.logger.error(f"Error in analyze_trend_vectorized: {e}")
            return ticker_cepe_df

    # ------------------------------------------------------------------
    # DataFrame normalization & interval conversion
    # ------------------------------------------------------------------

    def normalize_df(self, df: pd.DataFrame, trade_date: str,
                     trading_timestamps: pd.DatetimeIndex) -> pd.DataFrame:
        full_range_df = pd.DataFrame(trading_timestamps, columns=["time_stamp"])
        merged_df = pd.merge(full_range_df, df, on="time_stamp", how="left")

        numeric_cols = ["open_interest", "open_interest_change", "volume", "ltp", "ltp_change"]
        for col in numeric_cols:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].fillna(0).astype("float32")
        return merged_df

    def convert_candlestick_interval(self, df: pd.DataFrame,
                                     new_interval: str = "15min") -> pd.DataFrame:
        if df.empty:
            return df
        try:
            df["time_stamp"] = pd.to_datetime(df["time_stamp"])
            df.set_index("time_stamp", inplace=True)

            all_agg_rules = {
                "open_interest_x": "last",
                "open_interest_change_x": "sum",
                "volume_x": "sum",
                "ltp_x": "last",
                "ltp_change_x": "sum",
                "open_interest_y": "last",
                "open_interest_change_y": "sum",
                "volume_y": "sum",
                "ltp_y": "last",
                "ltp_change_y": "sum",
                "strike_price": "first",
            }
            agg_rules = {k: v for k, v in all_agg_rules.items() if k in df.columns}
            if not agg_rules:
                return pd.DataFrame()

            resampled = df.resample(new_interval).agg(agg_rules).fillna(0.0)
            resampled.reset_index(inplace=True)
            return resampled
        except Exception as e:
            self.logger.error(f"Error in convert_candlestick_interval: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Percentage & grading
    # ------------------------------------------------------------------

    def calculate_percentage(self, bullish: int, bearish: int) -> float:
        if bullish == 0:
            return 0
        return math.ceil(((bullish - bearish) / bullish) * 100)

    def get_grade(self, percentage: float) -> str:
        if percentage == 100:
            return Grade.A.value
        elif 85 < percentage < 100:
            return Grade.B.value
        elif 50 < percentage <= 85:
            return Grade.C.value
        return Grade.D.value

    # ------------------------------------------------------------------
    # Trend + grade analysis per time-slice
    # ------------------------------------------------------------------

    def trend_n_grade_analysis(self, df: pd.DataFrame) -> Dict:
        if df.empty:
            return {}
        try:
            n = len(df)

            calls_bullish = int((df["trend_x"] == "Bullish").sum())
            calls_bearish = int((df["trend_x"] == "Bearish").sum())
            puts_bullish = int((df["trend_y"] == "Bullish").sum())
            puts_bearish = int((df["trend_y"] == "Bearish").sum())

            calls_pct = self.calculate_percentage(calls_bullish, calls_bearish)
            puts_pct = self.calculate_percentage(puts_bullish, puts_bearish)

            calls_tn = math.ceil(((calls_bullish + calls_bearish) / n) * 100)
            puts_tn = math.ceil(((puts_bullish + puts_bearish) / n) * 100)

            result: Dict = {
                "time_stamp": df.iloc[0]["time_stamp"],
                "options": {
                    "calls": {
                        "bullish": calls_bullish,
                        "bearish": calls_bearish,
                        "percentage": calls_pct,
                        "grade": self.get_grade(calls_pct),
                        "tn_ratio": calls_tn,
                    },
                    "puts": {
                        "bullish": puts_bullish,
                        "bearish": puts_bearish,
                        "percentage": puts_pct,
                        "grade": self.get_grade(puts_pct),
                        "tn_ratio": puts_tn,
                    },
                },
                "callTrend": calls_bullish > calls_bearish,
                "putTrend": puts_bullish > puts_bearish,
            }

            if _USE_OI_WEIGHT:
                oi_data = self._compute_oi_magnitude_weights(df)
                result["options"]["calls"].update({
                    "bullish_oi_weight": oi_data["calls_bullish_weight"],
                    "bearish_oi_weight": oi_data["calls_bearish_weight"],
                })
                result["options"]["puts"].update({
                    "bullish_oi_weight": oi_data["puts_bullish_weight"],
                    "bearish_oi_weight": oi_data["puts_bearish_weight"],
                })
                if _OI_BLEND > 0:
                    for key, b_w, be_w in [
                        ("calls", oi_data["calls_bullish_weight"], oi_data["calls_bearish_weight"]),
                        ("puts", oi_data["puts_bullish_weight"], oi_data["puts_bearish_weight"]),
                    ]:
                        total_w = b_w + be_w
                        if total_w > 0:
                            oi_pct = math.ceil((b_w / total_w) * 100)
                        else:
                            oi_pct = 0
                        count_pct = result["options"][key]["percentage"]
                        blended = math.ceil(
                            count_pct * (1 - _OI_BLEND) + oi_pct * _OI_BLEND
                        )
                        result["options"][key]["percentage"] = blended
                        result["options"][key]["grade"] = self.get_grade(blended)

            return result
        except Exception as e:
            self.logger.error(f"Error in trend_n_grade_analysis: {e}")
            return {}

    @staticmethod
    def _compute_oi_magnitude_weights(df: pd.DataFrame) -> Dict[str, float]:
        """Sum absolute OI change for bullish/bearish rows on CE (x) and PE (y) legs."""
        oi_cx = df.get("open_interest_change_x", pd.Series(dtype=float)).abs()
        oi_cy = df.get("open_interest_change_y", pd.Series(dtype=float)).abs()

        return {
            "calls_bullish_weight": float(oi_cx[df["trend_x"] == "Bullish"].sum()),
            "calls_bearish_weight": float(oi_cx[df["trend_x"] == "Bearish"].sum()),
            "puts_bullish_weight": float(oi_cy[df["trend_y"] == "Bullish"].sum()),
            "puts_bearish_weight": float(oi_cy[df["trend_y"] == "Bearish"].sum()),
        }

    # ------------------------------------------------------------------
    # Time-slice simulation
    # ------------------------------------------------------------------

    def get_min_simulation(self, df: pd.DataFrame, interval: int = 15) -> List[pd.DataFrame]:
        if df.empty:
            return []
        try:
            records_per_day = trading_config.TRADING_MINUTES_PER_DAY // interval
            total = len(df)
            if total == 0:
                return []
            dfs = [
                df.iloc[i::records_per_day].copy()
                for i in range(min(records_per_day, total))
            ]
            return [d for d in dfs if not d.empty]
        except Exception as e:
            self.logger.error(f"Error in get_min_simulation: {e}")
            return []

    # ------------------------------------------------------------------
    # CE/PE pair processing  (uses instrument_seq integers)
    # ------------------------------------------------------------------

    def process_ce_pe_pair(
        self,
        ticker_df: pd.DataFrame,
        pair: CEPEPair,
        trade_date: str,
        trading_timestamps: pd.DatetimeIndex,
    ) -> pd.DataFrame:
        """Merge CE + PE tick streams, resample, and apply trend analysis."""
        try:
            columns = ["time_stamp", "open_interest", "open_interest_change",
                        "volume", "ltp", "ltp_change"]

            def _prepare_leg(seq_id: Optional[int]) -> pd.DataFrame:
                if seq_id is None or pd.isna(seq_id):
                    return pd.DataFrame(columns=columns)
                leg = ticker_df[ticker_df["instrument_id"] == seq_id].copy()
                if leg.empty:
                    return pd.DataFrame(columns=columns)
                leg["ltp"] = leg["close"]
                leg["ltp_change"] = leg["ltp"].diff()
                leg["open_interest_change"] = leg["open_interest"].diff()
                return leg[columns]

            ce_df = _prepare_leg(pair.ce_seq)
            pe_df = _prepare_leg(pair.pe_seq)

            ce_df = self.normalize_df(ce_df, trade_date, trading_timestamps)
            pe_df = self.normalize_df(pe_df, trade_date, trading_timestamps)

            if ce_df.empty and pe_df.empty:
                return pd.DataFrame()

            if ce_df.empty:
                ce_df = pd.DataFrame(index=pe_df.index, columns=columns)
                ce_df["time_stamp"] = pe_df["time_stamp"]
                ce_df = ce_df.fillna(0.0)
            if pe_df.empty:
                pe_df = pd.DataFrame(index=ce_df.index, columns=columns)
                pe_df["time_stamp"] = ce_df["time_stamp"]
                pe_df = pe_df.fillna(0.0)

            merged = pd.merge(ce_df, pe_df, on="time_stamp", how="outer",
                              suffixes=("_x", "_y")).fillna(0.0)

            for col in ["ltp_change_x", "ltp_change_y",
                        "open_interest_change_x", "open_interest_change_y"]:
                if col not in merged.columns:
                    merged[col] = 0.0

            merged = self.convert_candlestick_interval(
                merged, f"{trading_config.DEFAULT_INTERVAL}min"
            )
            if merged.empty:
                return pd.DataFrame()

            merged = self.analyze_trend_vectorized(merged)
            merged["strike_price"] = pair.strike_price
            return merged
        except Exception as e:
            self.logger.error(f"Error processing CE/PE pair: {e}")
            return pd.DataFrame()


# ======================================================================
# Option Ranking Service
# ======================================================================

class OptionRankingService:
    """Ranks and filters option predictions by consecutive-appearance logic."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def _validate_data(self, data: List[Dict]) -> bool:
        if not data or not isinstance(data, list):
            return False
        sample = data[0]
        return "name" in sample and "opt_data" in sample and isinstance(sample["opt_data"], list)

    def _within_trading_hours(self, ts: datetime) -> bool:
        try:
            t = ts.strftime("%H:%M:%S")
            return trading_config.TRADING_START_TIME <= t <= trading_config.TRADING_END_TIME
        except Exception:
            return False

    def _extract_qualified(self, data: List[Dict]) -> Tuple[List[StockPrediction], List[StockPrediction]]:
        calls, puts = [], []
        for stock_data in data:
            if not stock_data or "opt_data" not in stock_data or "name" not in stock_data:
                continue
            name = stock_data["name"]
            for t in range(min(len(stock_data["opt_data"]), analysis_config.MAX_TIME_INTERVALS)):
                td = stock_data["opt_data"][t]
                if "time_stamp" not in td or "options" not in td:
                    continue
                ts = td["time_stamp"]
                if not ts or not self._within_trading_hours(ts):
                    continue
                for option_key, option_type, bucket in [
                    ("calls", "call", calls), ("puts", "put", puts)
                ]:
                    opts = td["options"].get(option_key)
                    if not opts:
                        continue
                    if (opts["tn_ratio"] > analysis_config.TN_RATIO_THRESHOLD
                            and opts["bullish"] > opts["bearish"]
                            and opts["grade"] in analysis_config.ACCEPTED_GRADES):
                        bucket.append(StockPrediction(
                            stock=name, timestamp=ts, grade=opts["grade"],
                            option_type=option_type, tn_ratio=opts["tn_ratio"],
                            bullish_count=opts["bullish"], bearish_count=opts["bearish"],
                            bullish_oi_weight=opts.get("bullish_oi_weight", 0.0),
                            bearish_oi_weight=opts.get("bearish_oi_weight", 0.0),
                        ))
        return calls, puts

    def _group_by_timestamp(self, preds: List[StockPrediction]) -> Dict[datetime, List[StockPrediction]]:
        grouped: Dict[datetime, List[StockPrediction]] = defaultdict(list)
        for p in preds:
            grouped[p.timestamp].append(p)
        return dict(sorted(grouped.items()))

    def _find_consecutive(self, grouped: Dict[datetime, List[StockPrediction]]) -> Dict[str, List[StockPrediction]]:
        result: Dict[str, List[StockPrediction]] = defaultdict(list)
        timestamps = sorted(grouped.keys())
        prev_stocks: set = set()
        prev_ts = None
        cur_date = None

        for ts in timestamps:
            if cur_date != ts.date():
                cur_date = ts.date()
                prev_stocks = set()
                prev_ts = None

            cur_stocks = {p.stock for p in grouped[ts]}
            if prev_ts is not None:
                delta = ts - prev_ts
                if delta == timedelta(minutes=analysis_config.CONSECUTIVE_WINDOW_MINUTES):
                    for p in grouped[ts]:
                        if p.stock in prev_stocks:
                            result[ts.strftime("%Y-%m-%d")].append(p)

            prev_stocks = cur_stocks
            prev_ts = ts
        return dict(result)

    @staticmethod
    def _dedup(preds: List[StockPrediction]) -> List[StockPrediction]:
        seen: set = set()
        out: List[StockPrediction] = []
        for p in preds:
            if p.stock not in seen:
                seen.add(p.stock)
                out.append(p)
        return out

    def _format(self, cons_calls: Dict, cons_puts: Dict) -> PredictionResult:
        result = PredictionResult()
        for date, preds in cons_calls.items():
            unique = self._dedup(preds)
            result.call.append({
                "date": date,
                "stock_data": [
                    {"time_stamp": p.timestamp, "stock": p.stock, "grade": p.grade,
                     "tn_ratio": p.tn_ratio, "bullish_count": p.bullish_count,
                     "bearish_count": p.bearish_count}
                    for p in unique
                ],
            })
        for date, preds in cons_puts.items():
            unique = self._dedup(preds)
            result.put.append({
                "date": date,
                "stock_data": [
                    {"time_stamp": p.timestamp, "stock": p.stock, "grade": p.grade,
                     "tn_ratio": p.tn_ratio, "bullish_count": p.bullish_count,
                     "bearish_count": p.bearish_count}
                    for p in unique
                ],
            })
        return result

    def rank_options(self, data: List[Dict]) -> PredictionResult:
        if not self._validate_data(data):
            return PredictionResult()
        self.logger.info(f"Ranking options for {len(data)} stocks")

        calls, puts = self._extract_qualified(data)
        self.logger.info(f"Qualified: {len(calls)} call candidates, {len(puts)} put candidates")

        cons_calls = self._find_consecutive(self._group_by_timestamp(calls))
        cons_puts = self._find_consecutive(self._group_by_timestamp(puts))

        result = self._format(cons_calls, cons_puts)
        self.logger.info(f"Predictions: {len(result.call)} call dates, {len(result.put)} put dates")
        return result
