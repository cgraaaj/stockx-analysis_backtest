"""
Export View
==========
Handles all file export operations: Excel, CSV, and Pickle formats.
"""

import os
import glob
import pickle
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

from ..models.entities import ExportResultDTO, PredictionResult, TradeSignal
from ..config.settings import get_analysis_dir, strategy_config


class ExportView:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Predictions → Excel
    # ------------------------------------------------------------------

    def export_predictions_to_excel(
        self, predictions: PredictionResult, filename: str
    ) -> ExportResultDTO:
        try:
            calls_data = self._flatten_predictions(predictions.call)
            puts_data = self._flatten_predictions(predictions.put)

            calls_df = pd.DataFrame(calls_data)
            puts_df = pd.DataFrame(puts_data)

            with pd.ExcelWriter(filename, engine="openpyxl") as writer:
                calls_df.to_excel(writer, sheet_name="Calls", index=False)
                puts_df.to_excel(writer, sheet_name="Puts", index=False)

                summary = pd.DataFrame({
                    "Option_Type": ["Calls", "Puts"],
                    "Count": [len(calls_data), len(puts_data)],
                    "Unique_Stocks": [
                        calls_df["Stock"].nunique() if not calls_df.empty else 0,
                        puts_df["Stock"].nunique() if not puts_df.empty else 0,
                    ],
                    "Dates_Covered": [
                        calls_df["Date"].nunique() if not calls_df.empty else 0,
                        puts_df["Date"].nunique() if not puts_df.empty else 0,
                    ],
                })
                summary.to_excel(writer, sheet_name="Summary", index=False)

            self.logger.info(f"Predictions exported to {filename}")
            return ExportResultDTO(success=True, files_created=[filename])
        except Exception as e:
            return ExportResultDTO(success=False, error_message=str(e))

    def _flatten_predictions(self, date_entries: List[Dict]) -> List[Dict]:
        rows: List[Dict] = []
        for entry in date_entries:
            date = entry.get("date", "")
            for sd in entry.get("stock_data", []):
                rows.append({
                    "Date": date,
                    "Time": self._fmt_ts(sd.get("time_stamp", "")),
                    "Stock": sd.get("stock", ""),
                    "Grade": sd.get("grade", ""),
                    "TN_Ratio": sd.get("tn_ratio", 0),
                    "Bullish_Count": sd.get("bullish_count", 0),
                    "Bearish_Count": sd.get("bearish_count", 0),
                    "Market_Trend": sd.get("market_trend", ""),
                    "Trend_Alignment": sd.get("trend_alignment", ""),
                })
        return rows

    # ------------------------------------------------------------------
    # Enhanced Excel (with instrument keys, calls/puts sheets)
    # ------------------------------------------------------------------

    def export_enhanced_excel_with_calls_puts(
        self, calls_df: pd.DataFrame, puts_df: pd.DataFrame
    ) -> ExportResultDTO:
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"option_predictions_calls_puts_{ts}.xlsx"

            with pd.ExcelWriter(filename, engine="openpyxl") as writer:
                calls_df.to_excel(writer, sheet_name="Calls", index=False)
                puts_df.to_excel(writer, sheet_name="Puts", index=False)

                summary = pd.DataFrame({
                    "Option_Type": ["Calls", "Puts"],
                    "Total_Rows": [len(calls_df), len(puts_df)],
                    "Unique_Stocks": [
                        calls_df["Stock"].nunique() if not calls_df.empty else 0,
                        puts_df["Stock"].nunique() if not puts_df.empty else 0,
                    ],
                    "Rows_With_Keys": [
                        calls_df["instrument_key"].notna().sum() if not calls_df.empty else 0,
                        puts_df["instrument_key"].notna().sum() if not puts_df.empty else 0,
                    ],
                })
                summary.to_excel(writer, sheet_name="Summary", index=False)

            self.logger.info(f"Enhanced Excel saved: {filename}")
            return ExportResultDTO(success=True, files_created=[filename])
        except Exception as e:
            return ExportResultDTO(success=False, error_message=str(e))

    # ------------------------------------------------------------------
    # Trade signals → Excel
    # ------------------------------------------------------------------

    def export_trade_signals_to_excel(
        self,
        signals: List[TradeSignal],
        filename: str = None,
    ) -> ExportResultDTO:
        try:
            if not signals:
                return ExportResultDTO(success=False, error_message="No trade signals to export")

            if filename is None:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"trade_signals_{ts}.xlsx"

            rows = []
            for s in signals:
                rows.append({
                    "Stock": s.stock,
                    "Signal_Time": s.signal_time,
                    "Option_Type": s.option_type.upper(),
                    "Grade": s.grade,
                    "TN_Ratio": s.tn_ratio,
                    "Entry_Price": s.entry_price,
                    "Stop_Loss": s.stop_loss,
                    "Target": s.target,
                    "Risk": s.risk,
                    "Reward": s.reward,
                    "R:R": f"1:{strategy_config.REWARD_RATIO}",
                    "Outcome": s.outcome or "",
                    "Exit_Price": s.exit_price or "",
                    "Exit_Time": s.exit_time or "",
                    "PnL_Points": s.pnl_points,
                    "Holding_Candles": s.holding_candles,
                    "Market_Trend": s.market_trend or "",
                    "Trend_Alignment": s.trend_alignment or "",
                })

            df = pd.DataFrame(rows)

            with pd.ExcelWriter(filename, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="Trade_Signals", index=False)

                wins = sum(1 for s in signals if s.outcome == "target_hit")
                losses = sum(1 for s in signals if s.outcome == "sl_hit")
                time_exits = sum(1 for s in signals if s.outcome == "time_exit")
                total = len(signals)
                total_pnl = sum(s.pnl_points for s in signals)

                summary = pd.DataFrame({
                    "Metric": [
                        "Total Signals", "Target Hit", "SL Hit", "Time Exit",
                        "Win Rate %", "Total PnL (pts)",
                        "Avg PnL (pts)", "R:R Ratio",
                        "SL Method", "Max Holding Candles",
                    ],
                    "Value": [
                        total, wins, losses, time_exits,
                        round(wins / total * 100, 1) if total else 0,
                        round(total_pnl, 2),
                        round(total_pnl / total, 2) if total else 0,
                        f"1:{strategy_config.REWARD_RATIO}",
                        strategy_config.SL_METHOD,
                        strategy_config.MAX_HOLDING_CANDLES,
                    ],
                })
                summary.to_excel(writer, sheet_name="Summary", index=False)

            self.logger.info(
                f"Trade signals exported to {filename} "
                f"({len(signals)} signals, {wins}W/{losses}L/{time_exits}T)"
            )
            return ExportResultDTO(success=True, files_created=[filename])
        except Exception as e:
            return ExportResultDTO(success=False, error_message=str(e))

    # ------------------------------------------------------------------
    # OHLC → CSV
    # ------------------------------------------------------------------

    def export_ohlc_data_to_csv(
        self, dataframes: List[pd.DataFrame], option_type: str
    ) -> ExportResultDTO:
        try:
            if not dataframes:
                return ExportResultDTO(success=False,
                                      error_message=f"No data for {option_type}")

            combined = pd.concat(dataframes, ignore_index=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{option_type.lower()}_ohlc_data_{ts}.csv"
            combined.to_csv(filename, index=False)

            self.logger.info(f"Saved {len(combined)} {option_type} candle records to {filename}")
            return ExportResultDTO(success=True, files_created=[filename])
        except Exception as e:
            return ExportResultDTO(success=False, error_message=str(e))

    # ------------------------------------------------------------------
    # Pickle exports
    # ------------------------------------------------------------------

    def export_analysis_results_to_pickle(
        self, results: List[Dict], date_range: str
    ) -> ExportResultDTO:
        try:
            filename = f"analyzed_stocks_data_optimized_{date_range}.pickle"
            with open(filename, "wb") as f:
                pickle.dump(results, f, protocol=pickle.HIGHEST_PROTOCOL)
            self.logger.info(f"Analysis results saved to {filename}")
            return ExportResultDTO(success=True, files_created=[filename])
        except Exception as e:
            return ExportResultDTO(success=False, error_message=str(e))

    def export_predictions_to_pickle(
        self, predictions: PredictionResult, date_range: str
    ) -> ExportResultDTO:
        try:
            filename = f"prediction_optimized_{date_range}.pickle"
            data = {"call": predictions.call, "put": predictions.put}
            with open(filename, "wb") as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

            call_n = sum(len(p.get("stock_data", [])) for p in predictions.call)
            put_n = sum(len(p.get("stock_data", [])) for p in predictions.put)
            self.logger.info(f"Predictions saved to {filename} ({call_n} calls, {put_n} puts)")
            return ExportResultDTO(success=True, files_created=[filename])
        except Exception as e:
            return ExportResultDTO(success=False, error_message=str(e))

    # ------------------------------------------------------------------
    # Data separation helpers
    # ------------------------------------------------------------------

    def separate_calls_puts_data(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if "option_type" in df.columns:
            return (
                df[df["option_type"].str.upper() == "CE"].copy(),
                df[df["option_type"].str.upper() == "PE"].copy(),
            )

        for col in ["Option_Type", "Type", "CE_PE", "Call_Put"]:
            if col in df.columns:
                return (
                    df[df[col].str.contains("C|Call", case=False, na=False)].copy(),
                    df[df[col].str.contains("P|Put", case=False, na=False)].copy(),
                )

        if "Bullish_Count" in df.columns and "Bearish_Count" in df.columns:
            mask = df["Bullish_Count"] > df["Bearish_Count"]
            return df[mask].copy(), df[~mask].copy()

        self.logger.warning("No clear call/put separator found; duplicating data for both sheets")
        return df.copy(), df.copy()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fmt_ts(ts) -> str:
        if not ts:
            return ""
        try:
            if hasattr(ts, "strftime"):
                return ts.strftime("%H:%M:%S")
            return str(ts)
        except Exception:
            return str(ts)


class FileManager:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def find_latest_predictions_file(
        self, pattern: str = "option_predictions_optimized_*.xlsx"
    ) -> Optional[str]:
        try:
            patterns = [pattern, f"../{pattern}"]
            files: List[str] = []
            for p in patterns:
                files.extend(glob.glob(p))
            if not files:
                self.logger.error("No predictions Excel file found")
                return None
            latest = max(files, key=os.path.getctime)
            self.logger.info(f"Latest predictions file: {latest}")
            return latest
        except Exception as e:
            self.logger.error(f"Error finding predictions file: {e}")
            return None

    def create_date_range_string(self, dates: List[str]) -> str:
        if not dates:
            return datetime.now().strftime("%Y%m%d")
        if len(dates) == 1:
            return dates[0].replace("-", "")
        return f"{dates[0].replace('-', '')}_to_{dates[-1].replace('-', '')}"

    def ensure_output_directory(self, directory: str = None) -> str:
        if directory is None:
            directory = get_analysis_dir()
        os.makedirs(directory, exist_ok=True)
        return directory
