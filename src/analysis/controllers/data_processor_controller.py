"""
Data Processor Controller
========================
Orchestrates post-analysis processing:
  Read predictions Excel → add instrument keys from DB → fetch OHLC via Upstox → export CSVs

instrument_key is now read from options.stock (populated by the data
collection pipeline), so there is no longer a dependency on NSE.json.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import pandas as pd
import logging
from typing import Optional, Tuple

import db_config
from ..services.external_api_service import ExternalAPIService
from ..services.trade_service import TradeExecutionService
from ..views.export_view import ExportView, FileManager
from ..models.entities import AnalysisResultDTO, ExportResultDTO, StockPrediction


class DataProcessorController:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_service = ExternalAPIService()
        self.trade_service = TradeExecutionService()
        self.export_view = ExportView()
        self.file_manager = FileManager()

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    async def process_complete_pipeline(
        self, excel_file: Optional[str] = None
    ) -> AnalysisResultDTO:
        try:
            self.logger.info("Starting data processing pipeline...")

            if not excel_file:
                excel_file = self.file_manager.find_latest_predictions_file()
                if not excel_file:
                    return AnalysisResultDTO(success=False,
                                            error_message="No predictions Excel file found")

            calls_df, puts_df = self._process_excel(excel_file)
            if calls_df.empty and puts_df.empty:
                return AnalysisResultDTO(success=False,
                                        error_message="No data after processing")

            enhanced_exp = self.export_view.export_enhanced_excel_with_calls_puts(calls_df, puts_df)
            files: list = []
            if enhanced_exp.success:
                files.extend(enhanced_exp.files_created)

            calls_csv = puts_csv = None
            all_ohlc_dfs = []

            if not calls_df.empty:
                self.logger.info(f"Calls: {len(calls_df)} rows, "
                                 f"{calls_df['Stock'].nunique()} stocks, "
                                 f"{calls_df['instrument_key'].notna().sum()} with keys")
                res, ohlc_dfs = await self._fetch_ohlc(calls_df, "Calls")
                if res.success:
                    files.extend(res.files_created)
                    calls_csv = res.files_created[0] if res.files_created else None
                all_ohlc_dfs.extend(ohlc_dfs)

            if not puts_df.empty:
                self.logger.info(f"Puts: {len(puts_df)} rows, "
                                 f"{puts_df['Stock'].nunique()} stocks, "
                                 f"{puts_df['instrument_key'].notna().sum()} with keys")
                res, ohlc_dfs = await self._fetch_ohlc(puts_df, "Puts")
                if res.success:
                    files.extend(res.files_created)
                    puts_csv = res.files_created[0] if res.files_created else None
                all_ohlc_dfs.extend(ohlc_dfs)

            trade_signals_file = None
            if all_ohlc_dfs:
                ohlc_by_stock = self.api_service.group_ohlc_by_stock(all_ohlc_dfs)
                preds = self._build_predictions_from_dfs(calls_df, puts_df)
                if preds and ohlc_by_stock:
                    signals = self.trade_service.evaluate_trades(preds, ohlc_by_stock)
                    if signals:
                        sig_exp = self.export_view.export_trade_signals_to_excel(signals)
                        if sig_exp.success:
                            files.extend(sig_exp.files_created)
                            trade_signals_file = sig_exp.files_created[0]

            self.logger.info(f"Pipeline complete. Files created: {files}")

            return AnalysisResultDTO(
                success=True,
                data={"enhanced_excel": enhanced_exp.files_created[0] if enhanced_exp.success else None,
                      "calls_csv": calls_csv, "puts_csv": puts_csv,
                      "trade_signals": trade_signals_file},
                metadata={"input_file": excel_file,
                          "calls_processed": len(calls_df),
                          "puts_processed": len(puts_df),
                          "files_created": files},
            )
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}", exc_info=True)
            return AnalysisResultDTO(success=False, error_message=str(e))

    # ------------------------------------------------------------------
    # Excel processing
    # ------------------------------------------------------------------

    def _process_excel(self, excel_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        try:
            if not os.path.exists(excel_file):
                self.logger.error(f"File not found: {excel_file}")
                return pd.DataFrame(), pd.DataFrame()

            xl = pd.ExcelFile(excel_file)
            sheets = xl.sheet_names
            self.logger.info(f"Excel sheets: {sheets}")

            if len(sheets) > 1:
                calls_df, puts_df = self._read_multi_sheet(excel_file, sheets)
            else:
                df = pd.read_excel(excel_file)
                calls_df, puts_df = self.export_view.separate_calls_puts_data(df)

            # Load instrument keys from DB (not NSE.json)
            self.api_service.load_instrument_key_mapping(db_config.DATABASE_URL)

            if self.api_service.stock_to_instrument_key:
                calls_df = self.api_service.add_instrument_keys_to_dataframe(calls_df)
                puts_df = self.api_service.add_instrument_keys_to_dataframe(puts_df)
            else:
                calls_df["instrument_key"] = None
                puts_df["instrument_key"] = None

            return calls_df, puts_df
        except Exception as e:
            self.logger.error(f"Excel processing error: {e}", exc_info=True)
            return pd.DataFrame(), pd.DataFrame()

    @staticmethod
    def _read_multi_sheet(
        path: str, sheets: list
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        calls_sheet = puts_sheet = None
        for s in sheets:
            sl = s.lower()
            if "call" in sl or "ce" in sl:
                calls_sheet = s
            elif "put" in sl or "pe" in sl:
                puts_sheet = s

        if calls_sheet and puts_sheet:
            return pd.read_excel(path, sheet_name=calls_sheet), pd.read_excel(path, sheet_name=puts_sheet)

        df = pd.read_excel(path, sheet_name=sheets[0])
        from ..views.export_view import ExportView
        return ExportView().separate_calls_puts_data(df)

    # ------------------------------------------------------------------
    # OHLC fetching
    # ------------------------------------------------------------------

    async def _fetch_ohlc(
        self, df: pd.DataFrame, option_type: str
    ) -> tuple:
        """Returns (ExportResultDTO, list_of_ohlc_dataframes)."""
        try:
            requests = self.api_service.prepare_api_requests(df, option_type)
            if not requests:
                return (
                    ExportResultDTO(success=False,
                                    error_message=f"No API requests for {option_type}"),
                    [],
                )

            dfs = await self.api_service.fetch_all_ohlc(requests, option_type)
            if not dfs:
                return (
                    ExportResultDTO(success=False,
                                    error_message=f"No OHLC data for {option_type}"),
                    [],
                )

            export_result = self.export_view.export_ohlc_data_to_csv(dfs, option_type)
            return export_result, dfs
        except Exception as e:
            return ExportResultDTO(success=False, error_message=str(e)), []

    @staticmethod
    def _build_predictions_from_dfs(
        calls_df: pd.DataFrame, puts_df: pd.DataFrame
    ) -> list:
        """Convert Excel-sourced DataFrames back into StockPrediction objects for trade evaluation."""
        preds = []
        for df, opt_type in [(calls_df, "call"), (puts_df, "put")]:
            if df.empty:
                continue
            for _, row in df.iterrows():
                try:
                    ts = row.get("Time")
                    date = row.get("Date")
                    if ts and date:
                        from datetime import datetime as _dt
                        if isinstance(date, str):
                            date = pd.to_datetime(date)
                        if isinstance(ts, str):
                            ts = _dt.strptime(ts, "%H:%M:%S").time()
                            ts = _dt.combine(date.date(), ts)
                        elif hasattr(ts, "time"):
                            ts = _dt.combine(date.date(), ts.time())
                    elif date:
                        ts = pd.to_datetime(date)
                    else:
                        continue

                    preds.append(StockPrediction(
                        stock=row.get("Stock", ""),
                        timestamp=ts,
                        grade=str(row.get("Grade", "D")),
                        option_type=opt_type,
                        tn_ratio=int(row.get("TN_Ratio", 0)),
                        bullish_count=int(row.get("Bullish_Count", 0)),
                        bearish_count=int(row.get("Bearish_Count", 0)),
                    ))
                except Exception:
                    continue
        return preds

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def get_instrument_key_for_stock(self, stock_name: str) -> Optional[str]:
        if not self.api_service.stock_to_instrument_key:
            self.api_service.load_instrument_key_mapping(db_config.DATABASE_URL)
        return self.api_service.get_instrument_key_for_stock(stock_name)

    def get_available_instrument_keys(self) -> dict:
        if not self.api_service.stock_to_instrument_key:
            self.api_service.load_instrument_key_mapping(db_config.DATABASE_URL)
        return self.api_service.stock_to_instrument_key.copy()
