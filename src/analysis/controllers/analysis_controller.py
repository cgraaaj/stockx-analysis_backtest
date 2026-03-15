"""
Analysis Controller
==================
Orchestrates the full option analysis workflow:
  DB connect → fetch stocks → batch instruments/tickers → analyse → rank → export

Aligned with the ticker_ts hypertable (instrument_seq integer FK)
and parameterized queries from DatabaseService.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import asyncio
import logging
import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

from ..services.database_service import DatabaseService
from ..services.analysis_service import AnalysisService, OptionRankingService
from ..services.market_context_service import MarketContextService
from ..views.export_view import ExportView, FileManager
from ..models.entities import (
    Stock, CEPEPair, AnalysisResultDTO, PredictionResult, PerformanceMetrics,
)
from ..config.settings import processing_config, trading_config, market_context_config


class AnalysisController:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_service = DatabaseService()
        self.analysis_service = AnalysisService()
        self.ranking_service = OptionRankingService()
        self.market_ctx_service = MarketContextService()
        self.export_view = ExportView()
        self.file_manager = FileManager()
        self.performance_metrics: Optional[PerformanceMetrics] = None

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run_complete_analysis(
        self, trading_dates: Optional[List[str]] = None
    ) -> AnalysisResultDTO:
        start_time = time.time()
        try:
            self.logger.info("Starting complete option analysis workflow...")
            await self.db_service.connect()

            if not trading_dates:
                available = await self.db_service.get_available_trading_dates()
                if not available:
                    return AnalysisResultDTO(success=False,
                                            error_message="No trading dates in DB")
                trading_dates = [available[-1]]

            self.logger.info(f"Processing {len(trading_dates)} date(s): {trading_dates}")

            stocks = await self.db_service.get_active_stocks()
            if not stocks:
                return AnalysisResultDTO(success=False,
                                        error_message="No active stocks in DB")
            self.logger.info(f"Found {len(stocks)} active stocks")

            expiry_dates = await self.db_service.get_available_expiry_dates()
            if not expiry_dates:
                return AnalysisResultDTO(success=False,
                                        error_message="No expiry dates in DB")

            all_results: List[Dict] = []
            total_batches = (len(stocks) - 1) // processing_config.BATCH_SIZE + 1

            for trade_date in trading_dates:
                expiry = self._pick_expiry(trade_date, expiry_dates)
                self.logger.info(f"Date: {trade_date}  Expiry: {expiry}")

                if self.market_ctx_service.is_enabled:
                    index_ctx = await self.market_ctx_service.evaluate_market_context(trade_date)
                    self.logger.info(f"Market context: {len(index_ctx)} indices evaluated")

                date_results: List[Dict] = []
                for i in range(0, len(stocks), processing_config.BATCH_SIZE):
                    batch = stocks[i:i + processing_config.BATCH_SIZE]
                    batch_num = i // processing_config.BATCH_SIZE + 1
                    self.logger.info(f"Batch {batch_num}/{total_batches} ({len(batch)} stocks)")

                    results = await self._process_batch(batch, trade_date, expiry)
                    valid = [r for r in results if r is not None and not isinstance(r, Exception)]
                    date_results.extend(valid)
                    self.logger.info(f"Batch {batch_num} done: {len(valid)} valid results")

                all_results.extend(date_results)

            self.logger.info(f"Analysis done: {len(all_results)} stock results")

            predictions = self.ranking_service.rank_options(all_results)

            predictions = self._apply_market_context_filter(predictions)

            date_range = self.file_manager.create_date_range_string(trading_dates)
            analysis_exp = self.export_view.export_analysis_results_to_pickle(all_results, date_range)
            excel_fn = f"option_predictions_optimized_{date_range}.xlsx"
            excel_exp = self.export_view.export_predictions_to_excel(predictions, excel_fn)
            pickle_exp = self.export_view.export_predictions_to_pickle(predictions, date_range)

            elapsed = time.time() - start_time
            self.performance_metrics = PerformanceMetrics(
                total_stocks_processed=len(all_results),
                successful_predictions=len(predictions.call) + len(predictions.put),
                failed_predictions=len(stocks) * len(trading_dates) - len(all_results),
                processing_time_seconds=elapsed,
            )
            self.logger.info(f"Total time: {elapsed/60:.2f} min")

            files: List[str] = []
            for exp in (analysis_exp, excel_exp, pickle_exp):
                if exp.success:
                    files.extend(exp.files_created)

            market_ctx_info = {}
            if self.market_ctx_service.is_enabled:
                market_ctx_info = {
                    name: {"trend": ctx.trend, "confidence": ctx.confidence}
                    for name, ctx in self.market_ctx_service.contexts.items()
                }

            return AnalysisResultDTO(
                success=True, data=predictions,
                metadata={"trading_dates": trading_dates,
                          "total_stocks": len(stocks),
                          "execution_time_minutes": elapsed / 60,
                          "files_created": files,
                          "market_context": market_ctx_info},
            )
        except Exception as e:
            self.logger.error(f"Workflow error: {e}", exc_info=True)
            return AnalysisResultDTO(success=False, error_message=str(e))
        finally:
            try:
                await self.db_service.disconnect()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Batch processing
    # ------------------------------------------------------------------

    async def _process_batch(
        self, stocks: List[Stock], trade_date: str, expiry: str
    ) -> List[Optional[Dict]]:
        stock_ids = [s.id for s in stocks]

        instrument_df = await self.db_service.get_instruments_for_stocks(stock_ids, expiry)
        if instrument_df.empty:
            return []

        seqs = instrument_df["instrument_seq"].dropna().astype(int).tolist()
        ticker_df = await self.db_service.get_ticker_data_for_instruments(seqs, trade_date)
        if ticker_df.empty:
            return []

        results = []
        for stock in stocks:
            try:
                r = self._analyze_stock(stock, instrument_df, ticker_df, trade_date)
                results.append(r)
            except Exception as e:
                self.logger.error(f"Error processing {stock.name}: {e}")
                results.append(None)
        return results

    def _analyze_stock(
        self, stock: Stock, instrument_df: pd.DataFrame,
        ticker_df: pd.DataFrame, trade_date: str
    ) -> Optional[Dict]:
        try:
            # Filter instruments for this stock
            stock_insts = instrument_df[instrument_df["stock_id"].astype(str) == stock.id]
            if stock_insts.empty:
                return None

            # Filter ticker data for this stock's instrument_seqs
            stock_seqs = stock_insts["instrument_seq"].dropna().astype(int).tolist()
            stock_ticks = ticker_df[ticker_df["instrument_id"].isin(stock_seqs)]
            if stock_ticks.empty:
                return None

            pairs_df = self._make_ce_pe_pairs(stock_insts)
            if pairs_df.empty:
                return None

            timestamps = self.db_service.get_trading_timestamps(trade_date)

            processed: List[pd.DataFrame] = []
            for _, row in pairs_df.iterrows():
                pair = CEPEPair(
                    ce_seq=int(row["ce_seq"]) if pd.notna(row.get("ce_seq")) else None,
                    pe_seq=int(row["pe_seq"]) if pd.notna(row.get("pe_seq")) else None,
                    strike_price=row["strike_price"],
                )
                df = self.analysis_service.process_ce_pe_pair(
                    stock_ticks, pair, trade_date, timestamps,
                )
                if not df.empty:
                    processed.append(df)

            if not processed:
                return None

            combined = pd.concat(processed, ignore_index=True)
            sim_dfs = self.analysis_service.get_min_simulation(
                combined, trading_config.DEFAULT_INTERVAL,
            )
            sim_results = []
            for sdf in sim_dfs:
                if not sdf.empty:
                    analysis = self.analysis_service.trend_n_grade_analysis(sdf)
                    if analysis:
                        sim_results.append(analysis)

            return {"name": stock.name, "opt_data": sim_results}
        except Exception as e:
            self.logger.error(f"Error processing {stock.name}: {e}")
            return None

    # ------------------------------------------------------------------
    # Market context filter
    # ------------------------------------------------------------------

    def _apply_market_context_filter(
        self, predictions: PredictionResult
    ) -> PredictionResult:
        """Filter ranked predictions through the market context service."""
        if not self.market_ctx_service.is_enabled:
            return predictions

        from ..models.entities import StockPrediction

        def _reconstruct_preds(date_entries, option_type):
            preds = []
            for entry in date_entries:
                for sd in entry.get("stock_data", []):
                    preds.append(StockPrediction(
                        stock=sd["stock"], timestamp=sd["time_stamp"],
                        grade=sd["grade"], option_type=option_type,
                        tn_ratio=sd["tn_ratio"],
                        bullish_count=sd["bullish_count"],
                        bearish_count=sd["bearish_count"],
                    ))
            return preds

        def _rebuild_entries(filtered, original_entries):
            kept_stocks = {(p.stock, p.timestamp) for p in filtered}
            rebuilt = []
            for entry in original_entries:
                kept_data = [
                    sd for sd in entry.get("stock_data", [])
                    if (sd["stock"], sd["time_stamp"]) in kept_stocks
                ]
                if kept_data:
                    for sd in kept_data:
                        match = next(
                            (p for p in filtered
                             if p.stock == sd["stock"] and p.timestamp == sd["time_stamp"]),
                            None,
                        )
                        if match:
                            sd["grade"] = match.grade
                    rebuilt.append({**entry, "stock_data": kept_data})
            return rebuilt

        call_preds = _reconstruct_preds(predictions.call, "call")
        put_preds = _reconstruct_preds(predictions.put, "put")

        filtered_calls = self.market_ctx_service.filter_predictions(call_preds)
        filtered_puts = self.market_ctx_service.filter_predictions(put_preds)

        before = len(call_preds) + len(put_preds)
        after = len(filtered_calls) + len(filtered_puts)
        if before != after:
            self.logger.info(
                f"Market context filter: {before} → {after} predictions "
                f"(mode={market_context_config.FILTER_MODE})"
            )

        predictions.call = _rebuild_entries(filtered_calls, predictions.call)
        predictions.put = _rebuild_entries(filtered_puts, predictions.put)
        return predictions

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_ce_pe_pairs(instruments_df: pd.DataFrame) -> pd.DataFrame:
        """Create CE/PE pairs using instrument_seq as the join key for ticker_ts."""
        ce = instruments_df[instruments_df["instrument_type"] == "CE"][
            ["instrument_seq", "strike_price"]
        ].rename(columns={"instrument_seq": "ce_seq"})

        pe = instruments_df[instruments_df["instrument_type"] == "PE"][
            ["instrument_seq", "strike_price"]
        ].rename(columns={"instrument_seq": "pe_seq"})

        pairs = pd.merge(ce, pe, on="strike_price", how="outer").sort_values("strike_price")
        return pairs

    def _pick_expiry(self, trade_date: str, expiry_dates: List[str]) -> str:
        try:
            trade_dt = datetime.strptime(trade_date, "%Y-%m-%d")
            ym = trade_dt.strftime("%Y-%m")

            # Same-month expiry that hasn't passed yet
            for exp in expiry_dates:
                exp_dt = datetime.strptime(exp, "%Y-%m-%d")
                if exp_dt.strftime("%Y-%m") == ym and exp_dt >= trade_dt:
                    return exp

            # Next available future expiry
            for exp in expiry_dates:
                if datetime.strptime(exp, "%Y-%m-%d") >= trade_dt:
                    return exp

            if expiry_dates:
                return expiry_dates[-1]
        except Exception as e:
            self.logger.error(f"Expiry selection error for {trade_date}: {e}")

        return expiry_dates[-1] if expiry_dates else trade_date

    def get_performance_metrics(self) -> Optional[PerformanceMetrics]:
        return self.performance_metrics
