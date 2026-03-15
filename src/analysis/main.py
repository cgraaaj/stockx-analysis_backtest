"""
Main Application Entry Point
===========================
CLI and programmatic interface for the options analysis pipeline.
"""

import asyncio
import logging
import sys
from typing import Optional, List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

from .controllers.analysis_controller import AnalysisController
from .controllers.data_processor_controller import DataProcessorController


class OptionsAnalysisApp:
    """Facade over analysis + data-processing controllers."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.analysis_controller = AnalysisController()
        self.data_processor_controller = DataProcessorController()

    async def run_full_analysis_workflow(self, trading_dates: Optional[List[str]] = None):
        """Analysis → Processing → Export (complete pipeline)."""
        try:
            self.logger.info("Starting full analysis workflow")

            analysis_result = await self.analysis_controller.run_complete_analysis(trading_dates)
            if not analysis_result.success:
                self.logger.error(f"Analysis failed: {analysis_result.error_message}")
                return analysis_result

            self.logger.info(f"Analysis done. Files: {analysis_result.metadata.get('files_created', [])}")

            processing_result = await self.data_processor_controller.process_complete_pipeline()
            if not processing_result.success:
                self.logger.error(f"Processing failed: {processing_result.error_message}")
                return processing_result

            self.logger.info(f"Processing done. Files: {processing_result.metadata.get('files_created', [])}")

            metrics = self.analysis_controller.get_performance_metrics()
            if metrics:
                self.logger.info(
                    f"Performance: {metrics.total_stocks_processed} stocks, "
                    f"{metrics.successful_predictions} predictions, "
                    f"{metrics.processing_time_seconds/60:.2f} min"
                )

            all_files = analysis_result.metadata.get("files_created", []) + \
                        processing_result.metadata.get("files_created", [])

            return {
                "success": True,
                "analysis_result": analysis_result,
                "processing_result": processing_result,
                "all_files_created": all_files,
                "performance_metrics": metrics,
            }
        except Exception as e:
            self.logger.error(f"Workflow error: {e}")
            return {"success": False, "error": str(e)}

    async def run_analysis_only(self, trading_dates: Optional[List[str]] = None):
        return await self.analysis_controller.run_complete_analysis(trading_dates)

    async def run_data_processing_only(self, excel_file: Optional[str] = None):
        return await self.data_processor_controller.process_complete_pipeline(excel_file)

    def get_instrument_key_for_stock(self, stock_name: str) -> Optional[str]:
        return self.data_processor_controller.get_instrument_key_for_stock(stock_name)

    def get_available_instrument_keys(self) -> dict:
        return self.data_processor_controller.get_available_instrument_keys()


# ======================================================================
# CLI
# ======================================================================

async def _run_full():
    app = OptionsAnalysisApp()
    result = await app.run_full_analysis_workflow()
    if result.get("success"):
        print(f"Success. Files: {result.get('all_files_created', [])}")
    else:
        print(f"Failed: {result.get('error', 'unknown')}")

async def _run_analysis():
    app = OptionsAnalysisApp()
    result = await app.run_analysis_only()
    if result.success:
        print(f"Analysis done. Files: {result.metadata.get('files_created', [])}")
    else:
        print(f"Analysis failed: {result.error_message}")

async def _run_processing(excel_file=None):
    app = OptionsAnalysisApp()
    result = await app.run_data_processing_only(excel_file)
    if result.success:
        print(f"Processing done. Files: {result.metadata.get('files_created', [])}")
    else:
        print(f"Processing failed: {result.error_message}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Options Analysis Pipeline")
    parser.add_argument(
        "--mode",
        choices=["full", "analysis", "processing", "get-key", "list-keys"],
        default="full",
    )
    parser.add_argument("--excel-file", help="Excel file for processing mode")
    parser.add_argument("--stock-name", help="Stock name for get-key mode")
    parser.add_argument("--dates", nargs="+", help="Trading dates (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.mode == "full":
        asyncio.run(_run_full())
    elif args.mode == "analysis":
        asyncio.run(_run_analysis())
    elif args.mode == "processing":
        asyncio.run(_run_processing(args.excel_file))
    elif args.mode == "get-key":
        if not args.stock_name:
            print("Provide --stock-name")
            sys.exit(1)
        app = OptionsAnalysisApp()
        key = app.get_instrument_key_for_stock(args.stock_name)
        print(f"{args.stock_name}: {key}" if key else f"No key for {args.stock_name}")
    elif args.mode == "list-keys":
        app = OptionsAnalysisApp()
        for stock, key in sorted(app.get_available_instrument_keys().items()):
            print(f"  {stock}: {key}")
