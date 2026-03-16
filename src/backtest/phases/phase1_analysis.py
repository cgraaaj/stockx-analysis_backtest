"""Phase 1: OI Analysis + Ranking + Market Context tagging.

Optimised with concurrent async API calls — tick sub-batches and stock
batches are fetched in parallel via asyncio.gather.
"""

import asyncio
import json
import logging
import pickle
import time

import pandas as pd

from src.analysis.config.settings import (
    market_context_config,
    trading_config,
)
from src.analysis.models.entities import Stock
from src.analysis.services.analysis_service import AnalysisService, OptionRankingService
from src.analysis.services.market_context_service import MarketContextService
from src.analysis.views.export_view import ExportView
from src.backtest.config.schema import BacktestConfig
from src.backtest.core.analysis import analyze_stock, pick_expiry
from src.backtest.core.market_filter import (
    extract_preds,
    filter_by_cutoff,
    filter_preds_per_date,
    rebuild_entries,
)
from src.backtest.data import tickerflow as tf

logger = logging.getLogger("Phase1")


async def _fetch_batch_ticks(
    seqs: list[int], trade_date: str, tick_sub_size: int = 20,
) -> list[pd.DataFrame]:
    """Fetch all tick sub-batches for a stock batch concurrently."""
    tasks = []
    for ti in range(0, len(seqs), tick_sub_size):
        sub_seqs = seqs[ti : ti + tick_sub_size]
        tasks.append(
            tf.aget_ticks_batch(
                instrument_ids=sub_seqs,
                start=f"{trade_date} {trading_config.MARKET_START_TIME}",
                end=f"{trade_date} {trading_config.MARKET_END_TIME}",
            )
        )
    results = await asyncio.gather(*tasks, return_exceptions=True)
    frames = []
    for r in results:
        if isinstance(r, Exception):
            logger.warning(f"  Ticks sub-batch failed: {r}")
        elif not r.empty:
            frames.append(r)
    return frames


async def run(config: BacktestConfig, run_dir, *, predictions=None):
    """Execute Phase 1 and return (all_results, predictions, market_ctx_log).

    If *predictions* is already set (in-memory from a prior call) it is
    returned as-is — this path only matters when the orchestrator passes
    cached data.
    """
    start = time.time()

    dates = config.trading_dates
    logger.info(f"Phase 1: {len(dates)} trading dates ({dates[0]} -> {dates[-1]})")

    analysis_svc = AnalysisService()
    ranking_svc = OptionRankingService()
    market_svc = MarketContextService()

    logger.info(f"TickerFlow API: {tf._BASE_URL}")

    raw_stocks = await tf.aget_stocks()
    stocks = [
        Stock(
            id=str(s["id"]),
            name=s["name"],
            symbol=s["name"],
            is_active=s["is_active"],
            instrument_key=s.get("instrument_key"),
        )
        for s in raw_stocks
    ]
    expiry_dates = await tf.aget_expiries()
    logger.info(f"Loaded {len(stocks)} stocks, {len(expiry_dates)} expiry dates")

    all_results = []
    market_ctx_log: dict = {}
    date_index_contexts: dict = {}

    for trade_date in dates:
        date_start = time.time()
        expiry = pick_expiry(trade_date, expiry_dates)
        logger.info(f"--- {trade_date} (expiry={expiry}) ---")

        if market_svc.is_enabled:
            cutoff = config.signal_cutoff_time
            idx_ctx = await market_svc.evaluate_market_context(trade_date, cutoff_time=cutoff)
            primary_trend = market_svc.get_trend(market_context_config.PRIMARY_INDEX)
            market_ctx_log[trade_date] = {
                name: {"trend": ctx.trend, "confidence": ctx.confidence}
                for name, ctx in idx_ctx.items()
            }
            date_index_contexts[trade_date] = dict(idx_ctx)
            logger.info(f"  {market_context_config.PRIMARY_INDEX}: {primary_trend}")

        batch_size = config.batch_size
        date_results = []

        # --- Fetch instrument batches concurrently (all batches per date) ---
        batch_groups = []
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i : i + batch_size]
            stock_ids = [s.id for s in batch]
            batch_groups.append((batch, stock_ids))

        inst_tasks = [
            tf.aget_instruments_batch(stock_ids=ids, expiry=expiry)
            for _, ids in batch_groups
        ]
        inst_results = await asyncio.gather(*inst_tasks, return_exceptions=True)

        # --- For each batch that got instruments, fetch ticks concurrently ---
        tick_tasks = []
        tick_task_indices = []
        for idx, (inst_res, (batch, _)) in enumerate(zip(inst_results, batch_groups)):
            if isinstance(inst_res, Exception):
                logger.warning(f"  Instruments batch failed: {inst_res}")
                continue
            if inst_res.empty:
                continue
            seqs = inst_res["instrument_seq"].dropna().astype(int).tolist()
            if not seqs:
                continue
            tick_tasks.append(_fetch_batch_ticks(seqs, trade_date))
            tick_task_indices.append(idx)

        tick_results = await asyncio.gather(*tick_tasks, return_exceptions=True)

        # --- Analyze stocks with fetched data ---
        tick_map: dict[int, list[pd.DataFrame]] = {}
        for task_pos, batch_idx in enumerate(tick_task_indices):
            res = tick_results[task_pos]
            if isinstance(res, Exception):
                logger.warning(f"  Tick fetch failed for batch {batch_idx}: {res}")
                continue
            tick_map[batch_idx] = res

        for idx, (batch, _) in enumerate(batch_groups):
            batch_num = idx + 1
            total_batches = len(batch_groups)
            instrument_df = inst_results[idx]
            if isinstance(instrument_df, Exception) or instrument_df.empty:
                continue

            tick_frames = tick_map.get(idx, [])
            if not tick_frames:
                logger.info(f"  Batch {batch_num}/{total_batches}: no ticker data")
                continue
            ticker_df = pd.concat(tick_frames, ignore_index=True)

            valid = 0
            for stock in batch:
                try:
                    result = analyze_stock(stock, instrument_df, ticker_df, trade_date, analysis_svc)
                    if result:
                        date_results.append(result)
                        valid += 1
                except Exception as e:
                    logger.error(f"  Error on {stock.name}: {e}")

            logger.info(f"  Batch {batch_num}/{total_batches}: {valid}/{len(batch)} stocks with signals")

        all_results.extend(date_results)
        logger.info(f"  {trade_date} done: {len(date_results)} stocks ({time.time() - date_start:.1f}s)")

    logger.info(f"Total analysis results: {len(all_results)} stocks across {len(dates)} dates")

    preds = ranking_svc.rank_options(all_results)

    if market_svc.is_enabled and date_index_contexts:
        call_preds = extract_preds(preds.call, "call")
        put_preds = extract_preds(preds.put, "put")
        before = len(call_preds) + len(put_preds)

        filtered_calls = filter_preds_per_date(call_preds, date_index_contexts, market_svc)
        filtered_puts = filter_preds_per_date(put_preds, date_index_contexts, market_svc)

        after = len(filtered_calls) + len(filtered_puts)
        logger.info(f"Market context tagging (per-date): {before} -> {after} predictions (all kept)")
        preds.call = rebuild_entries(filtered_calls, preds.call)
        preds.put = rebuild_entries(filtered_puts, preds.put)

    cutoff_time = config.signal_cutoff_time
    if cutoff_time:
        preds.call = filter_by_cutoff(preds.call, cutoff_time)
        preds.put = filter_by_cutoff(preds.put, cutoff_time)

    # -- persist outputs --
    export = ExportView()
    excel_fn = str(run_dir / f"predictions_{config.name}.xlsx")
    export_result = export.export_predictions_to_excel(preds, excel_fn)
    if not export_result.success:
        logger.error(f"Excel export FAILED: {export_result.error_message}")

    pickle_fn = str(run_dir / f"all_results_{config.name}.pickle")
    with open(pickle_fn, "wb") as f:
        pickle.dump(all_results, f)

    call_count = sum(len(e.get("stock_data", [])) for e in preds.call)
    put_count = sum(len(e.get("stock_data", [])) for e in preds.put)

    summary = {
        "success": True,
        "backtest_name": config.name,
        "description": config.description,
        "trading_dates": dates,
        "num_trading_days": len(dates),
        "execution_time_minutes": round((time.time() - start) / 60, 2),
        "total_stocks_processed": len(all_results),
        "call_predictions": call_count,
        "put_predictions": put_count,
        "market_context": market_ctx_log,
        "results_directory": str(run_dir),
    }
    with open(run_dir / "backtest_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info("PHASE 1 COMPLETE")
    logger.info(f"  Duration        : {elapsed / 60:.1f} min")
    logger.info(f"  Call predictions : {call_count}")
    logger.info(f"  Put predictions  : {put_count}")
    logger.info("=" * 60)

    return all_results, preds, market_ctx_log
