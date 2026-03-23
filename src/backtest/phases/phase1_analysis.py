"""Phase 1: OI Analysis + Ranking + Market Context tagging.

Optimised with:
  - Concurrent async API calls within each date (batches + tick sub-batches)
  - Date-level parallelism — multiple trading dates processed concurrently

Environment tuning (optional):
  BACKTEST_DATE_CONCURRENCY — how many trading dates run at once (default 6, max 32).
  TICKERFLOW_API_CONCURRENCY — max concurrent HTTP calls to TickerFlow (default 40);
    raise both together when pushing throughput; watch for 429/OOM on the client or API.
"""

import asyncio
import json
import logging
import os
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


def _phase1_signal_cutoff(config: BacktestConfig) -> str | None:
    """Return cutoff time string, or None to keep all signals (no filter_by_cutoff).

    None and empty/whitespace disable the post-ranking clock filter so predictions
    across the full analysis session (e.g. 10:05, 14:05) are retained.
    """
    c = config.signal_cutoff_time
    if c is None:
        return None
    s = str(c).strip()
    return s or None


def _date_concurrency_from_env() -> int:
    """How many calendar dates to process concurrently in Phase 1."""
    raw = os.getenv("BACKTEST_DATE_CONCURRENCY")
    default = 6
    if raw is None or not str(raw).strip():
        return default
    try:
        n = int(str(raw).strip())
    except ValueError:
        logger.warning(
            "Invalid BACKTEST_DATE_CONCURRENCY=%r, using default %d", raw, default,
        )
        return default
    min_v, max_v = 1, 32
    clamped = max(min_v, min(n, max_v))
    if clamped != n:
        logger.warning(
            "BACKTEST_DATE_CONCURRENCY=%r out of range [%d, %d], using %d",
            raw, min_v, max_v, clamped,
        )
    return clamped


DATE_CONCURRENCY = _date_concurrency_from_env()


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


async def _process_single_date(
    trade_date: str,
    stocks: list[Stock],
    expiry_dates: list[str],
    config: BacktestConfig,
    analysis_svc: AnalysisService,
    date_semaphore: asyncio.Semaphore,
) -> dict:
    """Process one trading date end-to-end: fetch data + analyze.

    Returns {
        "trade_date": str,
        "results": list,
        "market_ctx": dict | None,
        "index_ctx": dict | None,
    }
    """
    async with date_semaphore:
        date_start = time.time()
        expiry = pick_expiry(trade_date, expiry_dates)
        logger.info(f"--- {trade_date} (expiry={expiry}) ---")

        market_ctx_entry = None
        index_ctx_entry = None
        market_svc = MarketContextService()
        if market_svc.is_enabled:
            cutoff = _phase1_signal_cutoff(config)
            idx_ctx = await market_svc.evaluate_market_context(trade_date, cutoff_time=cutoff)
            primary = market_context_config.PRIMARY_INDEX
            primary_trend = idx_ctx.get(primary)
            market_ctx_entry = {
                name: {"trend": ctx.trend, "confidence": ctx.confidence}
                for name, ctx in idx_ctx.items()
            }
            index_ctx_entry = dict(idx_ctx)
            logger.info(f"  {primary}: {primary_trend.trend if primary_trend else 'N/A'}")

        batch_size = config.batch_size
        date_results = []

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

        tick_tasks = []
        tick_task_indices = []
        for idx, (inst_res, (batch, _)) in enumerate(zip(inst_results, batch_groups)):
            if isinstance(inst_res, Exception):
                logger.warning(f"  [{trade_date}] Instruments batch failed: {inst_res}")
                continue
            if inst_res.empty:
                continue
            seqs = inst_res["instrument_seq"].dropna().astype(int).tolist()
            if not seqs:
                continue
            tick_tasks.append(_fetch_batch_ticks(seqs, trade_date))
            tick_task_indices.append(idx)

        tick_results = await asyncio.gather(*tick_tasks, return_exceptions=True)

        tick_map: dict[int, list[pd.DataFrame]] = {}
        for task_pos, batch_idx in enumerate(tick_task_indices):
            res = tick_results[task_pos]
            if isinstance(res, Exception):
                logger.warning(f"  [{trade_date}] Tick fetch failed for batch {batch_idx}: {res}")
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
                    logger.error(f"  [{trade_date}] Error on {stock.name}: {e}")

            logger.info(
                f"  [{trade_date}] Batch {batch_num}/{total_batches}: "
                f"{valid}/{len(batch)} stocks with signals"
            )

        logger.info(
            f"  {trade_date} done: {len(date_results)} stocks "
            f"({time.time() - date_start:.1f}s)"
        )

        return {
            "trade_date": trade_date,
            "results": date_results,
            "market_ctx": market_ctx_entry,
            "index_ctx": index_ctx_entry,
        }


async def run(config: BacktestConfig, run_dir, *, predictions=None):
    """Execute Phase 1 and return (all_results, predictions, market_ctx_log).

    Processes up to DATE_CONCURRENCY trading dates in parallel.
    """
    start = time.time()

    dates = config.trading_dates
    logger.info(f"Phase 1: {len(dates)} trading dates ({dates[0]} -> {dates[-1]})")
    logger.info(f"Date-level parallelism: {DATE_CONCURRENCY} concurrent dates (BACKTEST_DATE_CONCURRENCY)")
    logger.info(
        "TickerFlow: %s | max concurrent HTTP requests: %d (TICKERFLOW_API_CONCURRENCY)",
        tf._BASE_URL,
        tf.get_api_concurrency_limit(),
    )
    analysis_svc = AnalysisService()
    ranking_svc = OptionRankingService()

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

    date_semaphore = asyncio.Semaphore(DATE_CONCURRENCY)

    date_tasks = [
        _process_single_date(
            trade_date, stocks, expiry_dates, config, analysis_svc, date_semaphore,
        )
        for trade_date in dates
    ]

    date_outputs = await asyncio.gather(*date_tasks, return_exceptions=True)

    all_results = []
    market_ctx_log: dict = {}
    date_index_contexts: dict = {}

    for output in date_outputs:
        if isinstance(output, Exception):
            logger.error(f"  Date processing failed: {output}")
            continue
        all_results.extend(output["results"])
        td = output["trade_date"]
        if output["market_ctx"] is not None:
            market_ctx_log[td] = output["market_ctx"]
        if output["index_ctx"] is not None:
            date_index_contexts[td] = output["index_ctx"]

    logger.info(f"Total analysis results: {len(all_results)} stocks across {len(dates)} dates")

    preds = ranking_svc.rank_options(all_results)

    # Shared instance for post-ranking market tagging (per-stock index mapping)
    market_svc = MarketContextService()

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

    cutoff_time = _phase1_signal_cutoff(config)
    if cutoff_time:
        preds.call = filter_by_cutoff(preds.call, cutoff_time)
        preds.put = filter_by_cutoff(preds.put, cutoff_time)
    else:
        logger.info(
            "signal_cutoff_time unset — skipping filter_by_cutoff (all in-session signals kept)"
        )

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
