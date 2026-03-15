"""
Options Analysis Backtest Runner
================================
Points-based dry run: validates signal quality and win rate.

Runs the full pipeline per-date:
  market context → API fetch → OI analysis → ranking → market filter → export

All output files land in src/backtest/results/<run_name>_<timestamp>/

Data source: TickerFlow API (no direct DB access).
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from functools import lru_cache
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

import pandas as pd

from src.analysis.config.settings import (
    analysis_config,
    market_context_config,
    processing_config,
    strategy_config,
    trading_config,
)
from src.analysis.models.entities import CEPEPair, StockPrediction, Stock
from src.analysis.services.analysis_service import AnalysisService, OptionRankingService
from src.analysis.services.market_context_service import MarketContextService
from src.backtest import tickerflow_client as tf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("Backtest")

RESULTS_DIR = Path(__file__).parent / "results"

# ======================================================================
# Backtest parameters
# ======================================================================

BACKTEST_CONFIG = {
    "name": "full_2025_pct_sl",
    "description": (
        "Full year 2025 backtest (Feb-Dec): VWAP bias filter, 1% percentage SL, "
        "1:3 R:R, NIFTY VWAP trend (no lookahead, cutoff at signal time), "
        "hard exit at 15:15, pure OI magnitude grading"
    ),
    "trading_dates": [
        "2025-02-03", "2025-02-04", "2025-02-05", "2025-02-06", "2025-02-07",
        "2025-02-10", "2025-02-11", "2025-02-12", "2025-02-13", "2025-02-14",
        "2025-02-17", "2025-02-18", "2025-02-19", "2025-02-20", "2025-02-21",
        "2025-02-24", "2025-02-25", "2025-02-27", "2025-02-28",
        "2025-03-03", "2025-03-04", "2025-03-05", "2025-03-06", "2025-03-07",
        "2025-03-10", "2025-03-11", "2025-03-12", "2025-03-13", "2025-03-17",
        "2025-03-18", "2025-03-19", "2025-03-20", "2025-03-21", "2025-03-24",
        "2025-03-25", "2025-03-26", "2025-03-27", "2025-03-28",
        "2025-04-01", "2025-04-02", "2025-04-04", "2025-04-07", "2025-04-08",
        "2025-04-09", "2025-04-11", "2025-04-15", "2025-04-16", "2025-04-17",
        "2025-04-21", "2025-04-22", "2025-04-23", "2025-04-24", "2025-04-25",
        "2025-04-28", "2025-04-29", "2025-04-30",
        "2025-05-02", "2025-05-05", "2025-05-06", "2025-05-07", "2025-05-08",
        "2025-05-09", "2025-05-12", "2025-05-13", "2025-05-14", "2025-05-15",
        "2025-05-16", "2025-05-19", "2025-05-20", "2025-05-21", "2025-05-22",
        "2025-05-23", "2025-05-26", "2025-05-27", "2025-05-28", "2025-05-29",
        "2025-05-30",
        "2025-06-02", "2025-06-04", "2025-06-05", "2025-06-06", "2025-06-09",
        "2025-06-10", "2025-06-11", "2025-06-12", "2025-06-13", "2025-06-16",
        "2025-06-17", "2025-06-18", "2025-06-19", "2025-06-20", "2025-06-23",
        "2025-06-24", "2025-06-25", "2025-06-26", "2025-06-27", "2025-06-30",
        "2025-07-01", "2025-07-02", "2025-07-03", "2025-07-04", "2025-07-07",
        "2025-07-08", "2025-07-09", "2025-07-10", "2025-07-11", "2025-07-14",
        "2025-07-15", "2025-07-16", "2025-07-17", "2025-07-18", "2025-07-21",
        "2025-07-22", "2025-07-23", "2025-07-24", "2025-07-25", "2025-07-28",
        "2025-07-29", "2025-07-30", "2025-07-31",
        "2025-08-01", "2025-08-04", "2025-08-05", "2025-08-06", "2025-08-07",
        "2025-08-08", "2025-08-11", "2025-08-12", "2025-08-13", "2025-08-14",
        "2025-08-18", "2025-08-19", "2025-08-20", "2025-08-21", "2025-08-22",
        "2025-08-25", "2025-08-26", "2025-08-28", "2025-08-29",
        "2025-09-01", "2025-09-02", "2025-09-03", "2025-09-04", "2025-09-05",
        "2025-09-08", "2025-09-09", "2025-09-10", "2025-09-11", "2025-09-12",
        "2025-09-15", "2025-09-16", "2025-09-17", "2025-09-18", "2025-09-19",
        "2025-09-22", "2025-09-23", "2025-09-24", "2025-09-25", "2025-09-26",
        "2025-09-29", "2025-09-30",
        "2025-10-01", "2025-10-06", "2025-10-07", "2025-10-08", "2025-10-09",
        "2025-10-10", "2025-10-13", "2025-10-14", "2025-10-15", "2025-10-16",
        "2025-10-17", "2025-10-20", "2025-10-23", "2025-10-24", "2025-10-27",
        "2025-10-28", "2025-10-29", "2025-10-30", "2025-10-31",
        "2025-11-03", "2025-11-04", "2025-11-06", "2025-11-07", "2025-11-10",
        "2025-11-11", "2025-11-12", "2025-11-13", "2025-11-14", "2025-11-17",
        "2025-11-18", "2025-11-19", "2025-11-20", "2025-11-21", "2025-11-24",
        "2025-11-25", "2025-11-26", "2025-11-27", "2025-11-28",
        "2025-12-01", "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05",
        "2025-12-09", "2025-12-10", "2025-12-11", "2025-12-12", "2025-12-15",
        "2025-12-16", "2025-12-17", "2025-12-18", "2025-12-19", "2025-12-22",
        "2025-12-23", "2025-12-24", "2025-12-26", "2025-12-29", "2025-12-30",
        "2025-12-31",
    ],
    "signal_cutoff_time": "09:45:00",
    "strategy": {
        "reward_ratio": 3.0,
        "sl_method": "percentage",
        "sl_percentage": 1.0,
        "max_holding_candles": 24,
        "entry_candle_offset": 1,
        "use_vwap_bias": True,
        "max_exit_time": "15:15:00",
    },
    "analysis": {
        "oi_weight_blend": 1.0,
        "tn_ratio_threshold": 60,
    },
    "market_context": {
        "enabled": True,
        "primary_index": "NIFTY 50",
        "filter_mode": "tag",
        "index_trend_method": "vwap",
        "truncate_to_signal_time": True,
    },
}


def apply_config_overrides():
    sc = BACKTEST_CONFIG["strategy"]
    strategy_config.REWARD_RATIO = sc["reward_ratio"]
    strategy_config.SL_METHOD = sc["sl_method"]
    strategy_config.MAX_HOLDING_CANDLES = sc["max_holding_candles"]
    strategy_config.ENTRY_CANDLE_OFFSET = sc["entry_candle_offset"]
    strategy_config.USE_VWAP_BIAS = sc.get("use_vwap_bias", True)
    strategy_config.SL_PERCENTAGE = sc.get("sl_percentage", 1.0)
    strategy_config.MAX_EXIT_TIME = sc.get("max_exit_time", "15:15:00")

    ac = BACKTEST_CONFIG["analysis"]
    analysis_config.OI_WEIGHT_BLEND = ac["oi_weight_blend"]
    analysis_config.USE_OI_MAGNITUDE_WEIGHTING = True
    analysis_config.TN_RATIO_THRESHOLD = ac["tn_ratio_threshold"]

    mc = BACKTEST_CONFIG["market_context"]
    market_context_config.ENABLED = mc["enabled"]
    market_context_config.PRIMARY_INDEX = mc["primary_index"]
    market_context_config.FILTER_MODE = mc["filter_mode"]
    market_context_config.INDEX_TREND_METHOD = mc.get("index_trend_method", "vwap")
    market_context_config.TRUNCATE_TO_SIGNAL_TIME = mc.get("truncate_to_signal_time", True)

    logger.info("Config overrides applied:")
    logger.info(f"  R:R             = 1:{sc['reward_ratio']}")
    logger.info(f"  SL method       = {sc['sl_method']}")
    logger.info(f"  VWAP bias       = {sc.get('use_vwap_bias', True)}")
    logger.info(f"  SL percentage   = {sc.get('sl_percentage', 1.0)}%")
    logger.info(f"  Hard exit       = {sc.get('max_exit_time', '15:15:00')}")
    logger.info(f"  OI weight blend = {ac['oi_weight_blend']} (pure OI magnitude)")
    logger.info(f"  Market context  = {mc['filter_mode']} on {mc['primary_index']} ({mc.get('index_trend_method', 'vwap')})")
    logger.info(f"  Lookahead fix   = {mc.get('truncate_to_signal_time', True)}")


def setup_results_dir() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = BACKTEST_CONFIG["name"]
    run_dir = RESULTS_DIR / f"{name}_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


# ======================================================================
# Timestamp helper (replaces DatabaseService.get_trading_timestamps)
# ======================================================================

@lru_cache(maxsize=100)
def get_trading_timestamps(trade_date: str):
    start = pd.Timestamp(f"{trade_date} {trading_config.MARKET_START_TIME}")
    end = pd.Timestamp(f"{trade_date} 15:29:00")
    return pd.date_range(start=start, end=end, freq="1min")


# ======================================================================
# Core logic — direct service calls (no AnalysisController dependency)
# ======================================================================

def pick_expiry(trade_date: str, expiry_dates: list) -> str:
    from datetime import datetime as _dt
    trade_dt = _dt.strptime(trade_date, "%Y-%m-%d")
    ym = trade_dt.strftime("%Y-%m")
    for exp in expiry_dates:
        exp_dt = _dt.strptime(exp, "%Y-%m-%d")
        if exp_dt.strftime("%Y-%m") == ym and exp_dt >= trade_dt:
            return exp
    for exp in expiry_dates:
        if _dt.strptime(exp, "%Y-%m-%d") >= trade_dt:
            return exp
    return expiry_dates[-1] if expiry_dates else trade_date


def analyze_stock(stock, instrument_df, ticker_df, trade_date, analysis_svc):
    stock_insts = instrument_df[instrument_df["stock_id"].astype(str) == stock.id]
    if stock_insts.empty:
        return None

    stock_seqs = stock_insts["instrument_seq"].dropna().astype(int).tolist()
    stock_ticks = ticker_df[ticker_df["instrument_id"].isin(stock_seqs)]
    if stock_ticks.empty:
        return None

    ce = stock_insts[stock_insts["instrument_type"] == "CE"][
        ["instrument_seq", "strike_price"]
    ].rename(columns={"instrument_seq": "ce_seq"})
    pe = stock_insts[stock_insts["instrument_type"] == "PE"][
        ["instrument_seq", "strike_price"]
    ].rename(columns={"instrument_seq": "pe_seq"})
    pairs_df = pd.merge(ce, pe, on="strike_price", how="outer").sort_values("strike_price")
    if pairs_df.empty:
        return None

    timestamps = get_trading_timestamps(trade_date)

    processed = []
    for _, row in pairs_df.iterrows():
        pair = CEPEPair(
            ce_seq=int(row["ce_seq"]) if pd.notna(row.get("ce_seq")) else None,
            pe_seq=int(row["pe_seq"]) if pd.notna(row.get("pe_seq")) else None,
            strike_price=row["strike_price"],
        )
        df = analysis_svc.process_ce_pe_pair(stock_ticks, pair, trade_date, timestamps)
        if not df.empty:
            processed.append(df)

    if not processed:
        return None

    combined = pd.concat(processed, ignore_index=True)
    sim_dfs = analysis_svc.get_min_simulation(combined, trading_config.DEFAULT_INTERVAL)
    sim_results = []
    for sdf in sim_dfs:
        if not sdf.empty:
            analysis = analysis_svc.trend_n_grade_analysis(sdf)
            if analysis:
                sim_results.append(analysis)

    if not sim_results:
        return None
    return {"name": stock.name, "opt_data": sim_results}


async def run():
    start = time.time()
    apply_config_overrides()
    run_dir = setup_results_dir()
    logger.info(f"Results directory: {run_dir}")

    with open(run_dir / "backtest_config.json", "w") as f:
        json.dump(BACKTEST_CONFIG, f, indent=2)

    dates = BACKTEST_CONFIG["trading_dates"]
    logger.info(f"Backtest: {len(dates)} trading dates ({dates[0]} → {dates[-1]})")

    analysis_svc = AnalysisService()
    ranking_svc = OptionRankingService()
    market_svc = MarketContextService()

    logger.info(f"TickerFlow API: {tf._BASE_URL}")

    raw_stocks = tf.get_stocks()
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
    expiry_dates = tf.get_expiries()
    logger.info(f"Loaded {len(stocks)} stocks, {len(expiry_dates)} expiry dates")

    all_results = []
    market_ctx_log = {}
    date_index_contexts = {}

    for trade_date in dates:
        date_start = time.time()
        expiry = pick_expiry(trade_date, expiry_dates)
        logger.info(f"--- {trade_date} (expiry={expiry}) ---")

        if market_svc.is_enabled:
            cutoff = BACKTEST_CONFIG.get("signal_cutoff_time")
            idx_ctx = await market_svc.evaluate_market_context(trade_date, cutoff_time=cutoff)
            primary_trend = market_svc.get_trend(market_context_config.PRIMARY_INDEX)
            market_ctx_log[trade_date] = {
                name: {"trend": ctx.trend, "confidence": ctx.confidence}
                for name, ctx in idx_ctx.items()
            }
            date_index_contexts[trade_date] = dict(idx_ctx)
            logger.info(f"  {market_context_config.PRIMARY_INDEX}: {primary_trend}")

        # batch_size = processing_config.BATCH_SIZE
        batch_size = 10
        date_results = []
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(stocks) - 1) // batch_size + 1

            stock_ids = [s.id for s in batch]
            try:
                instrument_df = tf.get_instruments_batch(stock_ids=stock_ids, expiry=expiry)
            except Exception as e:
                logger.warning(f"  Instruments batch failed ({len(stock_ids)} ids): {e}")
                continue
            if instrument_df.empty:
                logger.info(f"  Batch {batch_num}/{total_batches}: no instruments")
                continue

            seqs = instrument_df["instrument_seq"].dropna().astype(int).tolist()
            tick_frames = []
            tick_sub_size = 20
            for ti in range(0, len(seqs), tick_sub_size):
                sub_seqs = seqs[ti:ti + tick_sub_size]
                try:
                    sub_df = tf.get_ticks_batch(
                        instrument_ids=sub_seqs,
                        start=f"{trade_date} {trading_config.MARKET_START_TIME}",
                        end=f"{trade_date} {trading_config.MARKET_END_TIME}",
                    )
                    if not sub_df.empty:
                        tick_frames.append(sub_df)
                except Exception as e:
                    logger.warning(f"  Ticks sub-batch failed ({len(sub_seqs)} seqs): {e}")
            if not tick_frames:
                logger.info(f"  Batch {batch_num}/{total_batches}: no ticker data")
                continue
            ticker_df = pd.concat(tick_frames, ignore_index=True)

            valid = 0
            for stock in batch:
                try:
                    result = analyze_stock(
                        stock, instrument_df, ticker_df, trade_date,
                        analysis_svc,
                    )
                    if result:
                        date_results.append(result)
                        valid += 1
                except Exception as e:
                    logger.error(f"  Error on {stock.name}: {e}")

            logger.info(f"  Batch {batch_num}/{total_batches}: {valid}/{len(batch)} stocks with signals")

        all_results.extend(date_results)
        logger.info(f"  {trade_date} done: {len(date_results)} stocks ({time.time()-date_start:.1f}s)")

    logger.info(f"Total analysis results: {len(all_results)} stocks across {len(dates)} dates")

    predictions = ranking_svc.rank_options(all_results)

    if market_svc.is_enabled and date_index_contexts:
        call_preds = _extract_preds(predictions.call, "call")
        put_preds = _extract_preds(predictions.put, "put")
        before = len(call_preds) + len(put_preds)

        filtered_calls = _filter_preds_per_date(call_preds, date_index_contexts, market_svc)
        filtered_puts = _filter_preds_per_date(put_preds, date_index_contexts, market_svc)

        after = len(filtered_calls) + len(filtered_puts)
        logger.info(f"Market context tagging (per-date): {before} → {after} predictions (all kept)")
        predictions.call = _rebuild_entries(filtered_calls, predictions.call)
        predictions.put = _rebuild_entries(filtered_puts, predictions.put)

    from src.analysis.views.export_view import ExportView
    export = ExportView()
    excel_fn = str(run_dir / f"predictions_{BACKTEST_CONFIG['name']}.xlsx")
    export_result = export.export_predictions_to_excel(predictions, excel_fn)
    if not export_result.success:
        logger.error(f"Excel export FAILED: {export_result.error_message}")

    import pickle
    pickle_fn = str(run_dir / f"all_results_{BACKTEST_CONFIG['name']}.pickle")
    with open(pickle_fn, "wb") as f:
        pickle.dump(all_results, f)

    elapsed = time.time() - start

    call_count = sum(len(e.get("stock_data", [])) for e in predictions.call)
    put_count = sum(len(e.get("stock_data", [])) for e in predictions.put)

    summary = {
        "success": True,
        "backtest_name": BACKTEST_CONFIG["name"],
        "description": BACKTEST_CONFIG["description"],
        "trading_dates": dates,
        "num_trading_days": len(dates),
        "execution_time_minutes": round(elapsed / 60, 2),
        "total_stocks_processed": len(all_results),
        "call_predictions": call_count,
        "put_predictions": put_count,
        "market_context": market_ctx_log,
        "results_directory": str(run_dir),
    }

    with open(run_dir / "backtest_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    logger.info("=" * 60)
    logger.info("BACKTEST COMPLETE")
    logger.info("=" * 60)
    logger.info(f"  Duration        : {elapsed/60:.1f} min")
    logger.info(f"  Trading days    : {len(dates)}")
    logger.info(f"  Stocks analyzed : {len(all_results)}")
    logger.info(f"  Call predictions: {call_count}")
    logger.info(f"  Put predictions : {put_count}")
    logger.info(f"  Results in      : {run_dir}")


# ======================================================================
# Helpers for market context filter
# ======================================================================

def _extract_preds(date_entries, option_type):
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


def _compute_trend_alignment(option_type, trend):
    """Determine alignment between option direction and market trend."""
    if trend == "neutral":
        return "neutral"
    if option_type == "call":
        return "along" if trend == "bullish" else "against"
    else:  # put
        return "along" if trend == "bearish" else "against"


def _filter_preds_per_date(preds, date_index_contexts, market_svc):
    """Tag each prediction with market_trend and trend_alignment (keep all)."""
    for pred in preds:
        pred_date = pred.timestamp.strftime("%Y-%m-%d") if hasattr(pred.timestamp, "strftime") else str(pred.timestamp)[:10]
        ctx = date_index_contexts.get(pred_date)
        if ctx is None:
            pred.market_trend = "neutral"
            pred.trend_alignment = "neutral"
            continue

        idx_name = market_svc.get_index_for_stock(pred.stock)
        idx_ctx = ctx.get(idx_name)
        if idx_ctx is None:
            pred.market_trend = "neutral"
            pred.trend_alignment = "neutral"
            continue

        trend = idx_ctx.trend
        pred.market_trend = trend
        pred.trend_alignment = _compute_trend_alignment(pred.option_type, trend)

    return preds


def _rebuild_entries(filtered, original_entries):
    trend_map = {(p.stock, p.timestamp): p for p in filtered}
    rebuilt = []
    for entry in original_entries:
        kept_data = []
        for sd in entry.get("stock_data", []):
            key = (sd["stock"], sd["time_stamp"])
            pred = trend_map.get(key)
            if pred is not None:
                sd["market_trend"] = pred.market_trend
                sd["trend_alignment"] = pred.trend_alignment
                kept_data.append(sd)
        if kept_data:
            rebuilt.append({**entry, "stock_data": kept_data})
    return rebuilt


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Options Analysis Backtest Runner")
    parser.add_argument("--date", type=str, help="Run for a single date (YYYY-MM-DD) instead of the full list")
    args = parser.parse_args()

    if args.date:
        BACKTEST_CONFIG["name"] = f"single_day_{args.date}"
        BACKTEST_CONFIG["description"] = f"Single-date test run for {args.date}"
        BACKTEST_CONFIG["trading_dates"] = [args.date]
        logger.info(f"*** SINGLE-DATE TEST: {args.date} ***")

    asyncio.run(run())
