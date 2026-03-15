"""
March 2026 Backtest: Full Pipeline
===================================
Runs Phase 1 (OI analysis) → Phase 2 (stock sim) → Phase 3 (F&O)
for March 2026 trading dates.

Config: Grade A+B | 10% SL / 30% Target | 10L Capital | Max 5L/trade | 5-min buffer

Usage:
  python -m src.backtest.run_mar2026
"""

import asyncio
import csv
import json
import math
import os
import sys
import time
import urllib.parse
from datetime import datetime
from itertools import groupby
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

import aiohttp
import pandas as pd

from src.analysis.config.settings import (
    analysis_config, market_context_config, processing_config,
    strategy_config, trading_config,
)
from src.analysis.models.entities import CEPEPair, Stock
from src.analysis.services.analysis_service import AnalysisService, OptionRankingService
from src.analysis.services.market_context_service import MarketContextService
from src.backtest import tickerflow_client as tf
from src.backtest.run_phase3_fno import (
    pick_expiry_for_date,
    compute_rsi,
    get_premium_rsi_at_signal,
)

# ── Config ────────────────────────────────────────────────────────────
TRADING_DATES = [
    "2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05",
]
SIGNAL_CUTOFF_TIME = "09:45:00"

STARTING_CAPITAL = 10_00_000
MAX_CAPITAL_PER_TRADE = 5_00_000
SL_PCT = 10.0
TARGET_PCT = 30.0
ACCEPTED_GRADES = ["A", "B"]
RSI_OVERBOUGHT = 80.0
ENTRY_OFFSET = 5
MAX_EXIT_TIME = "15:15:00"

UPSTOX_BASE = "https://api.upstox.com/v2/historical-candle"

RESULTS_DIR = Path(__file__).parent / "results"
# ──────────────────────────────────────────────────────────────────────

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
logger = logging.getLogger("Mar2026")


def apply_overrides():
    strategy_config.REWARD_RATIO = 2.0
    strategy_config.SL_METHOD = "percentage"
    strategy_config.SL_PERCENTAGE = 1.0
    strategy_config.MAX_HOLDING_CANDLES = 24
    strategy_config.ENTRY_CANDLE_OFFSET = 1
    strategy_config.USE_VWAP_BIAS = True
    strategy_config.MAX_EXIT_TIME = "15:15:00"
    analysis_config.OI_WEIGHT_BLEND = 1.0
    analysis_config.USE_OI_MAGNITUDE_WEIGHTING = True
    analysis_config.TN_RATIO_THRESHOLD = 60
    market_context_config.ENABLED = True
    market_context_config.PRIMARY_INDEX = "NIFTY 50"
    market_context_config.FILTER_MODE = "block"
    market_context_config.INDEX_TREND_METHOD = "vwap"
    market_context_config.TRUNCATE_TO_SIGNAL_TIME = True


def get_trading_timestamps(trade_date):
    start = pd.Timestamp(f"{trade_date} {trading_config.MARKET_START_TIME}")
    end = pd.Timestamp(f"{trade_date} 15:29:00")
    return pd.date_range(start=start, end=end, freq="1min")


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


def simulate_option(candles, signal_time, entry_offset, sl_pct, target_pct, max_exit_time):
    max_exit_ts = datetime.strptime(
        f"{signal_time.strftime('%Y-%m-%d')} {max_exit_time}", "%Y-%m-%d %H:%M:%S"
    )
    signal_idx = candles[candles["time_stamp"] <= signal_time].index
    if signal_idx.empty:
        return None
    entry_idx = signal_idx[-1] + entry_offset
    if entry_idx >= len(candles):
        return None
    entry_row = candles.iloc[entry_idx]
    entry_premium = float(entry_row["open"])
    entry_time = entry_row["time_stamp"]
    if entry_premium <= 0:
        return None
    sl_premium = entry_premium * (1 - sl_pct / 100.0)
    target_premium = entry_premium * (1 + target_pct / 100.0)
    for i in range(entry_idx + 1, len(candles)):
        row = candles.iloc[i]
        ts = row["time_stamp"]
        high, low, close = float(row["high"]), float(row["low"]), float(row["close"])
        if ts >= max_exit_ts:
            return dict(outcome="time_exit", entry_premium=entry_premium,
                        exit_premium=close, entry_time=entry_time, exit_time=ts,
                        sl_premium=sl_premium, target_premium=target_premium,
                        holding_candles=i - entry_idx)
        if low <= sl_premium:
            return dict(outcome="sl_hit", entry_premium=entry_premium,
                        exit_premium=sl_premium, entry_time=entry_time, exit_time=ts,
                        sl_premium=sl_premium, target_premium=target_premium,
                        holding_candles=i - entry_idx)
        if high >= target_premium:
            return dict(outcome="target_hit", entry_premium=entry_premium,
                        exit_premium=target_premium, entry_time=entry_time, exit_time=ts,
                        sl_premium=sl_premium, target_premium=target_premium,
                        holding_candles=i - entry_idx)
    last = candles.iloc[-1]
    return dict(outcome="time_exit", entry_premium=entry_premium,
                exit_premium=float(last["close"]), entry_time=entry_time,
                exit_time=last["time_stamp"], sl_premium=sl_premium,
                target_premium=target_premium,
                holding_candles=len(candles) - 1 - entry_idx)


async def fetch_stock_candles(session, instrument_key, date_str, stock_name):
    encoded = urllib.parse.quote(instrument_key, safe="")
    url = f"{UPSTOX_BASE}/{encoded}/1minute/{date_str}/{date_str}"
    for attempt in range(3):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "success":
                        candles = data.get("data", {}).get("candles", [])
                        if candles:
                            df = pd.DataFrame(candles, columns=[
                                "timestamp", "open", "high", "low", "close", "volume", "oi",
                            ])
                            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
                            df.sort_values("timestamp", inplace=True)
                            df.reset_index(drop=True, inplace=True)
                            return df
        except Exception:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    return pd.DataFrame()


async def run():
    start = time.time()
    apply_overrides()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = RESULTS_DIR / f"mar2026_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Results directory: {run_dir}")

    # ── Phase 1: OI Signal Generation ─────────────────────────────────
    logger.info(f"Phase 1: Analyzing {len(TRADING_DATES)} dates")
    analysis_svc = AnalysisService()
    ranking_svc = OptionRankingService()
    market_svc = MarketContextService()

    raw_stocks = tf.get_stocks()
    stocks = [
        Stock(id=str(s["id"]), name=s["name"], symbol=s["name"],
              is_active=s["is_active"], instrument_key=s.get("instrument_key"))
        for s in raw_stocks
    ]
    expiry_dates = tf.get_expiries()
    logger.info(f"Loaded {len(stocks)} stocks, {len(expiry_dates)} expiry dates")

    all_results = []
    date_index_contexts = {}

    for trade_date in TRADING_DATES:
        from src.backtest.run_backtest import pick_expiry
        expiry = pick_expiry(trade_date, expiry_dates)
        logger.info(f"--- {trade_date} (expiry={expiry}) ---")

        if market_svc.is_enabled:
            idx_ctx = await market_svc.evaluate_market_context(trade_date, cutoff_time=SIGNAL_CUTOFF_TIME)
            primary_trend = market_svc.get_trend(market_context_config.PRIMARY_INDEX)
            date_index_contexts[trade_date] = dict(idx_ctx)
            logger.info(f"  {market_context_config.PRIMARY_INDEX}: {primary_trend}")

        batch_size = 10  # Smaller batch to avoid URL-too-long on instruments API
        date_results = []
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i + batch_size]
            stock_ids = [s.id for s in batch]
            try:
                instrument_df = tf.get_instruments_batch(stock_ids=stock_ids, expiry=expiry)
            except Exception as e:
                logger.warning(f"  Instruments batch failed ({len(stock_ids)} ids): {e}")
                continue
            if instrument_df.empty:
                continue
            seqs = instrument_df["instrument_seq"].dropna().astype(int).tolist()
            # Fetch ticks in sub-batches to avoid URL-too-long
            tick_frames = []
            tick_sub_size = 20
            for ti in range(0, len(seqs), tick_sub_size):
                sub_seqs = seqs[ti:ti + tick_sub_size]
                try:
                    sub_df = tf.get_ticks_batch(
                        instrument_ids=sub_seqs,
                        start=f"{trade_date} {trading_config.MARKET_START_TIME}",
                        end=f"{trade_date} {trading_config.TRADING_END_TIME}",
                    )
                    if not sub_df.empty:
                        tick_frames.append(sub_df)
                except Exception as e:
                    logger.warning(f"  Ticks sub-batch failed ({len(sub_seqs)} seqs): {e}")
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
                    logger.error(f"  Error on {stock.name}: {e}")
            bn = i // batch_size + 1
            tb = (len(stocks) - 1) // batch_size + 1
            logger.info(f"  Batch {bn}/{tb}: {valid}/{len(batch)} stocks with signals")

        all_results.extend(date_results)
        logger.info(f"  {trade_date} done: {len(date_results)} stocks")

    logger.info(f"Phase 1 complete: {len(all_results)} stock-date entries")

    # Rank predictions
    predictions = ranking_svc.rank_options(all_results)

    # Apply market context filter
    from src.backtest.run_backtest import _extract_preds, _filter_preds_per_date, _rebuild_entries
    from src.analysis.models.entities import StockPrediction
    if market_svc.is_enabled and date_index_contexts:
        call_preds = _extract_preds(predictions.call, "call")
        put_preds = _extract_preds(predictions.put, "put")
        before = len(call_preds) + len(put_preds)
        filtered_calls = _filter_preds_per_date(call_preds, date_index_contexts, market_svc)
        filtered_puts = _filter_preds_per_date(put_preds, date_index_contexts, market_svc)
        after = len(filtered_calls) + len(filtered_puts)
        logger.info(f"Market context filter: {before} → {after} predictions")
        predictions.call = _rebuild_entries(filtered_calls, predictions.call)
        predictions.put = _rebuild_entries(filtered_puts, predictions.put)

    # Extract all predictions as flat list
    all_signals = []
    for entry in predictions.call:
        for sd in entry.get("stock_data", []):
            all_signals.append({
                "stock": sd["stock"], "signal_time": sd["time_stamp"],
                "grade": sd["grade"], "option_type": "call",
            })
    for entry in predictions.put:
        for sd in entry.get("stock_data", []):
            all_signals.append({
                "stock": sd["stock"], "signal_time": sd["time_stamp"],
                "grade": sd["grade"], "option_type": "put",
            })

    # Filter to Grade A+B
    all_signals = [s for s in all_signals if s["grade"] in ACCEPTED_GRADES]
    all_signals.sort(key=lambda s: s["signal_time"])
    logger.info(f"Grade A+B signals: {len(all_signals)}")

    for s in all_signals:
        logger.info(f"  {s['signal_time']} | {s['stock']:15s} | {s['option_type']:4s} | Grade {s['grade']}")

    if not all_signals:
        logger.info("No Grade A+B signals found for March 2026")
        # Save empty summary
        with open(run_dir / "mar2026_summary.json", "w") as f:
            json.dump({"signals": 0, "trades": 0, "total_pnl": 0}, f, indent=2)
        return

    # ── Phase 2+3: Fetch stock prices + option premiums, simulate ─────
    logger.info("Fetching stock equity prices and option premiums...")
    stock_key_map = tf.get_stock_key_map()
    prepared = []
    skipped = 0

    async with aiohttp.ClientSession() as session:
        for sig in all_signals:
            stock = sig["stock"]
            opt_type = sig["option_type"]
            signal_time = sig["signal_time"]
            if isinstance(signal_time, str):
                signal_time = datetime.strptime(signal_time, "%Y-%m-%d %H:%M:%S")
            trade_date = signal_time.strftime("%Y-%m-%d")

            # Get stock equity price at signal time
            inst_key = stock_key_map.get(stock)
            spot_price = None
            if inst_key:
                eq_candles = await fetch_stock_candles(session, inst_key, trade_date, stock)
                if not eq_candles.empty:
                    at_signal = eq_candles[eq_candles["timestamp"] <= signal_time]
                    if not at_signal.empty:
                        spot_price = float(at_signal.iloc[-1]["close"])

            if spot_price is None:
                logger.warning(f"  {stock}: no equity price, skipping")
                skipped += 1
                continue

            # Find ATM option and fetch premium candles
            expiry = pick_expiry_for_date(trade_date, expiry_dates)
            inst = tf.find_atm_instrument(stock, opt_type, spot_price, expiry)
            if inst is None:
                skipped += 1
                continue

            candles = tf.get_ticks(
                instrument_id=inst["instrument_seq"],
                start=f"{trade_date}T09:15:00",
                end=f"{trade_date}T15:30:00",
            )
            if candles.empty:
                skipped += 1
                continue

            rsi_val = get_premium_rsi_at_signal(candles, signal_time, 14)

            prepared.append({
                "stock": stock,
                "option_type": opt_type,
                "trade_date": trade_date,
                "signal_time": signal_time,
                "spot_price": spot_price,
                "lot_size": inst["lot_size"],
                "strike_price": inst["strike_price"],
                "trading_symbol": inst["trading_symbol"],
                "expiry": expiry,
                "candles": candles,
                "rsi": rsi_val,
                "grade": sig["grade"],
            })

    prepared.sort(key=lambda t: t["signal_time"])
    logger.info(f"Prepared {len(prepared)} trades ({skipped} skipped)")

    # ── Simulate with 10L capital, max 5L/trade, 5-min buffer ─────────
    capital = float(STARTING_CAPITAL)
    rows = []

    trade_queue = []
    for t in prepared:
        if t["rsi"] is not None and t["rsi"] > RSI_OVERBOUGHT:
            continue
        sim = simulate_option(t["candles"], t["signal_time"], ENTRY_OFFSET,
                              SL_PCT, TARGET_PCT, MAX_EXIT_TIME)
        if sim is None:
            continue
        entry_premium = sim["entry_premium"]
        if entry_premium <= 0:
            continue
        cost_per_lot = entry_premium * t["lot_size"]
        if cost_per_lot <= 0:
            continue
        trade_queue.append({"trade": t, "sim": sim, "cost_per_lot": cost_per_lot})

    for _, day_group in groupby(trade_queue, key=lambda x: x["trade"]["trade_date"]):
        day_trades = list(day_group)
        day_capital_pool = capital
        day_locked = 0.0
        day_results = []

        for item in day_trades:
            t = item["trade"]
            sim = item["sim"]
            cost_per_lot = item["cost_per_lot"]
            entry_premium = sim["entry_premium"]
            lot_size = t["lot_size"]

            available = day_capital_pool - day_locked
            if available <= 0:
                continue

            deploy = min(available, MAX_CAPITAL_PER_TRADE)
            num_lots = max(1, math.floor(deploy / cost_per_lot))
            capital_used = num_lots * cost_per_lot
            day_locked += capital_used

            pnl_per_unit = sim["exit_premium"] - entry_premium
            rupee_pnl = pnl_per_unit * lot_size * num_lots
            pnl_pct = (pnl_per_unit / entry_premium) * 100

            opt_label = "CE" if t["option_type"] == "call" else "PE"

            row = {
                "trade_no": len(rows) + len(day_results) + 1,
                "trade_date": t["trade_date"],
                "stock": t["stock"],
                "direction": t["option_type"].upper(),
                "grade": t["grade"],
                "spot_price": round(t["spot_price"], 2),
                "strike_price": int(t["strike_price"]),
                "option_type": opt_label,
                "expiry": t["expiry"],
                "trading_symbol": t["trading_symbol"],
                "lot_size": lot_size,
                "num_lots": num_lots,
                "entry_premium": round(entry_premium, 2),
                "sl_premium": round(sim["sl_premium"], 2),
                "target_premium": round(sim["target_premium"], 2),
                "exit_premium": round(sim["exit_premium"], 2),
                "buy_time": str(sim["entry_time"]),
                "sell_time": str(sim["exit_time"]),
                "holding_candles": sim["holding_candles"],
                "outcome": sim["outcome"],
                "pnl_per_unit": round(pnl_per_unit, 2),
                "pnl_pct": round(pnl_pct, 2),
                "rupee_pnl": round(rupee_pnl, 2),
                "capital_before": round(day_capital_pool, 2),
                "capital_used": round(capital_used, 2),
            }
            day_results.append((row, rupee_pnl))

        for row, rupee_pnl in day_results:
            capital += rupee_pnl
            row["capital_after"] = round(capital, 2)
            rows.append(row)

            tag = "WIN" if row["outcome"] == "target_hit" else ("LOSS" if row["outcome"] == "sl_hit" else "TIME")
            print(
                f"  #{row['trade_no']:>2} {row['trade_date']} | {row['stock']:15s} | "
                f"{row['strike_price']:>8} {row['option_type']} | "
                f"Prem {row['entry_premium']:>7.2f} → {row['exit_premium']:>7.2f} | "
                f"{tag:4s} | Lots {row['num_lots']:>3} x {row['lot_size']} | "
                f"Rs {row['rupee_pnl']:>+12,.0f} | Capital Rs {capital:>12,.0f}"
            )

    # ── Summary ───────────────────────────────────────────────────────
    total_trades = len(rows)
    wins = sum(1 for r in rows if r["outcome"] == "target_hit")
    losses = sum(1 for r in rows if r["outcome"] == "sl_hit")
    time_exits = sum(1 for r in rows if r["outcome"] == "time_exit")
    total_pnl = sum(r["rupee_pnl"] for r in rows)
    final_capital = STARTING_CAPITAL + total_pnl

    print(f"\n{'=' * 90}")
    print(f"  MARCH 2026 | Grade A+B | 10% SL / 30% Target | 10L Capital | Max 5L/trade | 5min buffer")
    print(f"{'=' * 90}")
    print(f"  Trading dates    : {TRADING_DATES}")
    print(f"  Starting Capital : Rs {STARTING_CAPITAL:>12,}")
    print(f"  Final Capital    : Rs {final_capital:>12,.0f}")
    print(f"  Total P&L        : Rs {total_pnl:>+12,.0f}")
    print(f"  ROI              : {(total_pnl / STARTING_CAPITAL) * 100:.2f}%")
    print(f"  Trades           : {total_trades} (W:{wins}  L:{losses}  T:{time_exits})")
    if total_trades:
        print(f"  Win Rate         : {wins / total_trades * 100:.1f}%")
    print(f"{'=' * 90}")

    # ── Save CSV ──────────────────────────────────────────────────────
    csv_path = run_dir / "mar2026_gradeAB_5minbuf_trades.csv"
    if rows:
        fieldnames = list(rows[0].keys())
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nCSV saved to: {csv_path}")

    # ── Save JSON ─────────────────────────────────────────────────────
    def _default(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    summary = {
        "strategy": "Grade A+B | 10% SL / 30% Target | 5min buffer",
        "trading_dates": TRADING_DATES,
        "starting_capital": STARTING_CAPITAL,
        "final_capital": round(final_capital, 2),
        "total_pnl": round(total_pnl, 2),
        "roi_pct": round((total_pnl / STARTING_CAPITAL) * 100, 2),
        "total_trades": total_trades,
        "wins": wins, "losses": losses, "time_exits": time_exits,
        "signals_found": len(all_signals),
        "trades": rows,
    }
    with open(run_dir / "mar2026_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=_default)

    elapsed = time.time() - start
    print(f"Completed in {elapsed:.1f}s")


if __name__ == "__main__":
    asyncio.run(run())
