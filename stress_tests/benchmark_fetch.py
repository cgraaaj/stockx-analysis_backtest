#!/usr/bin/env python3
"""Phase-1-style fetch load test (instruments + ticks only, no CPU analysis).

Reads TICKERFLOW_API_CONCURRENCY from the environment (set by sweep.py before import).

Run from repo root:
  TICKERFLOW_API_CONCURRENCY=64 python stress_tests/benchmark_fetch.py --parallel-dates 8
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Load .env before tickerflow import
from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

# Env must be set before this import (TICKERFLOW_API_CONCURRENCY used at import time)
from src.analysis.config.settings import trading_config
from src.analysis.models.entities import Stock
from src.backtest.config.schema import load_config
from src.backtest.core.analysis import pick_expiry
from src.backtest.data import tickerflow as tf

logging.basicConfig(level=logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("stress_benchmark")


async def _fetch_batch_ticks(
    seqs: list[int], trade_date: str, tick_sub_size: int = 20,
):
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
    return await asyncio.gather(*tasks, return_exceptions=True)


async def _one_date(
    trade_date: str,
    stocks: list[Stock],
    expiry_dates: list[str],
    batch_size: int,
    date_sem: asyncio.Semaphore,
) -> dict:
    errors = 0
    tick_rows = 0
    t0 = time.perf_counter()
    async with date_sem:
        expiry = pick_expiry(trade_date, expiry_dates)
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
        for idx, (inst_res, (_, _)) in enumerate(zip(inst_results, batch_groups)):
            if isinstance(inst_res, Exception):
                errors += 1
                continue
            if inst_res.empty:
                continue
            seqs = inst_res["instrument_seq"].dropna().astype(int).tolist()
            if not seqs:
                continue
            tick_tasks.append(_fetch_batch_ticks(seqs, trade_date))
            tick_task_indices.append(idx)

        tick_results = await asyncio.gather(*tick_tasks, return_exceptions=True)

        for task_pos, batch_idx in enumerate(tick_task_indices):
            res = tick_results[task_pos]
            if isinstance(res, Exception):
                errors += 1
                continue
            for frame in res:
                if isinstance(frame, Exception):
                    errors += 1
                elif hasattr(frame, "__len__"):
                    tick_rows += len(frame)

    elapsed = time.perf_counter() - t0
    return {
        "trade_date": trade_date,
        "elapsed_s": round(elapsed, 3),
        "errors": errors,
        "tick_rows": tick_rows,
    }


async def run(
    *,
    parallel_dates: int,
    stock_limit: int | None,
    config_name: str,
    dates_offset: int,
) -> dict:
    cfg = load_config(config_name)
    dates = cfg.trading_dates
    if dates_offset:
        dates = dates[dates_offset:]
    wave = dates[: max(1, parallel_dates)]
    if not wave:
        raise SystemExit("No trading dates after offset")

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
    if stock_limit and stock_limit > 0:
        stocks = stocks[:stock_limit]

    expiry_dates = await tf.aget_expiries()
    batch_size = cfg.batch_size

    date_sem = asyncio.Semaphore(parallel_dates)
    t_wall = time.perf_counter()
    parts = await asyncio.gather(
        *[
            _one_date(d, stocks, expiry_dates, batch_size, date_sem)
            for d in wave
        ],
        return_exceptions=True,
    )
    wall_s = time.perf_counter() - t_wall

    total_errors = 0
    total_ticks = 0
    ok_parts = []
    for p in parts:
        if isinstance(p, Exception):
            total_errors += 1
        else:
            total_errors += p["errors"]
            total_ticks += p["tick_rows"]
            ok_parts.append(p)

    try:
        await tf.close_async_client()
    except Exception:
        pass

    return {
        "parallel_dates": parallel_dates,
        "tickerflow_api_concurrency": tf.get_api_concurrency_limit(),
        "stock_count": len(stocks),
        "batch_size": batch_size,
        "dates_used": wave,
        "wall_s": round(wall_s, 3),
        "total_errors": total_errors,
        "tick_rows_total": total_ticks,
        "per_date": ok_parts if len(ok_parts) == len(wave) else str(parts),
    }


def main():
    ap = argparse.ArgumentParser(description="Fetch-only Phase 1 load benchmark")
    ap.add_argument("--parallel-dates", type=int, default=6, help="Concurrent trading dates (like BACKTEST_DATE_CONCURRENCY)")
    ap.add_argument("--stock-limit", type=int, default=100, help="Only first N stocks (0 = all). Smaller = faster sweep.")
    ap.add_argument("--config", type=str, default="q1_2026.json")
    ap.add_argument("--dates-offset", type=int, default=0, help="Skip first N dates from config (spread load across year)")
    args = ap.parse_args()

    lim = None if args.stock_limit == 0 else args.stock_limit
    result = asyncio.run(
        run(
            parallel_dates=args.parallel_dates,
            stock_limit=lim,
            config_name=args.config,
            dates_offset=args.dates_offset,
        )
    )
    print("STRESS_RESULT_JSON:" + json.dumps(result))
    if result["total_errors"] > 0:
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
