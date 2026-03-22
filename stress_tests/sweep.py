#!/usr/bin/env python3
"""Sweep BACKTEST_DATE_CONCURRENCY (simulated as --parallel-dates) vs TICKERFLOW_API_CONCURRENCY.

Runs stress_tests/benchmark_fetch.py in a subprocess for each (dc, ac) pair so the
TickerFlow client picks up a fresh semaphore for each API concurrency value.

Usage (from repo root):
  python stress_tests/sweep.py
  python stress_tests/sweep.py --quick
  python stress_tests/sweep.py --dc-list 4,6,8 --ac-list 40,64,96

Results: stress_tests/results/sweep_<timestamp>.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BENCH = ROOT / "stress_tests" / "benchmark_fetch.py"
RESULTS_DIR = ROOT / "stress_tests" / "results"


def _parse_json_line(stdout: str) -> dict | None:
    for line in stdout.splitlines():
        if line.startswith("STRESS_RESULT_JSON:"):
            return json.loads(line[len("STRESS_RESULT_JSON:") :])
    return None


def run_one(dc: int, ac: int, stock_limit: int, config: str, dates_offset: int) -> dict:
    env = os.environ.copy()
    env["TICKERFLOW_API_CONCURRENCY"] = str(ac)
    # Ensure child sees project .env for URL/key; ac overrides any file value
    cmd = [
        sys.executable,
        str(BENCH),
        "--parallel-dates",
        str(dc),
        "--stock-limit",
        str(stock_limit),
        "--config",
        config,
        "--dates-offset",
        str(dates_offset),
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=3600,
    )
    out = proc.stdout + "\n" + proc.stderr
    parsed = _parse_json_line(proc.stdout)
    err_msg = None
    if parsed is None:
        err_msg = out[-2000:] if out else "no output"
    return {
        "parallel_dates": dc,
        "api_concurrency": ac,
        "exit_code": proc.returncode,
        "wall_s": parsed.get("wall_s") if parsed else None,
        "tick_rows_total": parsed.get("tick_rows_total") if parsed else None,
        "total_errors": parsed.get("total_errors") if parsed else None,
        "parse_error": err_msg,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--quick",
        action="store_true",
        help="Smaller grid + fewer stocks (faster, less precise)",
    )
    ap.add_argument(
        "--dc-list",
        type=str,
        default="",
        help="Comma-separated parallel date counts e.g. 4,6,8,10",
    )
    ap.add_argument(
        "--ac-list",
        type=str,
        default="",
        help="Comma-separated API concurrency values e.g. 40,64,96",
    )
    ap.add_argument("--stock-limit", type=int, default=0, help="0 = use preset (100 quick / 160 full)")
    ap.add_argument("--config", type=str, default="q1_2026.json")
    ap.add_argument(
        "--dates-offset",
        type=int,
        default=0,
        help="Skip first N config dates so repeated sweeps don't hit identical cache",
    )
    args = ap.parse_args()

    if args.quick:
        dc_list = [4, 6, 8, 10]
        ac_list = [40, 56, 72, 88, 104]
        stock = args.stock_limit or 80
    else:
        dc_list = [4, 6, 8, 10, 12]
        ac_list = [40, 56, 72, 88, 104, 120]
        stock = args.stock_limit or 160

    if args.dc_list.strip():
        dc_list = [int(x.strip()) for x in args.dc_list.split(",") if x.strip()]
    if args.ac_list.strip():
        ac_list = [int(x.strip()) for x in args.ac_list.split(",") if x.strip()]

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_csv = RESULTS_DIR / f"sweep_{ts}.csv"

    rows = []
    print(
        f"Sweep: {len(dc_list)} x {len(ac_list)} = {len(dc_list)*len(ac_list)} runs | "
        f"stock_limit={stock} | config={args.config}",
        flush=True,
    )

    for dc in dc_list:
        for ac in ac_list:
            label = f"dc={dc} ac={ac}"
            print(f"  Running {label} ...", flush=True)
            row = run_one(dc, ac, stock, args.config, args.dates_offset)
            rows.append(row)
            ws = row.get("wall_s")
            te = row.get("total_errors")
            print(f"    -> wall_s={ws} errors={te} exit={row['exit_code']}", flush=True)

    fieldnames = [
        "parallel_dates",
        "api_concurrency",
        "wall_s",
        "tick_rows_total",
        "total_errors",
        "exit_code",
        "parse_error",
    ]
    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k) for k in fieldnames})

    # Recommend: min wall_s among rows with exit_code 0 and total_errors==0
    valid = [
        r
        for r in rows
        if r.get("exit_code") == 0
        and r.get("wall_s") is not None
        and r.get("total_errors") == 0
    ]
    best = None
    if valid:
        best = min(valid, key=lambda r: r["wall_s"])

    print()
    print("=" * 60)
    print(f"CSV written: {out_csv}")
    if best:
        print(
            f"Fastest clean run: parallel_dates={best['parallel_dates']} "
            f"TICKERFLOW_API_CONCURRENCY={best['api_concurrency']} "
            f"wall_s={best['wall_s']}",
        )
        print()
        print("Suggested .env (tune to your comfort; full Phase 1 adds CPU + market-context API):")
        print(f"  BACKTEST_DATE_CONCURRENCY={best['parallel_dates']}")
        print(f"  TICKERFLOW_API_CONCURRENCY={best['api_concurrency']}")
    else:
        print("No error-free run; check CSV and TickerFlow logs. Try lower concurrency.")

    failed = [r for r in rows if r.get("exit_code") != 0 or r.get("total_errors")]
    if failed:
        print(f"\nWarning: {len(failed)} runs had errors or non-zero exit.")

    return 0 if best else 1


if __name__ == "__main__":
    sys.exit(main())
