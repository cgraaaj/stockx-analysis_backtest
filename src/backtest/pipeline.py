"""Backtest pipeline orchestrator.

Coordinates Phases 1 -> 2 -> 3a / 3b, passing data in-memory
when phases run together, or loading from disk for standalone runs.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from src.backtest.config.schema import BacktestConfig, apply_to_globals

logger = logging.getLogger("Pipeline")

RESULTS_BASE = Path(__file__).parent / "results"


@dataclass
class PipelineContext:
    """Shared state flowing between phases."""

    config: BacktestConfig
    run_dir: Path

    all_results: list | None = None
    predictions: Any = None
    market_ctx_log: dict | None = None

    trade_signals: list | None = None
    phase2_trades: list[dict] | None = None

    phase3a_results: list[dict] | None = None
    phase3b_results: list[dict] | None = None

    def load_phase1_outputs(self):
        """Load Phase 1 outputs from disk (for standalone Phase 2 runs)."""
        pred_matches = list(self.run_dir.glob("predictions_*.xlsx"))
        if not pred_matches:
            raise FileNotFoundError(f"No predictions Excel in {self.run_dir}")
        self._predictions_file = pred_matches[0]

    def load_phase2_outputs(self):
        """Load Phase 2 outputs from disk (for standalone Phase 3 runs)."""
        p2 = self.run_dir / "phase2_summary.json"
        if not p2.exists():
            raise FileNotFoundError(f"phase2_summary.json not found in {self.run_dir}")
        with open(p2) as f:
            data = json.load(f)
        self.phase2_trades = data["trades"]

    @property
    def predictions_file(self) -> Path | None:
        return getattr(self, "_predictions_file", None)


def setup_run_dir(config: BacktestConfig, base: Path | None = None) -> Path:
    """Create a timestamped results directory."""
    base = base or RESULTS_BASE
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = base / f"{config.name}_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    with open(run_dir / "backtest_config.json", "w") as f:
        json.dump(config.to_dict(), f, indent=2)

    return run_dir


def _parse_phases(phase_arg: str) -> list[str]:
    """Expand 'all' into ['1', '2', '3a', '3b'], or return individual phase ids."""
    if phase_arg == "all":
        return ["1", "2", "3a", "3b"]
    return [p.strip() for p in phase_arg.split(",")]


async def run_pipeline(
    config: BacktestConfig,
    phases: str = "all",
    results_dir: Path | None = None,
):
    """Run the specified phases of the backtest pipeline.

    Args:
        config: Validated BacktestConfig
        phases: Comma-separated phase list or 'all'
        results_dir: Existing run dir to resume from (for phases 2/3)
    """
    apply_to_globals(config)
    pipeline_start = time.monotonic()

    phase_list = _parse_phases(phases)

    if results_dir and results_dir.exists():
        run_dir = results_dir
        logger.info(f"Resuming from existing results: {run_dir}")
    else:
        run_dir = setup_run_dir(config)
        logger.info(f"New results directory: {run_dir}")

    ctx = PipelineContext(config=config, run_dir=run_dir)

    logger.info(f"Phases to run: {phase_list}")
    logger.info(f"Config: {config.name} — {config.description}")

    if "1" in phase_list:
        from src.backtest.phases.phase1_analysis import run as run_p1

        ctx.all_results, ctx.predictions, ctx.market_ctx_log = await run_p1(
            config, run_dir
        )

    if "2" in phase_list:
        from src.backtest.phases.phase2_trades import run as run_p2

        pred_file = ctx.predictions_file
        if pred_file is None and "1" not in phase_list:
            ctx.load_phase1_outputs()
            pred_file = ctx.predictions_file

        ctx.trade_signals = await run_p2(
            config, run_dir, predictions_file=pred_file
        )

    needs_p2_data = any(p in phase_list for p in ("3a", "3b"))
    if needs_p2_data and ctx.phase2_trades is None and "2" not in phase_list:
        ctx.load_phase2_outputs()

    if "2" in phase_list and ctx.trade_signals:
        p2_summary = run_dir / "phase2_summary.json"
        if p2_summary.exists():
            with open(p2_summary) as f:
                ctx.phase2_trades = json.load(f)["trades"]

    if "3a" in phase_list:
        from src.backtest.phases.phase3_fno import run as run_p3a

        ctx.phase3a_results = await run_p3a(
            config, run_dir, phase2_trades=ctx.phase2_trades
        )

    if "3b" in phase_list:
        from src.backtest.phases.phase3_capital import run as run_p3b

        ctx.phase3b_results = await run_p3b(
            config, run_dir, phase2_trades=ctx.phase2_trades
        )

    from src.backtest.data.tickerflow import close_async_client
    await close_async_client()

    elapsed_s = time.monotonic() - pipeline_start
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Results in: {run_dir}")
    logger.info(
        f"Total pipeline wall time: {elapsed_s / 60:.1f} min ({elapsed_s:.0f} s)"
    )
    logger.info("=" * 60)

    return ctx
