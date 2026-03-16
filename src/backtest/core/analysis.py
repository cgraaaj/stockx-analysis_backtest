"""Shared analysis helpers used by Phase 1."""

from datetime import datetime
from functools import lru_cache

import pandas as pd

from src.analysis.config.settings import trading_config
from src.analysis.models.entities import CEPEPair


@lru_cache(maxsize=100)
def get_trading_timestamps(trade_date: str):
    start = pd.Timestamp(f"{trade_date} {trading_config.MARKET_START_TIME}")
    end = pd.Timestamp(f"{trade_date} 15:29:00")
    return pd.date_range(start=start, end=end, freq="1min")


def pick_expiry(trade_date: str, expiry_dates: list) -> str:
    """Pick the nearest monthly expiry >= trade_date, same month preferred."""
    trade_dt = datetime.strptime(trade_date, "%Y-%m-%d")
    ym = trade_dt.strftime("%Y-%m")
    for exp in expiry_dates:
        exp_dt = datetime.strptime(exp, "%Y-%m-%d")
        if exp_dt.strftime("%Y-%m") == ym and exp_dt >= trade_dt:
            return exp
    for exp in expiry_dates:
        if datetime.strptime(exp, "%Y-%m-%d") >= trade_dt:
            return exp
    return expiry_dates[-1] if expiry_dates else trade_date


def analyze_stock(stock, instrument_df, ticker_df, trade_date, analysis_svc):
    """Run OI analysis for a single stock on a single date.

    Returns {"name": str, "opt_data": list} or None.
    """
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
    for row in pairs_df.itertuples(index=False):
        pair = CEPEPair(
            ce_seq=int(row.ce_seq) if pd.notna(row.ce_seq) else None,
            pe_seq=int(row.pe_seq) if pd.notna(row.pe_seq) else None,
            strike_price=row.strike_price,
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
