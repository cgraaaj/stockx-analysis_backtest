"""
TickerFlow API Client
=====================
Thin httpx wrapper for all TickerFlow market-data endpoints.

Supports both single-item and batch queries.  Retry logic handles
transient 429 / 502 / 503 responses with exponential backoff.

Configuration is read from environment variables:
    TICKERFLOW_URL      - base URL (e.g. https://tickerflow.cgraaaj.in/api/v1)
    TICKERFLOW_API_KEY  - API key for X-API-KEY header
"""

import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("tickerflow_client")

_BASE_URL = os.getenv("TICKERFLOW_URL", "http://localhost:8000/api/v1")
_API_KEY = os.getenv("TICKERFLOW_API_KEY", "")
_TIMEOUT = 60
_MAX_RETRIES = 3
_IST = timezone(timedelta(hours=5, minutes=30))


def _headers() -> dict[str, str]:
    return {"X-API-KEY": _API_KEY}


def _normalize_ts(value: str | datetime | None) -> str | None:
    """Normalise a timestamp for the TickerFlow API.

    The DB stores proper UTC timestamptz.  Market times passed by callers
    are IST, so we append +05:30 when no timezone info is present.
    If the value already carries a timezone suffix (Z, +, -) it is kept
    as-is.
    """
    if value is None:
        return None
    s = str(value)
    if s.endswith("Z") or "+" in s[10:] or "-" in s[19:]:
        return s
    return s.replace(" ", "T") + "+05:30"


def _get(path: str, params: dict | None = None) -> dict[str, Any]:
    """GET with retry on transient errors."""
    url = f"{_BASE_URL}/{path.lstrip('/')}"
    for attempt in range(1, _MAX_RETRIES + 1):
        resp = httpx.get(url, params=params, headers=_headers(), timeout=_TIMEOUT)
        if resp.status_code in (429, 502, 503) and attempt < _MAX_RETRIES:
            wait = 2 ** attempt
            logger.warning(
                "HTTP %d from %s, retrying in %ds (attempt %d/%d)",
                resp.status_code, path, wait, attempt, _MAX_RETRIES,
            )
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return resp.json()


# ------------------------------------------------------------------
# Stocks
# ------------------------------------------------------------------

def get_stocks() -> list[dict]:
    """Fetch all active stocks (id, name, instrument_key, is_active)."""
    data = _get("stocks/")
    return data["results"]


def get_stock_key_map() -> dict[str, str]:
    """Return {stock_name: instrument_key} for all active stocks."""
    stocks = get_stocks()
    return {s["name"]: s["instrument_key"] for s in stocks if s.get("instrument_key")}


# ------------------------------------------------------------------
# Expiries
# ------------------------------------------------------------------

def get_expiries(instrument_type: str | None = None) -> list[str]:
    """Fetch distinct expiry dates as YYYY-MM-DD strings."""
    params: dict[str, Any] = {}
    if instrument_type:
        params["instrument_type"] = instrument_type
    data = _get("expiries/", params)
    return [
        r["expiry"] if isinstance(r["expiry"], str) else r["expiry"].strftime("%Y-%m-%d")
        for r in data["results"]
    ]


# ------------------------------------------------------------------
# Instruments (single + batch)
# ------------------------------------------------------------------

def get_instruments(
    stock_name: str | None = None,
    stock_id: str | None = None,
    instrument_type: str | None = None,
    expiry: str | None = None,
    nearest_strike: float | None = None,
    limit: int = 50,
) -> list[dict]:
    """Fetch instruments with optional filters (single stock)."""
    params: dict[str, Any] = {"limit": limit}
    if stock_name:
        params["stock_name"] = stock_name
    if stock_id:
        params["stock_id"] = stock_id
    if instrument_type:
        params["instrument_type"] = instrument_type
    if expiry:
        params["expiry"] = expiry
    if nearest_strike is not None:
        params["nearest_strike"] = nearest_strike
    data = _get("instruments/", params)
    return data["results"]


def get_instruments_batch(
    stock_ids: list[str] | None = None,
    stock_names: list[str] | None = None,
    instrument_type: str | None = None,
    expiry: str | None = None,
    limit: int = 2000,
) -> pd.DataFrame:
    """Batch-fetch instruments for multiple stocks.

    Returns a DataFrame matching the shape expected by Phase 1.
    """
    params: dict[str, Any] = {"limit": limit}
    if stock_ids:
        params["stock_ids"] = ",".join(stock_ids)
    if stock_names:
        params["stock_names"] = ",".join(stock_names)
    if instrument_type:
        params["instrument_type"] = instrument_type
    if expiry:
        params["expiry"] = expiry
    data = _get("instruments/", params)
    rows = data["results"]
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ------------------------------------------------------------------
# Ticks (single + batch)
# ------------------------------------------------------------------

def get_ticks(
    instrument_id: int,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    limit: int = 10000,
) -> pd.DataFrame:
    """Fetch tick data for a single instrument, returned as a DataFrame."""
    params: dict[str, Any] = {"instrument_id": instrument_id, "limit": limit}
    if start:
        params["start"] = _normalize_ts(start)
    if end:
        params["end"] = _normalize_ts(end)
    data = _get("ticks/", params)
    rows = data.get("results", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["time_stamp"] = pd.to_datetime(df["time_stamp"], utc=True)
    df["time_stamp"] = df["time_stamp"].dt.tz_convert(_IST).dt.tz_localize(None)
    df.sort_values("time_stamp", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def get_ticks_batch(
    instrument_ids: list[int],
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    limit: int = 50000,
) -> pd.DataFrame:
    """Batch-fetch tick data for multiple instruments.

    Returns a DataFrame ordered by (instrument_id, time_stamp), matching
    the shape produced by DatabaseService.get_ticker_data_for_instruments.
    """
    params: dict[str, Any] = {
        "instrument_ids": ",".join(str(i) for i in instrument_ids),
        "limit": limit,
    }
    if start:
        params["start"] = _normalize_ts(start)
    if end:
        params["end"] = _normalize_ts(end)
    data = _get("ticks/", params)
    rows = data.get("results", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["time_stamp"] = pd.to_datetime(df["time_stamp"], utc=True)
    df["time_stamp"] = df["time_stamp"].dt.tz_convert(_IST).dt.tz_localize(None)
    df.sort_values(["instrument_id", "time_stamp"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ------------------------------------------------------------------
# Candles
# ------------------------------------------------------------------

def get_candles(
    instrument_id: int,
    interval: str = "1m",
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Fetch OHLCV candles for a single instrument."""
    params: dict[str, Any] = {
        "instrument_id": instrument_id,
        "interval": interval,
        "limit": limit,
    }
    if start:
        params["start"] = _normalize_ts(start)
    if end:
        params["end"] = _normalize_ts(end)
    data = _get("candles/", params)
    rows = data.get("results", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    ts_col = "bucket" if "bucket" in df.columns else "time_stamp"
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
    df[ts_col] = df[ts_col].dt.tz_convert(_IST).dt.tz_localize(None)
    df.sort_values(ts_col, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ------------------------------------------------------------------
# Convenience: ATM instrument finder (used by Phase 3)
# ------------------------------------------------------------------

def find_atm_instrument(
    stock_name: str,
    option_type: str,
    spot_price: float,
    expiry_date: str,
) -> dict | None:
    """Find the nearest-strike instrument for a stock/option_type/expiry."""
    inst_type = "CE" if option_type == "call" else "PE"
    results = get_instruments(
        stock_name=stock_name,
        instrument_type=inst_type,
        expiry=expiry_date,
        nearest_strike=spot_price,
        limit=1,
    )
    if not results:
        return None
    r = results[0]
    return {
        "instrument_seq": r["instrument_seq"],
        "strike_price": float(r["strike_price"]),
        "lot_size": int(r["lot_size"]),
        "trading_symbol": r["trading_symbol"],
        "instrument_key": r.get("instrument_key"),
    }
