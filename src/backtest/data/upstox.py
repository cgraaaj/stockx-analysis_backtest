"""Upstox historical candle fetcher and OHLC resampler."""

import asyncio
import logging
import urllib.parse

import aiohttp
import pandas as pd

logger = logging.getLogger("upstox")

UPSTOX_BASE = "https://api.upstox.com/v2/historical-candle"


def resample_to_5min(df_1m: pd.DataFrame) -> pd.DataFrame:
    """Resample 1-minute candles to 5-minute OHLCV."""
    df = df_1m.set_index("timestamp")
    stock_name = df["stock"].iloc[0] if "stock" in df.columns else ""

    ohlc = df.resample("5min", label="left", closed="left").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna()

    ohlc.reset_index(inplace=True)
    ohlc["stock"] = stock_name
    return ohlc


async def fetch_stock_candles_raw(
    session: aiohttp.ClientSession,
    instrument_key: str,
    date: str,
    stock_name: str,
) -> pd.DataFrame:
    """Fetch raw 1-minute candles from Upstox public API."""
    encoded = urllib.parse.quote(instrument_key, safe="")
    url = f"{UPSTOX_BASE}/{encoded}/1minute/{date}/{date}"

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
                            df["stock"] = stock_name
                            df.sort_values("timestamp", inplace=True)
                            df.reset_index(drop=True, inplace=True)
                            return df
                logger.warning(f"HTTP {resp.status} for {stock_name} on {date}")
        except Exception as e:
            logger.error(f"Attempt {attempt+1} for {stock_name}: {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    return pd.DataFrame()


_UPSTOX_SEMAPHORE = asyncio.Semaphore(25)


async def _fetch_single(
    session: aiohttp.ClientSession,
    ikey: str,
    date: str,
    stock: str,
) -> tuple[str, dict | None]:
    """Fetch + resample one (stock, date) pair with concurrency throttle."""
    async with _UPSTOX_SEMAPHORE:
        raw_1m = await fetch_stock_candles_raw(session, ikey, date, stock)
    if raw_1m.empty:
        logger.warning(f"  {stock} ({date}): no candles")
        return f"{stock}_{date}", None
    resampled_5m = resample_to_5min(raw_1m)
    logger.info(f"  {stock} ({date}): {len(raw_1m)} 1m -> {len(resampled_5m)} 5m candles")
    return f"{stock}_{date}", {"raw_1m": raw_1m, "ohlc_5m": resampled_5m}


async def fetch_all_candles(
    stock_date_pairs: set[tuple[str, str]],
    key_map: dict[str, str],
) -> dict[str, dict]:
    """Fetch 1-min candles concurrently and return both raw and 5-min resampled.

    Uses asyncio.gather with a semaphore (10 concurrent) to stay within
    Upstox rate limits while dramatically cutting wall-clock time.

    Args:
        stock_date_pairs: set of (stock_name, date_str) tuples
        key_map: {stock_name: instrument_key}

    Returns:
        dict keyed by "{stock}_{date}" with {"raw_1m": df, "ohlc_5m": df}
    """
    result: dict[str, dict] = {}

    async with aiohttp.ClientSession() as session:
        tasks = []
        for stock, date in stock_date_pairs:
            ikey = key_map.get(stock)
            if not ikey:
                logger.warning(f"No instrument_key for {stock}")
                continue
            tasks.append(_fetch_single(session, ikey, date, stock))

        completed = await asyncio.gather(*tasks, return_exceptions=True)
        for item in completed:
            if isinstance(item, Exception):
                logger.error(f"  Candle fetch error: {item}")
                continue
            key, data = item
            if data is not None:
                result[key] = data

    return result
