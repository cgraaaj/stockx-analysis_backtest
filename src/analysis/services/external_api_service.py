"""
External API Service
===================
Handles Upstox historical candle API calls and instrument key mapping.

Key change: instrument_key is now read directly from options.stock
(populated by the data collection pipeline's stock_updater), eliminating
the previous NSE.json file dependency.
"""

import asyncio
import aiohttp
import logging
import urllib.parse
import pandas as pd
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text

from ..config.settings import api_config, processing_config
from ..models.entities import OHLCData, APIRequest


class ExternalAPIService:

    def __init__(self, max_concurrent: int = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.max_concurrent = max_concurrent or processing_config.MAX_CONCURRENT_REQUESTS
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        self.stock_to_instrument_key: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Instrument key mapping (from DB, not NSE.json)
    # ------------------------------------------------------------------

    def load_instrument_key_mapping(self, db_connection_string: str) -> Dict[str, str]:
        """Load stock name → instrument_key mapping from options.stock.

        The data collection pipeline (stock_updater) already populates
        instrument_key on every active stock row from NSE.json, so we
        just read it directly.
        """
        try:
            engine = create_engine(db_connection_string)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("DB connection verified for instrument key mapping")

            query = """
                SELECT name, instrument_key
                FROM options.stock
                WHERE is_active = true AND instrument_key IS NOT NULL
            """
            with engine.connect() as conn:
                df = pd.read_sql(query, conn)

            mapping = dict(zip(df["name"], df["instrument_key"]))
            self.stock_to_instrument_key = mapping
            self.logger.info(f"Loaded instrument keys for {len(mapping)} stocks from DB")
            return mapping
        except Exception as e:
            self.logger.error(f"Error loading instrument key mapping: {e}")
            return {}

    # ------------------------------------------------------------------
    # Upstox API helpers
    # ------------------------------------------------------------------

    def construct_api_url(self, instrument_key: str, date: str) -> str:
        encoded_key = urllib.parse.quote(instrument_key, safe="")
        interval = f"{api_config.DEFAULT_INTERVAL}minute"
        return (
            f"{api_config.UPSTOX_BASE_URL}/{encoded_key}"
            f"/{interval}/{date}/{date}"
        )

    async def fetch_ohlc_with_retries(
        self, session: aiohttp.ClientSession, url: str, stock_info: Dict
    ) -> Optional[pd.DataFrame]:
        async with self.semaphore:
            for attempt in range(processing_config.MAX_RETRIES):
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return self._parse_api_response(data, stock_info)
                        self.logger.warning(f"HTTP {resp.status} for {url}")
                except Exception as e:
                    self.logger.error(f"Attempt {attempt+1} failed for {url}: {e}")
                    if attempt < processing_config.MAX_RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)
            self.logger.error(f"All retries exhausted for {url}")
            return None

    def _parse_api_response(self, api_data: Dict, stock_info: Dict) -> Optional[pd.DataFrame]:
        if api_data.get("status") != "success":
            return None
        candles = api_data.get("data", {}).get("candles", [])
        if not candles:
            return None

        df = pd.DataFrame(candles, columns=[
            "timestamp", "open", "high", "low", "close", "volume", "open_interest",
        ])
        if df.empty:
            return None

        df["stock"] = stock_info["stock"]
        df["date"] = stock_info["date"]
        df["instrument_key"] = stock_info["instrument_key"]
        df["grade"] = stock_info.get("grade", "N/A")
        df["tn_ratio"] = stock_info.get("tn_ratio", "N/A")
        df["sheet_type"] = stock_info["sheet_type"]
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        cols = ["timestamp", "stock", "date", "grade", "tn_ratio",
                "open", "high", "low", "close", "volume", "open_interest",
                "instrument_key", "sheet_type"]
        df = df[[c for c in cols if c in df.columns]]

        self.logger.info(
            f"Fetched {len(df)} candles for {stock_info['stock']} "
            f"on {stock_info['date']} ({stock_info['sheet_type']})"
        )
        return df

    # ------------------------------------------------------------------
    # Batch API fetching
    # ------------------------------------------------------------------

    def prepare_api_requests(self, df: pd.DataFrame, sheet_type: str) -> List[APIRequest]:
        requests: List[APIRequest] = []
        if df.empty:
            return requests

        valid = df.dropna(subset=["instrument_key", "Date"])
        if valid.empty:
            return requests

        combos = valid.groupby(["Date", "Stock", "instrument_key"]).first().reset_index()
        self.logger.info(f"Prepared {len(combos)} {sheet_type} API requests")

        for _, row in combos.iterrows():
            date_val = row["Date"]
            if isinstance(date_val, pd.Timestamp):
                date_str = date_val.strftime("%Y-%m-%d")
            else:
                try:
                    date_str = pd.to_datetime(date_val).strftime("%Y-%m-%d")
                except Exception:
                    continue

            info = {
                "stock": row["Stock"],
                "date": date_str,
                "instrument_key": row["instrument_key"],
                "grade": row.get("Grade", "N/A"),
                "tn_ratio": row.get("TN_Ratio", "N/A"),
                "sheet_type": sheet_type,
            }
            requests.append(APIRequest(
                url=self.construct_api_url(row["instrument_key"], date_str),
                stock_info=info,
            ))
        return requests

    async def fetch_all_ohlc(
        self, requests: List[APIRequest], sheet_type: str
    ) -> List[pd.DataFrame]:
        if not requests:
            return []

        self.logger.info(f"Fetching {sheet_type} OHLC for {len(requests)} requests...")
        dfs: List[pd.DataFrame] = []

        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch_ohlc_with_retries(session, r.url, r.stock_info)
                for r in requests
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            ok = 0
            for r in results:
                if isinstance(r, pd.DataFrame):
                    dfs.append(r)
                    ok += 1
            self.logger.info(f"{sheet_type}: {ok}/{len(requests)} successful")
        return dfs

    # ------------------------------------------------------------------
    # Single-instrument candle fetch (for trade execution backfill)
    # ------------------------------------------------------------------

    async def fetch_candles_for_instrument(
        self,
        instrument_key: str,
        date: str,
        stock_name: str = "",
    ) -> Optional[pd.DataFrame]:
        """Fetch OHLC candles for a single instrument_key on a given date."""
        url = self.construct_api_url(instrument_key, date)
        info = {
            "stock": stock_name,
            "date": date,
            "instrument_key": instrument_key,
            "sheet_type": "single",
        }
        async with aiohttp.ClientSession() as session:
            return await self.fetch_ohlc_with_retries(session, url, info)

    def group_ohlc_by_stock(self, dfs: List[pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Concatenate and group fetched OHLC DataFrames by stock name."""
        if not dfs:
            return {}
        combined = pd.concat(dfs, ignore_index=True)
        return {name: group for name, group in combined.groupby("stock")}

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def get_instrument_key_for_stock(self, stock_name: str) -> Optional[str]:
        return self.stock_to_instrument_key.get(stock_name)

    def add_instrument_keys_to_dataframe(
        self, df: pd.DataFrame, stock_column: str = "Stock"
    ) -> pd.DataFrame:
        if self.stock_to_instrument_key:
            df["instrument_key"] = df[stock_column].map(self.stock_to_instrument_key)
            matched = df["instrument_key"].notna().sum()
            self.logger.info(f"Mapped instrument keys to {matched}/{len(df)} rows")
        else:
            self.logger.warning("No instrument key mapping loaded; adding empty column")
            df["instrument_key"] = None
        return df
