"""
Database Service
===============
Handles all database operations against the shared TimescaleDB instance.

Schema alignment:
  - options.stock        → active stocks with instrument_key
  - options.instrument   → option instruments with instrument_seq (BIGSERIAL)
  - options.ticker_ts    → hypertable keyed by (instrument_id=instrument_seq, time_stamp)

All queries use parameterized statements to prevent SQL injection,
matching the pattern used in tickerflow's market_data/queries.py.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import pandas as pd
from sqlalchemy import create_engine, text
from databases import Database
import logging
from datetime import date as _date, datetime as _dt
from typing import List, Optional
from functools import lru_cache

import db_config
from ..config.settings import database_config, trading_config
from ..models.entities import Stock


class DatabaseService:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._engine = None
        self._database = None
        self._initialize_connections()

    def _initialize_connections(self):
        try:
            self._engine = create_engine(
                db_config.DATABASE_URL,
                pool_size=database_config.POOL_SIZE,
                max_overflow=database_config.MAX_OVERFLOW,
                pool_pre_ping=True,
                pool_recycle=database_config.POOL_RECYCLE,
                echo=database_config.ECHO,
            )
            async_url = db_config.DATABASE_URL.replace(
                "postgresql+psycopg2://", "postgresql://"
            )
            self._database = Database(async_url)
            self.logger.info("Database connections initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize database connections: {e}")
            raise

    async def connect(self):
        try:
            await self._database.connect()
            self.logger.info("Async database connected")
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            raise

    async def disconnect(self):
        try:
            await self._database.disconnect()
            self.logger.info("Async database disconnected")
        except Exception as e:
            self.logger.error(f"Error disconnecting: {e}")

    async def _fetch_all(self, query: str, values: Optional[dict] = None) -> pd.DataFrame:
        """Execute a parameterized query and return a DataFrame."""
        try:
            if values:
                results = await self._database.fetch_all(query=query, values=values)
            else:
                results = await self._database.fetch_all(query=query)

            if results:
                columns = list(results[0]._mapping.keys())
                data = [dict(r._mapping) for r in results]
                df = pd.DataFrame(data, columns=columns)
                for col in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        df[col] = df[col].dt.tz_localize(None)
                return df
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Query error: {e}\nQuery: {query[:120]}...")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Stock queries
    # ------------------------------------------------------------------

    async def get_active_stocks(self) -> List[Stock]:
        """Fetch all active stocks including their instrument_key."""
        query = """
            SELECT id, name, instrument_key, is_active
            FROM options.stock
            WHERE is_active = true
            ORDER BY name
        """
        df = await self._fetch_all(query)
        if df.empty:
            return []

        stocks = []
        for _, row in df.iterrows():
            stocks.append(Stock(
                id=str(row["id"]),
                name=row["name"],
                symbol=row["name"],
                is_active=row["is_active"],
                instrument_key=row.get("instrument_key"),
            ))
        self.logger.info(f"Retrieved {len(stocks)} active stocks")
        return stocks

    # ------------------------------------------------------------------
    # Instrument queries  (returns instrument_seq for ticker_ts joins)
    # ------------------------------------------------------------------

    async def get_instruments_for_stocks(
        self, stock_ids: List[str], expiry_date: str
    ) -> pd.DataFrame:
        """Fetch CE/PE instruments with their instrument_seq for a batch of stocks."""
        if not stock_ids:
            return pd.DataFrame()

        placeholders = ", ".join(f":sid{i}" for i in range(len(stock_ids)))
        values = {f"sid{i}": sid for i, sid in enumerate(stock_ids)}
        values["expiry"] = _dt.strptime(expiry_date, "%Y-%m-%d").date()

        query = f"""
            SELECT id, instrument_seq, stock_id, instrument_type, strike_price,
                   trading_symbol, expiry, instrument_key
            FROM {database_config.INSTRUMENT_TABLE}
            WHERE stock_id::text IN ({placeholders})
              AND expiry = :expiry
              AND instrument_type != 'FUT'
            ORDER BY stock_id, strike_price
        """
        return await self._fetch_all(query, values)

    # ------------------------------------------------------------------
    # Ticker queries  (reads from TimescaleDB hypertable ticker_ts)
    # ------------------------------------------------------------------

    async def get_ticker_data_for_instruments(
        self, instrument_seqs: List[int], trade_date: str
    ) -> pd.DataFrame:
        """Fetch 1-minute OHLCV+OI data from ticker_ts for the given instrument_seq IDs."""
        if not instrument_seqs:
            return pd.DataFrame()

        placeholders = ", ".join(f":seq{i}" for i in range(len(instrument_seqs)))
        values = {f"seq{i}": seq for i, seq in enumerate(instrument_seqs)}
        values["ts_start"] = _dt.strptime(
            f"{trade_date} {trading_config.MARKET_START_TIME}", "%Y-%m-%d %H:%M:%S"
        )
        values["ts_end"] = _dt.strptime(
            f"{trade_date} {trading_config.MARKET_END_TIME}", "%Y-%m-%d %H:%M:%S"
        )

        query = f"""
            SELECT instrument_id, time_stamp, open, high, low, close, volume, open_interest
            FROM {database_config.TICKER_TABLE}
            WHERE instrument_id IN ({placeholders})
              AND time_stamp >= :ts_start
              AND time_stamp <= :ts_end
            ORDER BY instrument_id, time_stamp
        """
        return await self._fetch_all(query, values)

    # ------------------------------------------------------------------
    # Date helpers
    # ------------------------------------------------------------------

    async def get_available_trading_dates(self) -> List[str]:
        query = f"""
            SELECT DISTINCT DATE(time_stamp) as date
            FROM {database_config.TICKER_TABLE}
            ORDER BY date
        """
        df = await self._fetch_all(query)
        if df.empty:
            return []
        return [d.strftime("%Y-%m-%d") for d in df["date"]]

    async def get_available_expiry_dates(self) -> List[str]:
        query = f"""
            SELECT DISTINCT expiry
            FROM {database_config.INSTRUMENT_TABLE}
            WHERE instrument_type != 'FUT'
            ORDER BY expiry
        """
        df = await self._fetch_all(query)
        if df.empty:
            return []
        return [d.strftime("%Y-%m-%d") for d in df["expiry"]]

    # ------------------------------------------------------------------
    # Cached timestamp range (used during CE/PE normalization)
    # ------------------------------------------------------------------

    @lru_cache(maxsize=100)
    def get_trading_timestamps(self, trade_date: str):
        start = pd.Timestamp(f"{trade_date} {trading_config.MARKET_START_TIME}")
        end = pd.Timestamp(f"{trade_date} 15:29:00")
        return pd.date_range(start=start, end=end, freq="1min")

    def get_sync_engine(self):
        """Expose the synchronous SQLAlchemy engine for pandas read_sql etc."""
        return self._engine
