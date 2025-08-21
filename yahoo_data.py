"""
Yahoo Finance data integration using yfinance.
Provides convenience functions to fetch historical and latest prices and persist them into SQLite.
"""
from __future__ import annotations
import datetime as dt
import time
from typing import Iterable, List, Optional

import yfinance as yf

import database as db


def _download_with_retry(*args, retries: int = 3, delay: float = 1.0, **kwargs):
    """Call yf.download with simple retries to mitigate transient failures."""
    last_exc = None
    for _ in range(retries):
        try:
            return yf.download(*args, progress=False, threads=False, **kwargs)
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    # If yfinance threw, try a final attempt that returns empty DataFrame instead of throwing
    try:
        return yf.download(*args, progress=False, threads=False, **kwargs)
    except Exception:
        # Return None to signal failure
        return None


def fetch_and_store_history(ticker: str, start: str = "2019-01-01", end: Optional[str] = None, interval: str = "1d") -> List[tuple]:
    """Fetch OHLCV history from Yahoo and store close prices in DB.

    Returns list of (date, close) stored.
    """
    t = ticker.upper()
    end = end or dt.date.today().isoformat()
    # Use retry wrapper; if date-ranged query fails, fall back to a period-based query
    hist = _download_with_retry(t, start=start, end=end, interval=interval)
    if hist is None or getattr(hist, "empty", True):
        hist = _download_with_retry(t, period="1y", interval=interval)
    stored: List[tuple] = []
    if hist is None or getattr(hist, "empty", True):
        return stored

    stock_id = db.get_or_create_stock(t)
    for idx, row in hist.iterrows():
        if hasattr(idx, "to_pydatetime"):
            d = idx.to_pydatetime().date().isoformat()
        else:
            d = str(idx)[:10]
        close = float(row["Close"]) if "Close" in row else float(row.get("Adj Close", 0) or 0)
        if close > 0:
            db.upsert_price(stock_id, d, close)
            stored.append((d, close))
    return stored


def fetch_and_store_latest(ticker: str) -> Optional[tuple]:
    """Fetch the latest close/price and store it.

    Uses yfinance fast info or recent history.
    Returns (date, price) if stored.
    """
    t = ticker.upper()
    stock_id = db.get_or_create_stock(t)

    try:
        ticker_obj = yf.Ticker(t)
        price = None
        date_str = dt.date.today().isoformat()
        finfo = getattr(ticker_obj, "fast_info", None)
        if finfo and getattr(finfo, "last_price", None):
            price = float(finfo.last_price)
        else:
            hist = None
            # First try short period
            try:
                hist = ticker_obj.history(period="5d", interval="1d")
            except Exception:
                hist = None
            if hist is None or getattr(hist, "empty", True):
                # Fallback via download with retries
                hist = _download_with_retry(t, period="5d", interval="1d")
            if hist is not None and not getattr(hist, "empty", True):
                last = hist.tail(1)
                price = float(last.get("Close", last.get("Adj Close")).iloc[0])
                idx = last.index[-1]
                if hasattr(idx, "to_pydatetime"):
                    date_str = idx.to_pydatetime().date().isoformat()
        if price and price > 0:
            db.upsert_price(stock_id, date_str, price)
            return (date_str, price)
    except Exception:
        # Swallow network errors; caller can decide how to report
        return None
    return None
