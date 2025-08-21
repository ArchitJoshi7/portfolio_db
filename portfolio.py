"""
Portfolio domain logic: creation, transaction APIs wrapping database layer.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

import database as db


def create_portfolio(name: str) -> int:
    return db.create_portfolio(name)


def add_stock_if_needed(ticker: str, company_name: str = "", sector: Optional[str] = None) -> int:
    return db.get_or_create_stock(ticker, company_name, sector)


def record_buy(portfolio_name: str, ticker: str, quantity: float, price: float, date: Optional[str] = None) -> None:
    pid = db.get_portfolio_id_by_name(portfolio_name)
    if pid is None:
        raise ValueError(f"Portfolio '{portfolio_name}' does not exist")
    sid = db.get_or_create_stock(ticker)
    date = date or dt.date.today().isoformat()
    db.record_transaction(pid, sid, "BUY", quantity, price, date)


def record_sell(portfolio_name: str, ticker: str, quantity: float, price: float, date: Optional[str] = None) -> None:
    pid = db.get_portfolio_id_by_name(portfolio_name)
    if pid is None:
        raise ValueError(f"Portfolio '{portfolio_name}' does not exist")
    sid = db.get_or_create_stock(ticker)
    date = date or dt.date.today().isoformat()
    db.record_transaction(pid, sid, "SELL", quantity, price, date)


def get_holdings(portfolio_name: str):
    pid = db.get_portfolio_id_by_name(portfolio_name)
    if pid is None:
        raise ValueError(f"Portfolio '{portfolio_name}' does not exist")
    return db.list_holdings(pid)


def list_portfolios():
    return db.list_portfolios()

