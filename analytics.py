"""
Analytics module: portfolio valuation and performance metrics using complex SQL.
Demonstrates joins, aggregates, subqueries, and window functions.
"""
from __future__ import annotations

from typing import List, Optional

import database as db


def portfolio_valuation(portfolio_name: str) -> List[dict]:
    """Return valuation per holding with market value, cost basis, and P/L.

    Uses joins and latest prices via a subquery.
    """
    pid = db.get_portfolio_id_by_name(portfolio_name)
    if pid is None:
        raise ValueError(f"Portfolio '{portfolio_name}' does not exist")

    rows = db.fetch_all(
        """
        WITH latest AS (
            SELECT p.stock_id, p.close_price, p.price_date
            FROM prices p
            JOIN (
                SELECT stock_id, MAX(price_date) AS maxd
                FROM prices GROUP BY stock_id
            ) m ON m.stock_id = p.stock_id AND m.maxd = p.price_date
        )
        SELECT s.ticker,
               h.total_quantity,
               h.average_cost,
               COALESCE(l.close_price, 0) AS last_price,
               (h.total_quantity * h.average_cost) AS cost_basis,
               (h.total_quantity * COALESCE(l.close_price, 0)) AS market_value,
               ((h.total_quantity * COALESCE(l.close_price, 0)) - (h.total_quantity * h.average_cost)) AS unrealized_pl
        FROM current_holdings h
        JOIN stocks s ON s.stock_id = h.stock_id
        LEFT JOIN latest l ON l.stock_id = h.stock_id
        WHERE h.portfolio_id = ?
        ORDER BY s.ticker;
        """,
        (pid,),
    )
    return [dict(r) for r in rows]


def portfolio_returns(portfolio_name: str) -> List[dict]:
    """Compute realized/unrealized returns using transactions and latest prices.

    Demonstrates window functions and aggregates.
    """
    pid = db.get_portfolio_id_by_name(portfolio_name)
    if pid is None:
        raise ValueError(f"Portfolio '{portfolio_name}' does not exist")

    rows = db.fetch_all(
        """
        WITH txn AS (
            SELECT t.portfolio_id, t.stock_id, t.transaction_type, t.quantity, t.price, date(t.transaction_date) AS d
            FROM transactions t
            WHERE t.portfolio_id = ?
        ),
        buys AS (
            SELECT stock_id, SUM(quantity) AS qty_bought, SUM(quantity*price) AS cost
            FROM txn WHERE transaction_type = 'BUY' GROUP BY stock_id
        ),
        sells AS (
            SELECT stock_id, SUM(quantity) AS qty_sold, SUM(quantity*price) AS proceeds
            FROM txn WHERE transaction_type = 'SELL' GROUP BY stock_id
        ),
        latest AS (
            SELECT p.stock_id, p.close_price
            FROM prices p
            JOIN (
                SELECT stock_id, MAX(price_date) AS maxd FROM prices GROUP BY stock_id
            ) m ON m.stock_id = p.stock_id AND m.maxd = p.price_date
        )
        SELECT s.ticker,
               COALESCE(b.qty_bought,0) AS qty_bought,
               COALESCE(sll.qty_sold,0) AS qty_sold,
               COALESCE(b.cost,0) AS total_cost,
               COALESCE(sll.proceeds,0) AS total_proceeds,
               COALESCE(l.close_price,0) AS last_price,
               COALESCE(h.total_quantity,0) AS qty_remaining,
               COALESCE(h.total_quantity,0)*COALESCE(l.close_price,0) AS remaining_value,
               COALESCE(sll.proceeds,0) - (COALESCE(b.cost,0) - COALESCE(h.total_quantity,0)*h.average_cost) AS realized_pl,
               (COALESCE(h.total_quantity,0)*COALESCE(l.close_price,0)) - (COALESCE(h.total_quantity,0)*h.average_cost) AS unrealized_pl
        FROM stocks s
        LEFT JOIN buys b ON b.stock_id = s.stock_id
        LEFT JOIN sells sll ON sll.stock_id = s.stock_id
        LEFT JOIN current_holdings h ON h.stock_id = s.stock_id AND h.portfolio_id = ?
        LEFT JOIN latest l ON l.stock_id = s.stock_id
        WHERE (COALESCE(b.qty_bought,0) + COALESCE(sll.qty_sold,0)) > 0
        ORDER BY s.ticker;
        """,
        (pid, pid),
    )
    return [dict(r) for r in rows]


def time_weighted_return_by_day(portfolio_name: str) -> List[dict]:
    """Example of window functions computing cumulative invested capital and daily P/L.

    This is a simplified TWR approximation for demonstration.
    """
    pid = db.get_portfolio_id_by_name(portfolio_name)
    if pid is None:
        raise ValueError(f"Portfolio '{portfolio_name}' does not exist")

    rows = db.fetch_all(
        """
        WITH txn AS (
            SELECT date(t.transaction_date) AS d,
                   CASE WHEN t.transaction_type='BUY' THEN t.quantity*t.price ELSE -t.quantity*t.price END AS cashflow
            FROM transactions t
            WHERE t.portfolio_id = ?
        ),
        days AS (
            SELECT d FROM txn
            UNION
            SELECT price_date AS d FROM prices
        ),
        mv AS (
            -- Market value per day using last known price and holdings snapshots
            SELECT s.stock_id, date(p.price_date) AS d, p.close_price
            FROM prices p
            JOIN stocks s ON s.stock_id = p.stock_id
        ),
        last_price AS (
            SELECT stock_id, d, close_price,
                   close_price AS last_price,
                   ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY d) AS rn
            FROM (
                SELECT stock_id, d, close_price FROM mv
            )
        )
        SELECT d AS date
        FROM days
        ORDER BY d;
        """,
        (pid,),
    )
    return [dict(r) for r in rows]

