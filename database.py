"""
Database module for Portfolio Performance Database.

- Manages SQLite connection lifecycle with context managers
- Enforces foreign keys
- Provides transactional helpers with rollback on error
- Initializes schema (5 tables) with constraints and indexes
- Exposes CRUD/query helpers used by higher-level modules

This project uses a single SQLite file database stored alongside the code (portfolio.db).
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

DB_FILE = Path(__file__).with_name("portfolio.db")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    # Enforce foreign key constraints
    conn.execute("PRAGMA foreign_keys = ON;")
    # Reasonable defaults
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    # Row factory for dict-like access
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_connection() -> Iterable[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction() -> Iterable[sqlite3.Connection]:
    """Context manager for a database transaction with rollback on error.

    Usage:
        with transaction() as conn:
            conn.execute(...)
    """
    conn = _connect()
    try:
        conn.execute("BEGIN;")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def initialize_schema() -> None:
    """Create all tables, constraints, and indexes if they do not exist."""
    with get_connection() as conn:
        cursor = conn.cursor()
        # portfolios
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolios (
                portfolio_id   INTEGER PRIMARY KEY,
                name           TEXT NOT NULL UNIQUE,
                created_date   TEXT NOT NULL DEFAULT (date('now'))
            );
            """
        )
        # stocks
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stocks (
                stock_id      INTEGER PRIMARY KEY,
                ticker        TEXT NOT NULL UNIQUE,
                company_name  TEXT NOT NULL,
                sector        TEXT
            );
            """
        )
        # transactions
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id   INTEGER PRIMARY KEY,
                portfolio_id     INTEGER NOT NULL,
                stock_id         INTEGER NOT NULL,
                transaction_type TEXT NOT NULL CHECK (transaction_type IN ('BUY','SELL')),
                quantity         REAL NOT NULL CHECK (quantity > 0),
                price            REAL NOT NULL CHECK (price > 0),
                transaction_date TEXT NOT NULL,
                FOREIGN KEY (portfolio_id) REFERENCES portfolios(portfolio_id) ON DELETE CASCADE,
                FOREIGN KEY (stock_id) REFERENCES stocks(stock_id) ON DELETE RESTRICT
            );
            """
        )
        # prices
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
                price_id     INTEGER PRIMARY KEY,
                stock_id     INTEGER NOT NULL,
                price_date   TEXT NOT NULL,
                close_price  REAL NOT NULL CHECK (close_price > 0),
                UNIQUE(stock_id, price_date),
                FOREIGN KEY (stock_id) REFERENCES stocks(stock_id) ON DELETE CASCADE
            );
            """
        )
        # current_holdings
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS current_holdings (
                holding_id     INTEGER PRIMARY KEY,
                portfolio_id   INTEGER NOT NULL,
                stock_id       INTEGER NOT NULL,
                total_quantity REAL NOT NULL CHECK (total_quantity >= 0),
                average_cost   REAL NOT NULL CHECK (average_cost >= 0),
                last_updated   TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(portfolio_id, stock_id),
                FOREIGN KEY (portfolio_id) REFERENCES portfolios(portfolio_id) ON DELETE CASCADE,
                FOREIGN KEY (stock_id) REFERENCES stocks(stock_id) ON DELETE RESTRICT
            );
            """
        )

        # Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_txn_portfolio_date ON transactions(portfolio_id, transaction_date);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_txn_stock ON transactions(stock_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_stock_date ON prices(stock_id, price_date);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_holdings_portfolio ON current_holdings(portfolio_id);")
        conn.commit()


# Generic helpers

def fetch_all(sql: str, params: Sequence[Any] | None = None) -> List[sqlite3.Row]:
    with get_connection() as conn:
        cur = conn.execute(sql, params or [])
        return list(cur.fetchall())


def fetch_one(sql: str, params: Sequence[Any] | None = None) -> Optional[sqlite3.Row]:
    with get_connection() as conn:
        cur = conn.execute(sql, params or [])
        row = cur.fetchone()
        return row


def execute(sql: str, params: Sequence[Any] | None = None) -> int:
    """Execute a write statement and return lastrowid."""
    with get_connection() as conn:
        cur = conn.execute(sql, params or [])
        conn.commit()
        return int(cur.lastrowid)


def executemany(sql: str, seq_of_params: Iterable[Sequence[Any]]) -> None:
    with get_connection() as conn:
        conn.executemany(sql, seq_of_params)
        conn.commit()


# CRUD specific helpers

def get_or_create_stock(ticker: str, company_name: str = "", sector: str | None = None) -> int:
    row = fetch_one("SELECT stock_id FROM stocks WHERE ticker = ?;", (ticker.upper(),))
    if row:
        return int(row["stock_id"])
    return execute(
        "INSERT INTO stocks (ticker, company_name, sector) VALUES (?,?,?);",
        (ticker.upper(), company_name or ticker.upper(), sector),
    )


def get_portfolio_id_by_name(name: str) -> Optional[int]:
    row = fetch_one("SELECT portfolio_id FROM portfolios WHERE name = ?;", (name,))
    return int(row["portfolio_id"]) if row else None


def create_portfolio(name: str) -> int:
    return execute("INSERT INTO portfolios (name) VALUES (?);", (name,))


def upsert_price(stock_id: int, price_date: str, close_price: float) -> None:
    execute(
        """
        INSERT INTO prices (stock_id, price_date, close_price)
        VALUES (?,?,?)
        ON CONFLICT(stock_id, price_date) DO UPDATE SET close_price=excluded.close_price;
        """,
        (stock_id, price_date, close_price),
    )


def record_transaction(portfolio_id: int, stock_id: int, txn_type: str, quantity: float, price: float, txn_date: str) -> None:
    """Record a buy/sell and update holdings atomically with rollback on errors."""
    txn_type = txn_type.upper()
    if txn_type not in ("BUY", "SELL"):
        raise ValueError("transaction_type must be BUY or SELL")
    if quantity <= 0 or price <= 0:
        raise ValueError("quantity and price must be positive")

    with transaction() as conn:
        # Insert transaction
        conn.execute(
            """
            INSERT INTO transactions (portfolio_id, stock_id, transaction_type, quantity, price, transaction_date)
            VALUES (?,?,?,?,?,?);
            """,
            (portfolio_id, stock_id, txn_type, quantity, price, txn_date),
        )
        # Fetch current holding
        row = conn.execute(
            "SELECT holding_id, total_quantity, average_cost FROM current_holdings WHERE portfolio_id = ? AND stock_id = ?;",
            (portfolio_id, stock_id),
        ).fetchone()

        if txn_type == "BUY":
            if row:
                total_qty = row["total_quantity"] + quantity
                # Weighted average cost
                new_avg_cost = (
                    (row["total_quantity"] * row["average_cost"]) + (quantity * price)
                ) / total_qty
                conn.execute(
                    """
                    UPDATE current_holdings
                    SET total_quantity = ?, average_cost = ?, last_updated = datetime('now')
                    WHERE holding_id = ?;
                    """,
                    (total_qty, new_avg_cost, row["holding_id"]),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO current_holdings (portfolio_id, stock_id, total_quantity, average_cost)
                    VALUES (?,?,?,?);
                    """,
                    (portfolio_id, stock_id, quantity, price),
                )
        else:  # SELL
            if not row or row["total_quantity"] < quantity:
                raise ValueError("Cannot sell more shares than currently held")
            remaining = row["total_quantity"] - quantity
            if remaining == 0:
                conn.execute(
                    "DELETE FROM current_holdings WHERE holding_id = ?;",
                    (row["holding_id"],),
                )
            else:
                conn.execute(
                    """
                    UPDATE current_holdings
                    SET total_quantity = ?, last_updated = datetime('now')
                    WHERE holding_id = ?;
                    """,
                    (remaining, row["holding_id"]),
                )


def latest_price_for_stock(stock_id: int) -> Optional[sqlite3.Row]:
    return fetch_one(
        """
        SELECT p.*
        FROM prices p
        WHERE p.stock_id = ?
        ORDER BY p.price_date DESC
        LIMIT 1;
        """,
        (stock_id,),
    )


def list_portfolios() -> List[sqlite3.Row]:
    return fetch_all("SELECT portfolio_id, name, created_date FROM portfolios ORDER BY name;")


def list_holdings(portfolio_id: int) -> List[sqlite3.Row]:
    return fetch_all(
        """
        SELECT h.portfolio_id, s.ticker, s.company_name, h.total_quantity, h.average_cost,
               COALESCE(lp.close_price, NULL) AS last_close,
               h.last_updated
        FROM current_holdings h
        JOIN stocks s ON s.stock_id = h.stock_id
        LEFT JOIN (
            SELECT stock_id, close_price, price_date
            FROM prices
            WHERE (stock_id, price_date) IN (
                SELECT stock_id, MAX(price_date) FROM prices GROUP BY stock_id
            )
        ) AS lp ON lp.stock_id = s.stock_id
        WHERE h.portfolio_id = ?
        ORDER BY s.ticker;
        """,
        (portfolio_id,),
    )

