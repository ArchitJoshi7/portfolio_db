"""
Setup script to initialize DB and load sample data for quick testing.
"""
from __future__ import annotations

import datetime as dt

import database as db
import portfolio
import yahoo_data


def run() -> None:
    db.initialize_schema()

    # Create sample portfolios
    try:
        portfolio_id = portfolio.create_portfolio("Demo")
    except Exception:
        pass  # exists

    # Sample transactions
    try:
        portfolio.record_buy("Demo", "AAPL", 10, 150.00, (dt.date.today() - dt.timedelta(days=30)).isoformat())
        portfolio.record_buy("Demo", "MSFT", 5, 300.00, (dt.date.today() - dt.timedelta(days=25)).isoformat())
        portfolio.record_sell("Demo", "AAPL", 3, 160.00, (dt.date.today() - dt.timedelta(days=5)).isoformat())
    except Exception:
        pass

    # Fetch some prices
    for t in ["AAPL", "MSFT"]:
        yahoo_data.fetch_and_store_history(t, start=(dt.date.today() - dt.timedelta(days=90)).isoformat())
        yahoo_data.fetch_and_store_latest(t)

    print("Database and sample data ready at:", db.DB_FILE)


if __name__ == "__main__":
    run()

