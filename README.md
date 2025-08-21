# Portfolio Performance Database (SQLite)

A minimal, professional portfolio management system showcasing DBMS design, SQLite schema with constraints and indexes, transaction management with rollback, analytics using complex SQL, Yahoo Finance data integration, reporting, and a user-friendly CLI.

Flat structure with 8 core files:
- database.py
- yahoo_data.py
- portfolio.py
- analytics.py
- reports.py
- cli.py
- setup_db.py
- requirements.txt

The SQLite database file (portfolio.db) will be created alongside the code after initialization.

## Quick start (Windows PowerShell)

1) Navigate to the project directory:
- C:\Users\arc_j\Documents\projects\portfolio_db

2) Install dependencies:
- python -m pip install -r requirements.txt

3) Initialize the database schema:
- python -c "import database as db; db.initialize_schema(); print(db.DB_FILE)"
  or
- python cli.py init-db

4) Load sample data (creates Demo portfolio, sample transactions, and fetches prices):
- python setup_db.py

5) Use the CLI:
- Create portfolio:            python cli.py create-portfolio "MyPort"
- Record BUY:                  python cli.py buy "MyPort" AAPL 10 150 --date 2025-07-01
- Record SELL:                 python cli.py sell "MyPort" AAPL 3 160 --date 2025-07-20
- Update prices (history+latest): python cli.py update-prices AAPL --history --start 2024-01-01
- List portfolios:             python cli.py list-portfolios
- View holdings:               python cli.py holdings "MyPort"
- Valuation report:            python cli.py valuation "MyPort"
- Returns report:              python cli.py returns "MyPort"
- Export valuation to CSV:     python cli.py export "MyPort" --output exports

## Highlights

- Strong schema design: 5 tables with PRIMARY KEYs, UNIQUEs, FOREIGN KEYs, CHECK constraints, and useful indexes.
- Transaction-safe operations with rollback for BUY/SELL and holdings updates.
- Yahoo Finance integration via yfinance for historical and latest prices.
- Analytics using complex SQL: joins, aggregates, subqueries, and example window function usage.
- Reporting to console (tabulate) and CSV export.
- Clear separation of concerns across modules.

## Module overview

- database.py: Connection management, PRAGMAs, schema initialization, helpers, and atomic transaction logic for recording transactions and updating holdings.
- yahoo_data.py: Fetch historical and latest stock prices, storing close prices into the DB.
- portfolio.py: Domain-level helpers to create portfolios and record trades by name/ticker.
- analytics.py: Portfolio valuation and return summaries using SQL CTEs and joins.
- reports.py: Console table printing and CSV export helpers.
- cli.py: User-friendly CLI with subcommands.
- setup_db.py: Sample data loader to quickly test the system.

## Notes

- This is a teaching/demo project emphasizing DBMS concepts and SQLite features.
- Network calls to Yahoo Finance can fail; these are caught and won’t break the flow.
- For reproducibility and simplicity, only the daily close price is stored.

