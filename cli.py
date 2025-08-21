"""
CLI for Portfolio Performance Database
Provides options to initialize DB, create portfolios, record transactions, fetch prices, view summaries, reports, and export CSVs.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import analytics
import database as db
import portfolio
import yahoo_data
import reports


def ensure_db() -> None:
    db.initialize_schema()


def cmd_init_db(args: argparse.Namespace) -> None:
    ensure_db()
    print("Database initialized at:", db.DB_FILE)


def cmd_create_portfolio(args: argparse.Namespace) -> None:
    ensure_db()
    pid = portfolio.create_portfolio(args.name)
    print(f"Created portfolio '{args.name}' (id={pid})")


def cmd_buy(args: argparse.Namespace) -> None:
    ensure_db()
    portfolio.record_buy(args.portfolio, args.ticker, args.quantity, args.price, args.date)
    print("BUY recorded.")


def cmd_sell(args: argparse.Namespace) -> None:
    ensure_db()
    portfolio.record_sell(args.portfolio, args.ticker, args.quantity, args.price, args.date)
    print("SELL recorded.")


def cmd_update_prices(args: argparse.Namespace) -> None:
    ensure_db()
    if args.history:
        stored = yahoo_data.fetch_and_store_history(args.ticker, start=args.start, end=args.end)
        print(f"Stored {len(stored)} price rows for {args.ticker}")
    latest = yahoo_data.fetch_and_store_latest(args.ticker)
    if latest:
        print(f"Latest: {args.ticker} {latest[0]} close={latest[1]:.2f}")


def cmd_list_portfolios(args: argparse.Namespace) -> None:
    ensure_db()
    rows = [dict(r) for r in portfolio.list_portfolios()]
    reports.print_table("Portfolios", rows)


def cmd_holdings(args: argparse.Namespace) -> None:
    ensure_db()
    rows = [dict(r) for r in portfolio.get_holdings(args.portfolio)]
    reports.print_table(f"Holdings - {args.portfolio}", rows)


def cmd_valuation(args: argparse.Namespace) -> None:
    ensure_db()
    rows = analytics.portfolio_valuation(args.portfolio)
    reports.print_table(f"Valuation - {args.portfolio}", rows)


def cmd_returns(args: argparse.Namespace) -> None:
    ensure_db()
    rows = analytics.portfolio_returns(args.portfolio)
    reports.print_table(f"Returns - {args.portfolio}", rows)


def cmd_export(args: argparse.Namespace) -> None:
    ensure_db()
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = analytics.portfolio_valuation(args.portfolio)
    path = reports.export_csv(out_dir / f"valuation_{args.portfolio}.csv", rows)
    print("Exported:", path)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="portfolio_db", description="Portfolio Performance Database CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("init-db", help="Initialize database schema")
    sp.set_defaults(func=cmd_init_db)

    sp = sub.add_parser("create-portfolio", help="Create a new portfolio")
    sp.add_argument("name")
    sp.set_defaults(func=cmd_create_portfolio)

    sp = sub.add_parser("buy", help="Record a BUY transaction")
    sp.add_argument("portfolio")
    sp.add_argument("ticker")
    sp.add_argument("quantity", type=float)
    sp.add_argument("price", type=float)
    sp.add_argument("--date", default=None, help="YYYY-MM-DD")
    sp.set_defaults(func=cmd_buy)

    sp = sub.add_parser("sell", help="Record a SELL transaction")
    sp.add_argument("portfolio")
    sp.add_argument("ticker")
    sp.add_argument("quantity", type=float)
    sp.add_argument("price", type=float)
    sp.add_argument("--date", default=None, help="YYYY-MM-DD")
    sp.set_defaults(func=cmd_sell)

    sp = sub.add_parser("update-prices", help="Fetch and store Yahoo prices")
    sp.add_argument("ticker")
    sp.add_argument("--history", action="store_true", help="Fetch history starting at --start")
    sp.add_argument("--start", default="2019-01-01")
    sp.add_argument("--end", default=None)
    sp.set_defaults(func=cmd_update_prices)

    sp = sub.add_parser("list-portfolios", help="List all portfolios")
    sp.set_defaults(func=cmd_list_portfolios)

    sp = sub.add_parser("holdings", help="View holdings for a portfolio")
    sp.add_argument("portfolio")
    sp.set_defaults(func=cmd_holdings)

    sp = sub.add_parser("valuation", help="Portfolio valuation summary")
    sp.add_argument("portfolio")
    sp.set_defaults(func=cmd_valuation)

    sp = sub.add_parser("returns", help="Portfolio returns summary")
    sp.add_argument("portfolio")
    sp.set_defaults(func=cmd_returns)

    sp = sub.add_parser("export", help="Export valuation to CSV")
    sp.add_argument("portfolio")
    sp.add_argument("--output", default="exports")
    sp.set_defaults(func=cmd_export)

    return p


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    main()

