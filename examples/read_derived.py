"""Read any published derived table with the existing client configuration."""
import argparse

from market_data_api import MarketDataClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', default='daily_quality')
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date', required=True)
    parser.add_argument('--symbols', nargs='+')
    parser.add_argument('--columns', nargs='+')
    args = parser.parse_args()
    with MarketDataClient.connect() as client:
        print(client.tables(args.table))
        rows = 0
        for batch in client.iter_derived(
            args.table, args.start_date, args.end_date,
            symbols=args.symbols, columns=args.columns,
        ):
            if rows == 0:
                print(batch.slice(0, 5).to_pydict())
            rows += batch.num_rows
        print({'rows': rows, 'stats': client.last_read_stats})


if __name__ == '__main__':
    main()
