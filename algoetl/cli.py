"""algoetl command line.

  python -m algoetl.cli export --start 50000000 --end 50000100 --out ./data
  python -m algoetl.cli follow --out s3://my-bucket/algorand --start 50000000
"""
from __future__ import annotations

import argparse
import os

from .client import DEFAULT_INDEXER, IndexerClient
from .compact import compact_all
from .pg import PostgresSink
from .pipeline import export_range, follow_forever
from .sink import Store


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(prog="algoetl")
    ap.add_argument("--indexer", default=os.environ.get("ALGOETL_INDEXER", DEFAULT_INDEXER))
    ap.add_argument("--token", default=os.environ.get("ALGOETL_TOKEN"))
    ap.add_argument("--rps", type=float, default=4.0)
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export")
    e.add_argument("--start", type=int, required=True)
    e.add_argument("--end", type=int, required=True)
    e.add_argument("--out", required=True)
    e.add_argument("--batch", type=int, default=200)
    f = sub.add_parser("follow")
    f.add_argument("--out", required=True)
    f.add_argument("--start", type=int)
    f.add_argument("--interval", type=float, default=30.0)
    for p in (e, f):
        p.add_argument("--pg-dsn", default=os.environ.get("ALGOETL_PG_DSN"),
                       help="also load into Postgres (needs algoetl[postgres])")
    c = sub.add_parser("compact", help="merge each day's small Parquet files into one per table")
    c.add_argument("--out", required=True)
    c.add_argument("--date", help="only this UTC day (YYYY-MM-DD); default: every day")
    a = ap.parse_args(argv)

    if a.cmd == "compact":
        res = compact_all(Store(a.out), day=a.date)
        print(f"compacted {sum(1 for r in res if r.files_in)} partitions, "
              f"{sum(r.files_in for r in res)} files merged")
        return

    client = IndexerClient(base_url=a.indexer, api_token=a.token, max_rps=a.rps)
    store = Store(a.out)
    pg = PostgresSink.connect(a.pg_dsn) if a.pg_dsn else None
    if a.cmd == "export":
        r = export_range(client, store, a.start, a.end, batch=a.batch, pg=pg)
        print(f"exported {r.blocks} blocks -> {r.rows} ({len(r.files)} files)")
    else:
        follow_forever(client, store, interval=a.interval, start_round=a.start, pg=pg)


if __name__ == "__main__":
    main()
