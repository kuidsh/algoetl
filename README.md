# algoetl: Algorand → Parquet → Athena (open-source ETL)

**Status: prototype v0.1 (2026-11-16).** 11 passing tests. Tested offline against
**synthetic** Indexer-shaped blocks only. The build workspace can't reach Algorand APIs,
so the first live run is step 3 below.

## Why
Algorand isn't covered by Dune's data catalog (checked 2026-11-16), and there's no maintained
`ethereum-etl`-style Python exporter for it. Teams and analysts who want SQL over Algorand
history end up running a full Indexer + PostgreSQL. algoetl turns any Indexer v2 endpoint
into date-partitioned Parquet that Athena, DuckDB, Spark or BigQuery can read directly.

## Tables
| table | grain | notes |
|---|---|---|
| `blocks` | round | tx_count includes inner txns; fees in microAlgos |
| `transactions` | txn (incl. inner) | inner txns get `txid = <root>:<path>`, `depth > 0`, `parent_txid` |
| `asset_transfers` | pay + axfer | ALGO is `asset_id = 0`; `amount` is decimal(20,0) base units |
| `app_calls` | appl | `method_selector` = ARC-4 4-byte selector (hex) when present |
| `assets` | acfg | one row per asset-config event: `action` = create / config / destroy; `total` is decimal(20,0) |

Optional Postgres sink: `pip install -e ".[postgres]"`, then add `--pg-dsn postgresql://user:pw@host/db`
(or set `ALGOETL_PG_DSN`) to `export` / `follow`. Tables are created on first use and loads are idempotent.

Layout: `<out>/<table>/date=YYYY-MM-DD/r<first>-<last>.parquet` (zstd). Re-running a range
overwrites the same files, so it's idempotent. A checkpoint in `<out>/_state/` makes `follow` resumable.

## Run locally
```bash
pip install pyarrow            # boto3 only if writing to s3://
python -m pytest -q            # 11 tests, offline
python -m algoetl.cli export --start 58000000 --end 58000100 --out ./data
python -m algoetl.cli follow --out ./data --start 58000000
# merge each day's small files into one compact-<day>.parquet per table (safe to re-run)
python -m algoetl.cli compact --out ./data [--date 2026-11-17]
duckdb -c "select tx_type, count(*) from './data/transactions/*/*.parquet' group by 1"
```
Default endpoint: Nodely free Indexer. Its limits are 128k requests/day and 5 req/s for
bot traffic from 2027-01-01, and algoetl defaults to 4 req/s with 429 backoff. Following the
tip needs about 31k requests/day. A full historical backfill needs your own Indexer, a paid
Nodely plan, or Nodely's public-goods exemption.

## Deploy to AWS (not done; needs your account)
```bash
sam build -t infra/template.yaml && sam deploy --guided   # set StartRound to a recent round
```
This creates a private, encrypted S3 bucket, an arm64 Lambda that runs every 5 minutes
(concurrency 1), a Glue database with partition projection (no crawler), and an Athena
workgroup capped at 1 GB scanned per query. See `COSTS.md`.

## Roadmap
- v0.2: daily compaction (merge the ~288 small files per table per day), `accounts_daily` balances table, asset metadata (`acfg`) table, real fixture captures.
- v0.3: public sample dataset (last 30 days) plus a DuckDB-WASM query page, for adoption metrics.
- v0.4: PostgreSQL sink option, plus a dbt package with standard models (DAU, asset flows, app usage).

License: Apache-2.0.
