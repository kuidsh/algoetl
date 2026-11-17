# algoetl — notes for Claude Code

Open-source Algorand Indexer -> Parquet exporter (portfolio project). Owner: David.
License Apache-2.0. Public repo: github.com/kuidsh/algoetl.

## Commands
- Setup: `python -m venv .venv && source .venv/bin/activate && pip install -e ".[dev,aws]"`
- Tests: `pytest -q` (offline; blocks.json is SYNTHETIC, real_blocks.json is 20 captured mainnet blocks)
- Live smoke test: `algoetl export --start <recent round> --end <+50> --out ./data`
- Query: `duckdb -c "select tx_type, count(*) from './data/transactions/*/*.parquet' group by 1"`

## Layout
- algoetl/client.py  Indexer REST client (stdlib, 4 req/s, 429 backoff)
- algoetl/transform.py  block JSON -> rows (blocks, transactions, asset_transfers, app_calls)
- algoetl/sink.py  Parquet writer (date= partitions, zstd) + checkpoint, local or s3://
- algoetl/pipeline.py  export_range / follow_once
- infra/template.yaml  SAM (NOT deployed; never deploy without David's OK)

## Rules
- Never commit keys, tokens, wallet mnemonics or .env files.
- Keep Nodely free-tier limits: <=4 req/s, 128k req/day (from 2027-01-01).
- Every change keeps `pytest -q` green; add a test with each feature.
- Real-data fixtures: capture small samples with tests/fixtures/capture.py, never commit >1 MB.

## Roadmap
See docs/ROADMAP.md.
