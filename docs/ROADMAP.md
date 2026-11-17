# Roadmap

Done
- v0.1: Indexer -> date-partitioned Parquet (`blocks`, `transactions` incl. inner txns, `asset_transfers`, `app_calls`), checkpointed `follow`, S3 or local output, SAM template.
- v0.2: tests over captured real mainnet blocks, `compact` command, `assets` table (acfg), optional Postgres sink.

Next
- `accounts_daily` balances table.
- Public 30-day sample dataset plus a DuckDB-WASM query page.
- dbt package with standard models (DAU, asset flows, app usage).
- Verify the Postgres sink against a real Postgres in CI.
