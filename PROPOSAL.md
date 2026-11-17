# DRAFT: xGov retroactive proposal "algoetl: open-source Algorand → Parquet/SQL exporter"

> Draft for David. Submit only after v0.3 ships and the forum thread has collected feedback.

**Category:** Small (retroactive). **Requested:** 35,000 ALGO (≈ US$3,500 at proposal time; adjust to the price).
**License:** Apache-2.0. **Repo:** github.com/<you>/algoetl (public from day 1).

## Problem
Most chains have SQL-ready public datasets (Dune, BigQuery, `*-etl` exporters). Algorand isn't in
Dune's catalog, so analysts and teams who want history in SQL have to run a full Indexer + Postgres.
That raises the bar for ecosystem dashboards, grant reporting and research.

## What was delivered
- A Python exporter for any Indexer v2 endpoint → date-partitioned Parquet (blocks, transactions with
  inner txns, asset transfers, app calls with ARC-4 selectors). It's idempotent and resumable, and rate-limit friendly.
- One-command AWS deploy (Lambda + S3 + Glue/Athena with partition projection) at about US$1–4 a month.
- A public 30-day sample dataset plus a browser SQL page (DuckDB-WASM). *(v0.3)*
- Test suite and docs.

## Adoption metrics (fill in at submission)
- GitHub stars/forks/clones · dataset downloads · forum/Discord users · teams using it.

## Budget rationale
About 90 h of engineering at ≈ US$38/h (the same benchmark as recent retro SDK proposals).

## Maintenance
12 months of bug fixes on a best-effort basis. Compaction and new tables go in a follow-up proposal.
