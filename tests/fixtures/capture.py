"""Capture REAL mainnet blocks from an Indexer into a fixture file.

  python tests/fixtures/capture.py 58000000 20 tests/fixtures/real_blocks.json
"""
import json
import sys

from algoetl.client import IndexerClient

start, n, out = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
c = IndexerClient()
blocks = {str(r): c.block(r) for r in range(start, start + n)}
with open(out, "w") as f:
    json.dump(blocks, f)
print(f"saved {n} blocks ({sum(len(b.get('transactions', [])) for b in blocks.values())} txns) to {out}")
