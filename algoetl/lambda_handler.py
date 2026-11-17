"""AWS Lambda entry point: one EventBridge tick = catch up to the tip.

Env: ALGOETL_OUT (s3://bucket/prefix), ALGOETL_INDEXER, ALGOETL_START_ROUND,
     ALGOETL_MAX_BLOCKS (default 600: ~150 s at 4 req/s, inside a 300 s timeout;
     a 5-min tick only produces ~100 new rounds at ~2.8 s per block)
"""
from __future__ import annotations

import os

from .client import DEFAULT_INDEXER, IndexerClient
from .pipeline import follow_once
from .sink import Store


def handler(event, context):
    client = IndexerClient(base_url=os.environ.get("ALGOETL_INDEXER", DEFAULT_INDEXER),
                           api_token=os.environ.get("ALGOETL_TOKEN"),
                           max_rps=float(os.environ.get("ALGOETL_RPS", "4")))
    store = Store(os.environ["ALGOETL_OUT"])
    start = os.environ.get("ALGOETL_START_ROUND")
    r = follow_once(client, store, max_blocks=int(os.environ.get("ALGOETL_MAX_BLOCKS", "600")),
                    start_round=int(start) if start else None)
    return {"first": r.first, "last": r.last, "blocks": r.blocks, "rows": r.rows}
