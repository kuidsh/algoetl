"""Run an export over a round range, or follow the chain tip from a checkpoint."""
from __future__ import annotations

import time
from dataclasses import dataclass

from .client import IndexerClient
from .sink import Store, write_tables
from .transform import merge, transform_block


@dataclass
class RunResult:
    first: int
    last: int
    blocks: int
    rows: dict
    files: list


def export_range(client: IndexerClient, store: Store, first: int, last: int,
                 batch: int = 200, checkpoint: bool = True, pg=None) -> RunResult:
    if last < first:
        return RunResult(first, last, 0, {}, [])
    files, totals, n = [], {}, 0
    start = first
    while start <= last:
        end = min(start + batch - 1, last)
        acc: dict = {}
        for rnd in range(start, end + 1):
            merge(acc, transform_block(client.block(rnd)))
            n += 1
        files += write_tables(store, acc, start, end)
        if pg is not None:
            pg.write(acc)  # idempotent (ON CONFLICT DO NOTHING), so a retry after a crash is safe
        for k, v in acc.items():
            totals[k] = totals.get(k, 0) + len(v)
        if checkpoint:
            store.set_checkpoint(end)  # only after the batch is durable
        start = end + 1
    return RunResult(first, last, n, totals, files)


def follow_once(client: IndexerClient, store: Store, max_blocks: int = 2000,
                start_round: int | None = None, batch: int = 200, pg=None) -> RunResult:
    """One scheduled tick: from checkpoint+1 to the tip, capped at max_blocks."""
    cp = store.checkpoint()
    tip = client.tip()
    first = (cp + 1) if cp is not None else (start_round if start_round is not None else tip)
    last = min(tip, first + max_blocks - 1)
    return export_range(client, store, first, last, batch=batch, pg=pg)


def follow_forever(client: IndexerClient, store: Store, interval: float = 30.0, **kw) -> None:
    while True:
        r = follow_once(client, store, **kw)
        print(f"rounds {r.first}-{r.last}: {r.blocks} blocks {r.rows}")
        time.sleep(interval)
