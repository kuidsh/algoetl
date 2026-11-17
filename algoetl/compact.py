"""Daily compaction: merge the many small r<first>-<last>.parquet files of each
<table>/date=<day>/ partition into a single compact-<day>.parquet.

Safe to re-run and to run while `follow` is writing: the merged file is written
first and the sources are deleted only afterwards, so a crash can leave
duplicates on disk (removed by the next run) but never lose data. Rows are
de-duplicated by key, so overlapping exports don't double count.
"""
from __future__ import annotations

import io
import re
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .sink import SCHEMAS, Store

KEYS = {"blocks": ["round"], "transactions": ["txid"],
        "asset_transfers": ["txid"], "app_calls": ["txid"], "assets": ["txid"]}
_PART = re.compile(r"^(?P<table>[^/]+)/date=(?P<day>\d{4}-\d{2}-\d{2})/(?P<name>[^/]+\.parquet)$")


@dataclass
class CompactResult:
    table: str
    day: str
    files_in: int   # files merged (0 = nothing to do)
    rows: int


def compact_partition(store: Store, table: str, day: str) -> CompactResult:
    prefix = f"{table}/date={day}/"
    target = f"{prefix}compact-{day}.parquet"
    keys = [k for k in store.list(prefix) if k.endswith(".parquet")]
    small = [k for k in keys if k != target]
    if not small:
        return CompactResult(table, day, 0, 0)
    schema = SCHEMAS[table]
    tbl = pa.concat_tables([pq.read_table(io.BytesIO(store.get(k)), schema=schema) for k in keys])
    seen, keep = set(), []
    for i, k in enumerate(zip(*(tbl.column(c).to_pylist() for c in KEYS[table]))):
        if k not in seen:
            seen.add(k)
            keep.append(i)
    tbl = tbl.take(keep)
    order = [("round", "ascending")] + ([("txid", "ascending")] if "txid" in tbl.schema.names else [])
    tbl = tbl.take(pc.sort_indices(tbl, sort_keys=order))
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    store.put(target, buf.getvalue())      # durable first ...
    for k in small:                        # ... then drop the sources
        store.delete(k)
    return CompactResult(table, day, len(small), tbl.num_rows)


def compact_all(store: Store, day: str | None = None) -> list[CompactResult]:
    found = set()
    for table in SCHEMAS:
        for k in store.list(f"{table}/date="):
            m = _PART.match(k)
            if m and (day is None or m["day"] == day):
                found.add((m["table"], m["day"]))
    return [compact_partition(store, t, d) for t, d in sorted(found)]
