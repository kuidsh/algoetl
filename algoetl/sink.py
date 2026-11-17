"""Write tables as Hive-partitioned Parquet (date=YYYY-MM-DD) to a local
folder or s3://bucket/prefix, plus a JSON checkpoint for resumable runs.

Layout:  <root>/<table>/date=2026-11-16/r<first>-<last>.parquet
         <root>/_state/checkpoint.json
Re-running a range rewrites the same file names, so loads are idempotent.
"""
from __future__ import annotations

import io
import json
import os
from collections import defaultdict
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMAS = {
    "blocks": pa.schema([("round", pa.int64()), ("ts", pa.int64()), ("tx_count", pa.int32()),
                         ("fees", pa.int64()), ("proposer", pa.string()), ("genesis_id", pa.string())]),
    "transactions": pa.schema([("round", pa.int64()), ("ts", pa.int64()), ("txid", pa.string()),
                               ("parent_txid", pa.string()), ("depth", pa.int16()), ("intra", pa.string()),
                               ("tx_type", pa.string()), ("sender", pa.string()), ("fee", pa.int64()),
                               ("group", pa.string()), ("note_text", pa.string())]),
    "asset_transfers": pa.schema([("round", pa.int64()), ("ts", pa.int64()), ("txid", pa.string()),
                                  ("asset_id", pa.int64()), ("sender", pa.string()), ("receiver", pa.string()),
                                  ("amount", pa.decimal128(20, 0)), ("close_to", pa.string())]),
    "app_calls": pa.schema([("round", pa.int64()), ("ts", pa.int64()), ("txid", pa.string()),
                            ("app_id", pa.int64()), ("sender", pa.string()), ("on_completion", pa.string()),
                            ("n_args", pa.int16()), ("depth", pa.int16()), ("method_selector", pa.string())]),
    "assets": pa.schema([("round", pa.int64()), ("ts", pa.int64()), ("txid", pa.string()),
                         ("asset_id", pa.int64()), ("action", pa.string()), ("sender", pa.string()),
                         ("creator", pa.string()), ("name", pa.string()), ("unit_name", pa.string()),
                         ("url", pa.string()), ("decimals", pa.int16()), ("total", pa.decimal128(20, 0)),
                         ("default_frozen", pa.bool_()), ("manager", pa.string()), ("reserve", pa.string()),
                         ("freeze", pa.string()), ("clawback", pa.string())]),
}

DECIMAL_COLS = {"asset_transfers": "amount", "assets": "total"}


class Store:
    def __init__(self, root: str, s3_client=None):
        self.root = root.rstrip("/")
        self.is_s3 = self.root.startswith("s3://")
        self._s3 = s3_client
        if self.is_s3 and self._s3 is None:
            import boto3  # only needed in AWS
            self._s3 = boto3.client("s3")

    def _split(self, key: str) -> tuple[str, str]:
        bucket, _, prefix = self.root[5:].partition("/")
        return bucket, f"{prefix}/{key}" if prefix else key

    def put(self, key: str, data: bytes) -> str:
        if self.is_s3:
            b, k = self._split(key)
            self._s3.put_object(Bucket=b, Key=k, Body=data)
            return f"s3://{b}/{k}"
        path = os.path.join(self.root, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def get(self, key: str) -> Optional[bytes]:
        if self.is_s3:
            b, k = self._split(key)
            try:
                return self._s3.get_object(Bucket=b, Key=k)["Body"].read()
            except self._s3.exceptions.NoSuchKey:
                return None
        path = os.path.join(self.root, key)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            return f.read()

    def list(self, prefix: str) -> list[str]:
        """Keys (relative to root) starting with prefix, sorted."""
        if self.is_s3:
            b, k = self._split(prefix)
            root_prefix = k[: len(k) - len(prefix)]
            keys, token = [], None
            while True:
                kw = {"Bucket": b, "Prefix": k}
                if token:
                    kw["ContinuationToken"] = token
                resp = self._s3.list_objects_v2(**kw)
                keys += [o["Key"][len(root_prefix):] for o in resp.get("Contents", [])]
                if not resp.get("IsTruncated"):
                    return sorted(keys)
                token = resp["NextContinuationToken"]
        out = []
        for d, _, files in os.walk(self.root):
            for fn in files:
                key = os.path.relpath(os.path.join(d, fn), self.root).replace(os.sep, "/")
                if key.startswith(prefix):
                    out.append(key)
        return sorted(out)

    def delete(self, key: str) -> None:
        if self.is_s3:
            b, k = self._split(key)
            self._s3.delete_object(Bucket=b, Key=k)
            return
        path = os.path.join(self.root, key)
        if os.path.exists(path):
            os.remove(path)

    # checkpoint -----------------------------------------------------------
    def checkpoint(self) -> Optional[int]:
        raw = self.get("_state/checkpoint.json")
        return None if raw is None else int(json.loads(raw)["last_round"])

    def set_checkpoint(self, rnd: int) -> None:
        self.put("_state/checkpoint.json", json.dumps({"last_round": int(rnd)}).encode())


def write_tables(store: Store, tables: dict[str, list[dict]], first: int, last: int) -> list[str]:
    written = []
    for name, rows in tables.items():
        if not rows:
            continue
        by_day: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            by_day[r["date"]].append(r)
        schema = SCHEMAS[name]
        for day, part in sorted(by_day.items()):
            cols = {f.name: [r.get(f.name) for r in part] for f in schema}
            if name in DECIMAL_COLS:
                from decimal import Decimal
                c = DECIMAL_COLS[name]
                cols[c] = [None if v is None else Decimal(v) for v in cols[c]]
            tbl = pa.table(cols, schema=schema)
            buf = io.BytesIO()
            pq.write_table(tbl, buf, compression="zstd")
            written.append(store.put(f"{name}/date={day}/r{first:010d}-{last:010d}.parquet", buf.getvalue()))
    return written
