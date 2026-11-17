"""Optional Postgres sink: load the same tables into Postgres, idempotently.

  algoetl export --start A --end B --out ./data --pg-dsn postgresql://user:pw@host/db

Tables are created on first use (primary key = txid, or round for blocks) and rows
are inserted with ON CONFLICT DO NOTHING, so re-running a range never duplicates.
Needs `pip install "algoetl[postgres]"` (psycopg 3). Any DB-API connection works;
pass paramstyle="?" for sqlite (used in the tests).
"""
from __future__ import annotations

import pyarrow as pa

from .sink import SCHEMAS

PKEYS = {"blocks": "round", "transactions": "txid", "asset_transfers": "txid",
         "app_calls": "txid", "assets": "txid"}


def _sql_type(t: pa.DataType) -> str:
    if pa.types.is_decimal(t):
        return f"NUMERIC({t.precision},{t.scale})"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_int16(t):
        return "SMALLINT"
    if pa.types.is_int32(t):
        return "INTEGER"
    if pa.types.is_integer(t):
        return "BIGINT"
    return "TEXT"


def columns(table: str) -> list[str]:
    return ["date"] + [f.name for f in SCHEMAS[table]]


def ddl(table: str) -> str:
    cols = ["date DATE NOT NULL"] + [f'"{f.name}" {_sql_type(f.type)}' for f in SCHEMAS[table]]
    cols.append(f'PRIMARY KEY ("{PKEYS[table]}")')
    return f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(cols)})'


class PostgresSink:
    def __init__(self, conn, paramstyle: str = "%s", chunk: int = 5000):
        self.conn, self.ph, self.chunk = conn, paramstyle, chunk
        self._ready = False

    @classmethod
    def connect(cls, dsn: str) -> "PostgresSink":
        try:
            import psycopg
        except ImportError as e:  # pragma: no cover
            raise SystemExit('Postgres sink needs: pip install "algoetl[postgres]"') from e
        return cls(psycopg.connect(dsn))

    def create_tables(self) -> None:
        cur = self.conn.cursor()
        for t in SCHEMAS:
            cur.execute(ddl(t))
        self.conn.commit()
        self._ready = True

    def write(self, tables: dict[str, list[dict]]) -> dict[str, int]:
        """Insert rows (as produced by transform); one transaction for the whole call."""
        if not self._ready:
            self.create_tables()
        cur = self.conn.cursor()
        sent = {}
        try:
            for name, rows in tables.items():
                if not rows:
                    continue
                cols = columns(name)
                sql = (f'INSERT INTO "{name}" ({", ".join(chr(34) + c + chr(34) for c in cols)}) '
                       f'VALUES ({", ".join([self.ph] * len(cols))}) ON CONFLICT DO NOTHING')
                data = [tuple(r.get(c) for c in cols) for r in rows]
                for i in range(0, len(data), self.chunk):
                    cur.executemany(sql, data[i:i + self.chunk])
                sent[name] = len(data)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return sent

    def close(self) -> None:
        self.conn.close()
