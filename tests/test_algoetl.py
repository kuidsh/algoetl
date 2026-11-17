import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from algoetl.client import IndexerClient
from algoetl.pipeline import export_range, follow_once
from algoetl.sink import Store
from algoetl.transform import transform_block

FIX = json.loads((Path(__file__).parent / "fixtures" / "blocks.json").read_text())
FIRST = min(int(k) for k in FIX)
LAST = max(int(k) for k in FIX)


class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.sleeps = []

    def now(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


def fake_client(fail_first=0, tip=LAST):
    calls = {"n": 0}

    def transport(url, headers):
        calls["n"] += 1
        if calls["n"] <= fail_first:
            return 429, b"slow down"
        if url.endswith("/health"):
            return 200, json.dumps({"round": tip}).encode()
        rnd = url.rsplit("/", 1)[1]
        if rnd not in FIX:
            return 404, b""
        return 200, json.dumps(FIX[rnd]).encode()

    clk = FakeClock()
    c = IndexerClient(base_url="https://idx.example", transport=transport, sleep=clk.sleep, clock=clk.now)
    return c, clk, calls


def test_transform_counts_inner_txns():
    blk = FIX[str(FIRST)]
    out = transform_block(blk)
    top = len(blk["transactions"])
    inner = sum(len(t.get("inner-txns", [])) for t in blk["transactions"])
    assert len(out["transactions"]) == top + inner
    assert out["blocks"][0]["tx_count"] == top + inner
    assert out["blocks"][0]["fees"] == sum(t["fee"] for t in blk["transactions"])


def test_inner_txn_ids_are_unique_and_linked():
    out = transform_block(FIX[str(FIRST + 3)])
    ids = [t["txid"] for t in out["transactions"]]
    assert len(ids) == len(set(ids))
    for t in out["transactions"]:
        if t["depth"] > 0:
            assert t["parent_txid"] and t["txid"].startswith(t["parent_txid"] + ":")


def test_payment_is_asset_zero_and_note_decoded():
    found_note = False
    for blk in FIX.values():
        out = transform_block(blk)
        for r in out["asset_transfers"]:
            if r["txid"] in {t["id"] for t in blk["transactions"] if t["tx-type"] == "pay"}:
                assert r["asset_id"] == 0
        found_note |= any((t["note_text"] or "").startswith("invoice #") for t in out["transactions"])
    assert found_note


def test_app_call_selector():
    for blk in FIX.values():
        for r in transform_block(blk)["app_calls"]:
            assert r["method_selector"] is None or len(r["method_selector"]) == 8
            assert r["app_id"] > 0


def test_unknown_tx_type_rejected():
    blk = {"round": 1, "timestamp": 0, "transactions": [{"id": "x", "tx-type": "zzz", "fee": 0}]}
    with pytest.raises(ValueError):
        transform_block(blk)


def test_export_writes_partitioned_parquet_and_checkpoint(tmp_path):
    c, _, _ = fake_client()
    store = Store(str(tmp_path))
    r = export_range(c, store, FIRST, FIRST + 24, batch=10)
    assert r.blocks == 25
    assert store.checkpoint() == FIRST + 24
    files = sorted(tmp_path.glob("transactions/date=*/r*.parquet"))
    # fixtures start at 23:59 UTC, so batch 1 spans midnight and splits into two partitions
    assert len(files) == 4
    assert {f.parent.name for f in files} == {"date=2026-11-16", "date=2026-11-17"}
    total = sum(pq.read_table(f).num_rows for f in files)
    assert total == r.rows["transactions"]
    assert "txid" in pq.read_table(files[0]).schema.names


def test_rerun_is_idempotent(tmp_path):
    c, _, _ = fake_client()
    store = Store(str(tmp_path))
    export_range(c, store, FIRST, FIRST + 9, batch=10)
    before = sorted(p.name for p in tmp_path.rglob("*.parquet"))
    export_range(c, store, FIRST, FIRST + 9, batch=10)
    assert sorted(p.name for p in tmp_path.rglob("*.parquet")) == before


def test_follow_resumes_from_checkpoint(tmp_path):
    c, _, _ = fake_client(tip=FIRST + 30)
    store = Store(str(tmp_path))
    r1 = follow_once(c, store, max_blocks=20, start_round=FIRST)
    assert (r1.first, r1.last) == (FIRST, FIRST + 19)
    r2 = follow_once(c, store, max_blocks=20)
    assert (r2.first, r2.last) == (FIRST + 20, FIRST + 30)
    r3 = follow_once(c, store, max_blocks=20)
    assert r3.blocks == 0  # caught up


def test_client_backs_off_on_429():
    c, clk, calls = fake_client(fail_first=3)
    assert c.tip() == LAST
    assert calls["n"] == 4
    backoffs = [s for s in clk.sleeps if s >= 0.5]
    assert backoffs[:3] == [0.5, 1.0, 2.0]


def test_client_throttles_to_max_rps():
    c, clk, _ = fake_client()
    for r in range(FIRST, FIRST + 9):
        c.block(r)
    assert clk.t >= 8 / c.max_rps - 1e-9


def test_s3_store_uses_bucket_and_prefix():
    class S3:
        class exceptions:
            class NoSuchKey(Exception):
                pass

        def __init__(self):
            self.objs = {}

        def put_object(self, Bucket, Key, Body):
            self.objs[(Bucket, Key)] = Body

        def get_object(self, Bucket, Key):
            if (Bucket, Key) not in self.objs:
                raise self.exceptions.NoSuchKey()
            import io
            return {"Body": io.BytesIO(self.objs[(Bucket, Key)])}

    s3 = S3()
    st = Store("s3://bkt/algorand/mainnet", s3_client=s3)
    assert st.checkpoint() is None
    st.set_checkpoint(42)
    assert ("bkt", "algorand/mainnet/_state/checkpoint.json") in s3.objs
    assert st.checkpoint() == 42


# ---- real mainnet blocks (captured with tests/fixtures/capture.py) ----
REAL = json.loads((Path(__file__).parent / "fixtures" / "real_blocks.json").read_text())


def _count_txns(txns):
    return sum(1 + _count_txns(t.get("inner-txns")) for t in txns or [])


def test_real_blocks_transform_without_error():
    assert len(REAL) == 20
    for blk in REAL.values():
        out = transform_block(blk)
        assert out["blocks"][0]["round"] == blk["round"]


def test_real_blocks_row_counts_match_source():
    for blk in REAL.values():
        out = transform_block(blk)
        n = _count_txns(blk.get("transactions"))
        assert len(out["transactions"]) == n
        assert out["blocks"][0]["tx_count"] == n
        assert len(out["app_calls"]) == len([t for t in out["transactions"] if t["tx_type"] == "appl"])


def test_real_blocks_txids_unique_across_range():
    ids = [t["txid"] for blk in REAL.values() for t in transform_block(blk)["transactions"]]
    assert len(ids) == len(set(ids))


def test_real_blocks_transfers_and_app_calls_are_well_formed():
    seen_types = set()
    for blk in REAL.values():
        out = transform_block(blk)
        for t in out["transactions"]:
            seen_types.add(t["tx_type"])
            assert t["sender"]
            assert t["fee"] >= 0
        for r in out["asset_transfers"]:
            assert r["asset_id"] >= 0 and r["amount"] >= 0
            assert r["receiver"]
        for r in out["app_calls"]:
            assert r["app_id"] > 0
            assert r["method_selector"] is None or len(r["method_selector"]) == 8
    assert {"pay", "axfer", "appl"} <= seen_types


# ---- compaction ----
from algoetl.compact import compact_all  # noqa: E402

TABLES = ("blocks", "transactions", "asset_transfers", "app_calls")


def _rows(root, table):
    files = sorted(Path(root).glob(f"{table}/date=*/*.parquet"))
    return files, sum(pq.read_table(f).num_rows for f in files)


def test_compact_merges_files_per_day_and_keeps_rows(tmp_path):
    c, _, _ = fake_client()
    store = Store(str(tmp_path))
    export_range(c, store, FIRST, FIRST + 24, batch=5)
    before = {t: _rows(tmp_path, t) for t in TABLES}
    assert len(before["transactions"][0]) > 2
    res = compact_all(store)
    assert sum(r.files_in for r in res) == sum(len(f) for f, _ in before.values())
    for t, (_, n) in before.items():
        files, n_after = _rows(tmp_path, t)
        assert n_after == n
        assert all(f.name == f"compact-{f.parent.name[5:]}.parquet" for f in files)
        assert len({f.parent for f in files}) == len(files)  # one file per day


def test_compact_is_idempotent_and_absorbs_new_files(tmp_path):
    c, _, _ = fake_client()
    store = Store(str(tmp_path))
    export_range(c, store, FIRST, FIRST + 9, batch=5)
    compact_all(store)
    snap = {f: f.read_bytes() for f in tmp_path.rglob("compact-*.parquet")}
    assert all(r.files_in == 0 for r in compact_all(store))
    assert snap == {f: f.read_bytes() for f in tmp_path.rglob("compact-*.parquet")}
    export_range(c, store, FIRST + 10, FIRST + 14, batch=5)  # new small files arrive
    compact_all(store)
    files, n = _rows(tmp_path, "blocks")
    assert n == 15 and all(f.name.startswith("compact-") for f in files)


def test_compact_dedupes_overlapping_exports(tmp_path):
    c, _, _ = fake_client()
    store = Store(str(tmp_path))
    export_range(c, store, FIRST, FIRST + 9, batch=10)
    export_range(c, store, FIRST + 5, FIRST + 14, batch=10)  # overlaps, different file names
    compact_all(store)
    assert _rows(tmp_path, "blocks")[1] == 15
    expected = sum(transform_block(FIX[str(r)])["blocks"][0]["tx_count"] for r in range(FIRST, FIRST + 15))
    assert _rows(tmp_path, "transactions")[1] == expected


def test_compact_single_day_filter(tmp_path):
    c, _, _ = fake_client()
    store = Store(str(tmp_path))
    export_range(c, store, FIRST, FIRST + 24, batch=5)
    res = compact_all(store, day="2026-11-17")
    assert {r.day for r in res} == {"2026-11-17"}
    assert list(tmp_path.glob("transactions/date=2026-11-16/r*.parquet"))  # untouched


def test_compact_works_on_s3_store():
    class S3:
        class exceptions:
            class NoSuchKey(Exception):
                pass

        def __init__(self):
            self.objs = {}

        def put_object(self, Bucket, Key, Body):
            self.objs[(Bucket, Key)] = Body

        def get_object(self, Bucket, Key):
            import io
            return {"Body": io.BytesIO(self.objs[(Bucket, Key)])}

        def delete_object(self, Bucket, Key):
            self.objs.pop((Bucket, Key), None)

        def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
            ks = sorted(k for b, k in self.objs if b == Bucket and k.startswith(Prefix))
            return {"Contents": [{"Key": k} for k in ks], "IsTruncated": False}

    s3 = S3()
    store = Store("s3://bkt/algo", s3_client=s3)
    c, _, _ = fake_client()
    export_range(c, store, FIRST, FIRST + 9, batch=5)
    compact_all(store)
    keys = [k for _, k in s3.objs if k.startswith("algo/blocks/")]
    assert keys and all("/compact-" in k for k in keys)
    assert store.checkpoint() == FIRST + 9


# ---- assets table (acfg) ----
import base64  # noqa: E402
import sqlite3  # noqa: E402

from algoetl.pg import PostgresSink, ddl  # noqa: E402


def _acfg_block():
    def b64(s):
        return base64.b64encode(s).decode()

    return {"round": 5, "timestamp": 1_700_000_000, "genesis-id": "mainnet-v1.0", "transactions": [
        {"id": "CREATE", "tx-type": "acfg", "sender": "ALICE", "fee": 1000, "created-asset-index": 777,
         "asset-config-transaction": {"params": {
             "creator": "ALICE", "decimals": 6, "total": 18446744073709551615, "default-frozen": False,
             "name": "Coin", "unit-name": "CN", "url": "https://x", "manager": "ALICE", "clawback": "BOB"}}},
        {"id": "BYTES", "tx-type": "acfg", "sender": "ALICE", "fee": 1000, "created-asset-index": 778,
         "asset-config-transaction": {"params": {
             "creator": "ALICE", "decimals": 0, "total": 1, "name-b64": b64(b"Cool"), "unit-name-b64": b64(b"\xff\xfe")}}},
        {"id": "CONFIG", "tx-type": "acfg", "sender": "ALICE", "fee": 1000,
         "asset-config-transaction": {"asset-id": 777, "params": {"manager": "BOB", "reserve": "BOB"}}},
        {"id": "DESTROY", "tx-type": "acfg", "sender": "BOB", "fee": 1000,
         "asset-config-transaction": {"asset-id": 777}},
        {"id": "APP", "tx-type": "appl", "sender": "ALICE", "fee": 1000,
         "application-transaction": {"application-id": 5}, "inner-txns": [
             {"tx-type": "acfg", "sender": "APPADDR", "fee": 0, "created-asset-index": 900,
              "asset-config-transaction": {"params": {"creator": "APPADDR", "total": 10, "name": "Inner"}}}]},
    ]}


def test_assets_create_config_destroy_and_inner():
    rows = {r["txid"]: r for r in transform_block(_acfg_block())["assets"]}
    assert len(rows) == 5  # 4 top-level + 1 inner
    c = rows["CREATE"]
    assert (c["action"], c["asset_id"], c["creator"], c["name"], c["unit_name"], c["total"]) == \
        ("create", 777, "ALICE", "Coin", "CN", 2**64 - 1)
    assert c["decimals"] == 6 and c["default_frozen"] is False and c["clawback"] == "BOB"
    assert rows["BYTES"]["name"] == "Cool" and rows["BYTES"]["unit_name"] is None  # non-utf8 -> None
    assert (rows["CONFIG"]["action"], rows["CONFIG"]["asset_id"], rows["CONFIG"]["manager"]) == ("config", 777, "BOB")
    assert rows["CONFIG"]["total"] is None
    assert (rows["DESTROY"]["action"], rows["DESTROY"]["asset_id"]) == ("destroy", 777)
    inner = [r for r in transform_block(_acfg_block())["assets"] if r["asset_id"] == 900][0]
    assert inner["action"] == "create" and inner["txid"].startswith("APP:")


def test_assets_written_to_parquet_with_u64_total(tmp_path):
    store = Store(str(tmp_path))
    from algoetl.sink import write_tables
    files = write_tables(store, transform_block(_acfg_block()), 5, 5)
    f = [x for x in files if "/assets/" in x.replace("\\", "/")]
    assert len(f) == 1
    t = pq.read_table(f[0])
    assert t.num_rows == 5 and str(max(v for v in t.column("total").to_pylist() if v is not None)) == str(2**64 - 1)


def test_real_blocks_assets_match_acfg_transactions():
    n_acfg = n_assets = 0
    for blk in REAL.values():
        out = transform_block(blk)
        n_acfg += sum(1 for t in out["transactions"] if t["tx_type"] == "acfg")
        n_assets += len(out["assets"])
        for r in out["assets"]:
            assert r["asset_id"] > 0 and r["action"] in {"create", "config", "destroy"}
    assert n_acfg == n_assets > 0


def _pg():
    return PostgresSink(sqlite3.connect(":memory:"), paramstyle="?")


def test_postgres_sink_loads_all_tables_and_is_idempotent():
    pg = _pg()
    c, _, _ = fake_client()
    acc = {}
    from algoetl.transform import merge
    for rnd in range(FIRST, FIRST + 5):
        merge(acc, transform_block(FIX[str(rnd)]))
    blk = _acfg_block()
    blk["transactions"][0]["asset-config-transaction"]["params"]["total"] = 2**62  # sqlite can't hold u64
    merge(acc, transform_block(blk))
    pg.write(acc)
    pg.write(acc)  # re-run: no duplicates
    cur = pg.conn.cursor()
    for name, rows in acc.items():
        got = cur.execute(f'select count(*) from "{name}"').fetchone()[0]
        assert got == len(rows), name
    assert cur.execute('select total from assets where txid = ?', ("CREATE",)).fetchone()[0] == 2**62
    assert cur.execute('select date from blocks limit 1').fetchone()[0].startswith("2026-")


def test_postgres_ddl_uses_numeric_and_primary_keys():
    assert "NUMERIC(20,0)" in ddl("asset_transfers") and "NUMERIC(20,0)" in ddl("assets")
    assert 'PRIMARY KEY ("round")' in ddl("blocks") and 'PRIMARY KEY ("txid")' in ddl("assets")
    assert '"group" TEXT' in ddl("transactions")  # reserved word is quoted


def test_export_range_also_writes_to_postgres(tmp_path):
    pg = _pg()
    c, _, _ = fake_client()
    r = export_range(c, Store(str(tmp_path)), FIRST, FIRST + 9, batch=5, pg=pg)
    cur = pg.conn.cursor()
    assert cur.execute("select count(*) from blocks").fetchone()[0] == 10
    assert cur.execute("select count(*) from transactions").fetchone()[0] == r.rows["transactions"]
    export_range(c, Store(str(tmp_path)), FIRST, FIRST + 9, batch=5, pg=pg)  # retry is safe
    assert cur.execute("select count(*) from transactions").fetchone()[0] == r.rows["transactions"]
