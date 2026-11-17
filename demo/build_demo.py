"""Run the real pipeline over the synthetic fixtures and summarise the Parquet output
into demo/data.json for the demo page. The data is SYNTHETIC, not mainnet."""
import json
import sys
import tempfile
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from algoetl.pipeline import export_range  # noqa: E402
from algoetl.sink import Store  # noqa: E402
from test_algoetl import FIRST, LAST, fake_client  # noqa: E402

with tempfile.TemporaryDirectory() as d:
    client, _, _ = fake_client()
    store = Store(d)
    r = export_range(client, store, FIRST, LAST, batch=20)
    files = sorted(str(Path(p).relative_to(d)) for p in r.files)

    def table(name):
        return ds.dataset(f"{d}/{name}", format="parquet", partitioning="hive").to_table()

    tx = table("transactions")
    by_type = tx.group_by("tx_type").aggregate([("txid", "count")]).to_pylist()
    by_day = tx.group_by("date").aggregate([("txid", "count"), ("sender", "count_distinct"), ("fee", "sum")]).to_pylist()
    at = table("asset_transfers")
    top_assets = sorted(at.group_by("asset_id").aggregate([("txid", "count"), ("receiver", "count_distinct")]).to_pylist(),
                        key=lambda x: -x["txid_count"])
    ac = table("app_calls")
    apps = ac.group_by("app_id").aggregate([("txid", "count")]).to_pylist()
    inner = pc.sum(pc.greater(tx["depth"], 0)).as_py()
    sample = tx.filter(pc.greater(tx["depth"], 0)).slice(0, 3).select(
        ["round", "txid", "parent_txid", "depth", "tx_type"]).to_pylist()

data = {
    "range": [FIRST, LAST], "blocks": r.blocks, "rows": r.rows, "files": len(files), "file_sample": files[:6],
    "by_type": by_type, "by_day": by_day, "top_assets": top_assets, "apps": apps,
    "inner_txns": inner, "inner_sample": sample,
}
out = Path(__file__).with_name("data.json")
out.write_text(json.dumps(data, indent=1, default=str))
print(json.dumps(data, default=str)[:1500])
