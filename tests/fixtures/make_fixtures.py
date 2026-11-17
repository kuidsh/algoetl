"""Generate SYNTHETIC Indexer v2 block JSON for offline tests and the demo.

Shapes follow the Indexer /v2/blocks/{round} response (tx-type, payment-transaction,
asset-transfer-transaction, application-transaction, inner-txns, base64 note/args).
Addresses, ids and amounts are made up. Replace with real captures once the
workspace (or a user machine) can reach an Indexer endpoint.
"""
import base64
import hashlib
import json
import random
import sys
from pathlib import Path

rng = random.Random(7)
ADDRS = ["".join(rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") for _ in range(58)) for _ in range(40)]
ASSETS = [31566704, 312769, 386192725, 1138500612]  # ids used only as labels in fixtures
APPS = [1002541853, 552635992, 1134695678]
B64 = lambda b: base64.b64encode(b).decode()


def txid(*parts):
    return base64.b32encode(hashlib.sha256(repr(parts).encode()).digest()).decode().rstrip("=")


def make_block(rnd: int, ts: int) -> dict:
    txns = []
    for i in range(rng.randint(3, 9)):
        kind = rng.choices(["pay", "axfer", "appl"], [4, 4, 3])[0]
        tid = txid(rnd, i)
        snd = rng.choice(ADDRS)
        tx = {"id": tid, "tx-type": kind, "sender": snd, "fee": 1000, "confirmed-round": rnd, "round-time": ts}
        if kind == "pay":
            tx["payment-transaction"] = {"receiver": rng.choice(ADDRS), "amount": rng.randint(1, 50_000) * 1000}
            if rng.random() < 0.3:
                tx["note"] = B64(b"invoice #" + str(rng.randint(100, 999)).encode())
        elif kind == "axfer":
            tx["asset-transfer-transaction"] = {"asset-id": rng.choice(ASSETS), "receiver": rng.choice(ADDRS),
                                                "amount": rng.randint(1, 10**9)}
        else:
            app = rng.choice(APPS)
            tx["application-transaction"] = {"application-id": app, "on-completion": "noop",
                                             "application-args": [B64(rng.randbytes(4)), B64(b"\x00\x01")]}
            if rng.random() < 0.6:
                tx["fee"] = 2000
                tx["inner-txns"] = [
                    {"tx-type": "axfer", "sender": "APP" + str(app), "fee": 0,
                     "asset-transfer-transaction": {"asset-id": rng.choice(ASSETS), "receiver": snd,
                                                    "amount": rng.randint(1, 10**8)}},
                ]
        if i in (0, 1) and len(txns) == 1:
            g = B64(hashlib.sha256(str(rnd).encode()).digest())
            txns[0]["group"] = g
            tx["group"] = g
        txns.append(tx)
    return {"round": rnd, "timestamp": ts, "genesis-id": "mainnet-v1.0", "proposer": rng.choice(ADDRS),
            "transactions": txns}


def main(out: Path, first=58_000_000, n=60, ts0=1_794_873_540):  # ts0 = 2026-11-16 23:59 UTC
    blocks = {}
    for k in range(n):
        rnd = first + k
        blocks[rnd] = make_block(rnd, ts0 + int(k * 2.8))
    out.write_text(json.dumps(blocks, indent=1))
    print(f"wrote {n} synthetic blocks to {out}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_name("blocks.json"))
