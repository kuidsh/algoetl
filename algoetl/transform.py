"""Flatten Indexer block JSON into analytics tables.

Tables (one row each):
  blocks          - one per round
  transactions    - every txn, including inner txns (depth > 0)
  asset_transfers - pay (ALGO, asset_id = 0) and axfer movements
  app_calls       - appl txns
  assets          - acfg txns (create / config / destroy events for ASAs)
All amounts stay in base units (microAlgos / asset base units) as int.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Iterator

TX_TYPES = {"pay", "keyreg", "acfg", "axfer", "afrz", "appl", "stpf", "hb"}


def _date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _note_text(b64: str | None) -> str | None:
    if not b64:
        return None
    raw = base64.b64decode(b64)
    try:
        txt = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return txt if txt.isprintable() else None


def _walk(txns, parent_id, depth, path) -> Iterator[tuple[dict, str, int, str]]:
    for i, tx in enumerate(txns or []):
        p = f"{path}/{i}" if path else str(i)
        yield tx, parent_id, depth, p
        root = tx.get("id") or parent_id
        yield from _walk(tx.get("inner-txns"), root, depth + 1, p)


def transform_block(block: dict) -> dict[str, list[dict]]:
    rnd = int(block["round"])
    ts = int(block["timestamp"])
    day = _date(ts)
    out: dict[str, list[dict]] = {"blocks": [], "transactions": [], "asset_transfers": [], "app_calls": [],
                                  "assets": []}

    tx_count = 0
    fees = 0
    for tx, parent, depth, path in _walk(block.get("transactions"), None, 0, ""):
        ttype = tx.get("tx-type")
        if ttype not in TX_TYPES:
            raise ValueError(f"unknown tx-type {ttype!r} in round {rnd}")
        tx_count += 1
        fee = int(tx.get("fee", 0))
        fees += fee
        # inner txns have no id; key them by root id + path
        txid = tx.get("id") or f"{parent}:{path}"
        out["transactions"].append({
            "round": rnd, "date": day, "ts": ts, "txid": txid,
            "parent_txid": parent if depth else None, "depth": depth, "intra": path,
            "tx_type": ttype, "sender": tx.get("sender"), "fee": fee,
            "group": tx.get("group"), "note_text": _note_text(tx.get("note")),
        })
        if ttype == "pay":
            p = tx.get("payment-transaction", {})
            out["asset_transfers"].append({
                "round": rnd, "date": day, "ts": ts, "txid": txid, "asset_id": 0,
                "sender": tx.get("sender"), "receiver": p.get("receiver"),
                "amount": int(p.get("amount", 0)), "close_to": p.get("close-remainder-to"),
            })
        elif ttype == "axfer":
            a = tx.get("asset-transfer-transaction", {})
            out["asset_transfers"].append({
                "round": rnd, "date": day, "ts": ts, "txid": txid, "asset_id": int(a.get("asset-id", 0)),
                "sender": a.get("sender") or tx.get("sender"), "receiver": a.get("receiver"),
                "amount": int(a.get("amount", 0)), "close_to": a.get("close-to"),
            })
        elif ttype == "acfg":
            out["assets"].append(_asset_row(tx, txid, rnd, day, ts))
        elif ttype == "appl":
            a = tx.get("application-transaction", {})
            app_id = int(a.get("application-id", 0)) or int(tx.get("created-application-index", 0))
            out["app_calls"].append({
                "round": rnd, "date": day, "ts": ts, "txid": txid, "app_id": app_id,
                "sender": tx.get("sender"), "on_completion": a.get("on-completion"),
                "n_args": len(a.get("application-args") or []), "depth": depth,
                "method_selector": _selector(a.get("application-args")),
            })

    out["blocks"].append({
        "round": rnd, "date": day, "ts": ts, "tx_count": tx_count, "fees": fees,
        "proposer": block.get("proposer"), "genesis_id": block.get("genesis-id"),
    })
    return out


def _text(params: dict, key: str) -> str | None:
    """Indexer gives `name` (utf-8) or `name-b64` (raw bytes); return printable text or None."""
    if params.get(key) is not None:
        return params[key]
    b64 = params.get(f"{key}-b64")
    return _note_text(b64) if b64 else None


def _asset_row(tx: dict, txid: str, rnd: int, day: str, ts: int) -> dict:
    """One row per asset-config txn. action: create | config | destroy.

    create:  asset-id 0 in the txn, the new id is `created-asset-index`.
    destroy: an existing asset-id with no params.
    config:  an existing asset-id with params (reconfigure; unset roles mean "cleared").
    """
    a = tx.get("asset-config-transaction") or {}
    params = a.get("params") or {}
    asset_id = int(a.get("asset-id") or 0)
    if asset_id == 0:
        action, asset_id = "create", int(tx.get("created-asset-index") or 0)
    else:
        action = "config" if params else "destroy"
    total = params.get("total")
    return {
        "round": rnd, "date": day, "ts": ts, "txid": txid, "asset_id": asset_id, "action": action,
        "sender": tx.get("sender"), "creator": params.get("creator"),
        "name": _text(params, "name"), "unit_name": _text(params, "unit-name"), "url": _text(params, "url"),
        "decimals": params.get("decimals"), "total": int(total) if total is not None else None,
        "default_frozen": params.get("default-frozen"),
        "manager": params.get("manager"), "reserve": params.get("reserve"),
        "freeze": params.get("freeze"), "clawback": params.get("clawback"),
    }


def _selector(args) -> str | None:
    """ARC-4 method selector = first 4 bytes of arg 0 (hex)."""
    if not args:
        return None
    raw = base64.b64decode(args[0])
    return raw[:4].hex() if len(raw) == 4 else None


def merge(acc: dict[str, list[dict]], part: dict[str, list[dict]]) -> dict[str, list[dict]]:
    for k, rows in part.items():
        acc.setdefault(k, []).extend(rows)
    return acc
