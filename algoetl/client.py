"""Minimal Algorand Indexer REST client (stdlib only).

Talks to any Indexer v2 endpoint, e.g. Nodely's free tier
(https://mainnet-idx.4160.nodely.dev) or a self-hosted indexer.
Respects rate limits with a token bucket and retries 429/5xx with
exponential backoff, as Nodely's fair-usage policy asks.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Optional

DEFAULT_INDEXER = "https://mainnet-idx.4160.nodely.dev"

Transport = Callable[[str, dict], tuple[int, bytes]]


def _urllib_transport(url: str, headers: dict) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:  # non-2xx
        return e.code, e.read() or b""


@dataclass
class IndexerClient:
    base_url: str = DEFAULT_INDEXER
    api_token: Optional[str] = None
    max_rps: float = 4.0  # stay under the 5 req/s bot limit
    max_retries: int = 6
    transport: Transport = _urllib_transport
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    _last: float = field(default=0.0, init=False)

    def _headers(self) -> dict:
        h = {"Accept": "application/json", "User-Agent": "algoetl/0.1"}
        if self.api_token:
            h["X-Indexer-API-Token"] = self.api_token
        return h

    def _throttle(self) -> None:
        gap = 1.0 / self.max_rps
        wait = self._last + gap - self.clock()
        if wait > 0:
            self.sleep(wait)
        self._last = self.clock()

    def get(self, path: str) -> dict:
        url = self.base_url.rstrip("/") + path
        delay = 0.5
        for attempt in range(self.max_retries + 1):
            self._throttle()
            status, body = self.transport(url, self._headers())
            if 200 <= status < 300:
                return json.loads(body)
            if status == 404:
                raise LookupError(f"not found: {path}")
            if status in (403, 429) or status >= 500:
                if attempt == self.max_retries:
                    break
                self.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            raise RuntimeError(f"HTTP {status} for {path}: {body[:200]!r}")
        raise RuntimeError(f"gave up after {self.max_retries} retries: {path}")

    def tip(self) -> int:
        """Latest round the indexer has processed."""
        return int(self.get("/health")["round"])

    def block(self, rnd: int) -> dict:
        return self.get(f"/v2/blocks/{int(rnd)}")
