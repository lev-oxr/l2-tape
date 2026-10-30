"""REST order book snapshots on a schedule -> data/snapshots, one Parquet file per snapshot.

Snapshots are the reference for the book rebuilt from WebSocket deltas. Compare them by
update id, not by time: Binance `lastUpdateId` is on the same counter as the stream's U/u;
Bybit REST `u` is on the same counter as the `orderbook.1000` stream (checked 2026-10-30).
"""

import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

# Max depth each exchange serves. Binance weight at 1000 levels is 20 of 2400/min (measured).
URLS = {
    "binancefutures": "https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000",
    "bybit": "https://api.bybit.com/v5/market/orderbook?category=linear&symbol={symbol}&limit=1000",
}

SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("symbol", pa.string()),
        ("local_ts", pa.int64()),  # ns UTC, when the HTTP response arrived
        ("exch_ts", pa.int64()),  # ns UTC, exchange message time (Binance E, Bybit ts)
        ("last_update_id", pa.int64()),  # Binance lastUpdateId, Bybit u
        ("seq", pa.int64()),  # Bybit cross sequence; null for Binance
        ("side", pa.string()),  # bid | ask
        ("level", pa.int32()),  # 0 = best price
        ("price", pa.float64()),
        ("qty", pa.float64()),
    ]
)


def parse(exchange: str, symbol: str, local_ts: int, body: dict) -> pa.Table:
    if exchange == "binancefutures":
        bids, asks, update_id = body["bids"], body["asks"], body["lastUpdateId"]
        exch_ms, seq = body["E"], None
    else:
        if body["retCode"] != 0:
            raise ValueError(f"bybit {symbol}: {body['retCode']} {body['retMsg']}")
        r = body["result"]
        bids, asks, update_id = r["b"], r["a"], r["u"]
        exch_ms, seq = r["ts"], r["seq"]
    # Both exchanges return each side sorted from the best price outwards.
    rows = [
        (side, level, float(price), float(qty))
        for side, levels in (("bid", bids), ("ask", asks))
        for level, (price, qty) in enumerate(levels)
    ]
    side, level, price, qty = zip(*rows, strict=True)
    n = len(rows)
    return pa.table(
        {
            "exchange": [exchange] * n,
            "symbol": [symbol] * n,
            "local_ts": [local_ts] * n,
            "exch_ts": [exch_ms * 1_000_000] * n,
            "last_update_id": [update_id] * n,
            "seq": [seq] * n,
            "side": side,
            "level": level,
            "price": price,
            "qty": qty,
        },
        schema=SCHEMA,
    )


def write_atomic(table: pa.Table, path: Path) -> None:
    """Readers see either the whole file or none: write a temp file, fsync, rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "wb") as f:
            pq.write_table(table, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def snapshot_once(client: httpx.Client, data_dir: str, exchange: str, symbol: str) -> Path:
    resp = client.get(URLS[exchange].format(symbol=symbol))
    resp.raise_for_status()
    local_ts = time.time_ns()
    table = parse(exchange, symbol, local_ts, resp.json())
    day = datetime.fromtimestamp(local_ts / 1e9, UTC).date().isoformat()
    path = (
        Path(data_dir, "snapshots", f"exchange={exchange}", f"symbol={symbol}", f"date={day}")
        / f"{local_ts}.parquet"
    )
    write_atomic(table, path)
    return path


def run(cfg: dict) -> None:
    interval = cfg["snapshot_interval_min"] * 60
    with httpx.Client(timeout=10) as client:
        while True:
            # Wake on wall-clock boundaries (:00, :15, ...) so snapshot times are predictable.
            time.sleep(interval - time.time() % interval)
            for exchange in URLS:
                for symbol in cfg["symbols"]:
                    try:
                        path = snapshot_once(client, cfg["data_dir"], exchange, symbol)
                        log.info("snapshot %s", path)
                    except Exception:
                        # Keep the schedule alive; health reports a missed snapshot as stale.
                        log.exception("snapshot failed: %s %s", exchange, symbol)
