"""Raw collector files -> normalized Parquet (data/events) and hftbacktest arrays (data/hbt).

events: one row per book level or trade, the same schema for both exchanges. `conn_id`
grows with every snapshot in the file (the collector's start or a reconnect): levels of an
older connection are stale and must not be mixed with a newer one. A day file starts
mid-stream with conn_id 0 and no snapshot (the collector writes none on day rotation).

hbt: hftbacktest's own converters, plus an initial snapshot built from our first REST
snapshot of the day, since the day file itself has none.
"""

import logging
import os
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from l2tape.check import load_ref, read_raw

log = logging.getLogger(__name__)

MS = 1_000_000  # exchange times are in ms, ours in ns
BATCH = 1_000_000

# (tick size, lot size); /fapi/v1/exchangeInfo and /v5/market/instruments-info, 2026-11-02
SPECS = {
    ("binancefutures", "BTCUSDT"): (0.1, 0.001),
    ("binancefutures", "ETHUSDT"): (0.01, 0.001),
    ("bybit", "BTCUSDT"): (0.1, 0.001),
    ("bybit", "ETHUSDT"): (0.01, 0.01),
}

COLUMNS = [
    ("local_ts", pa.int64()),
    ("exch_ts", pa.int64()),
    ("kind", pa.string()),  # snapshot | delta | trade
    ("side", pa.string()),  # bid | ask; for a trade, the aggressor: buy | sell
    ("price", pa.float64()),
    ("qty", pa.float64()),
    ("first_id", pa.int64()),  # Binance U
    ("last_id", pa.int64()),  # Binance u / snapshot lastUpdateId / trade id t; Bybit u
    ("prev_id", pa.int64()),  # Binance pu
    ("seq", pa.int64()),  # Bybit cross sequence
    ("conn_id", pa.int32()),
]
SCHEMA = pa.schema([("exchange", pa.string()), ("symbol", pa.string()), *COLUMNS])


def binance_rows(lines: Iterable[tuple[int, dict]]) -> Iterator[tuple]:
    """Exchange time is the transaction time `T`, as in hftbacktest's converter.
    `ids` = first_id, last_id, prev_id, seq."""
    conn = 0
    for ts, m in lines:
        if "lastUpdateId" in m:  # REST snapshot the collector fetched at start or on a gap
            conn += 1
            exch, ids = m["T"] * MS, (None, m["lastUpdateId"], None, None)
            for side, levels in (("bid", m["bids"]), ("ask", m["asks"])):
                for p, q in levels:
                    yield ts, exch, "snapshot", side, float(p), float(q), *ids, conn
            continue
        d = m.get("data", {})
        if d.get("e") == "depthUpdate":
            exch, ids = d["T"] * MS, (d["U"], d["u"], d["pu"], None)
            for side, levels in (("bid", d["b"]), ("ask", d["a"])):
                for p, q in levels:
                    yield ts, exch, "delta", side, float(p), float(q), *ids, conn
        # X "NA" trades carry price 0 and qty 0 (6607 a day on BTC, 2026-11-01); hftbacktest
        # also keeps MARKET only. m: the buyer is the maker, so the aggressor sold.
        elif d.get("e") == "trade" and d["X"] == "MARKET":
            side, ids = "sell" if d["m"] else "buy", (None, d["t"], None, None)
            yield ts, d["T"] * MS, "trade", side, float(d["p"]), float(d["q"]), *ids, conn


def bybit_rows(lines: Iterable[tuple[int, dict]]) -> Iterator[tuple]:
    """Only orderbook.1000 (orderbook.1 and .50 are its subsets) and trades. Exchange time
    is the message `ts` for the book (as in hftbacktest) and the trade time `T` for trades."""
    conn = 0
    for ts, m in lines:
        topic = m.get("topic", "")
        if topic.startswith("orderbook.1000."):
            d = m["data"]
            conn += m["type"] == "snapshot"
            exch, ids = m["ts"] * MS, (None, d["u"], None, d.get("seq"))
            for side, levels in (("bid", d["b"]), ("ask", d["a"])):
                for p, q in levels:
                    yield ts, exch, m["type"], side, float(p), float(q), *ids, conn
        elif topic.startswith("publicTrade."):
            for t in m["data"]:
                side, ids = t["S"].lower(), (None, None, None, t.get("seq"))
                yield ts, t["T"] * MS, "trade", side, float(t["p"]), float(t["v"]), *ids, conn


def write_events(rows: Iterable[tuple], exchange: str, symbol: str, path: Path) -> int:
    """Stream rows into one Parquet file in batches; written to .tmp, then renamed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp, n = path.with_suffix(".tmp"), 0
    try:
        with pq.ParquetWriter(tmp, SCHEMA) as w:
            batch: list[tuple] = []
            for row in rows:
                batch.append(row)
                if len(batch) == BATCH:
                    n += flush(w, batch, exchange, symbol)
            n += flush(w, batch, exchange, symbol)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return n


def flush(w: pq.ParquetWriter, batch: list[tuple], exchange: str, symbol: str) -> int:
    n = len(batch)
    if n:
        cols = [pa.array([exchange] * n), pa.array([symbol] * n)]
        cols += [
            pa.array(c, t) for c, (_, t) in zip(zip(*batch, strict=True), COLUMNS, strict=True)
        ]
        w.write_table(pa.Table.from_arrays(cols, schema=SCHEMA))
        batch.clear()
    return n


def initial_snapshot(ref: Path):
    """hftbacktest snapshot (as `create_last_snapshot` makes) from one REST snapshot."""
    import numpy as np
    from hftbacktest.types import (
        BUY_EVENT,
        DEPTH_SNAPSHOT_EVENT,
        EXCH_EVENT,
        LOCAL_EVENT,
        SELL_EVENT,
        event_dtype,
    )

    _, book = load_ref(ref)
    ev = EXCH_EVENT | LOCAL_EVENT | DEPTH_SNAPSHOT_EVENT
    rows = [(ev | BUY_EVENT, 0, 0, p, q, 0, 0, 0.0) for p, q in sorted(book.bids.items())[::-1]]
    rows += [(ev | SELL_EVENT, 0, 0, p, q, 0, 0, 0.0) for p, q in sorted(book.asks.items())]
    return np.array(rows, event_dtype)


def to_hbt(raw: Path, exchange: str, out: Path, rows: int) -> None:
    import numpy as np
    from hftbacktest.data.utils import binancefutures, bybit

    out.parent.mkdir(parents=True, exist_ok=True)
    # ponytail: the whole day in memory, ~12 GB peak measured for Binance BTC (64 B/event, doubled
    # by event-order correction); convert hour by hour if that stops fitting.
    size = rows + 1_000_000  # hftbacktest preallocates its buffer; we know the row count
    if exchange == "binancefutures":
        data = binancefutures.convert(str(raw), buffer_size=size)
    else:
        level = bybit.BybitDepthLevel.LEVEL_1000
        data = bybit.convert_depth(str(raw), buffer_size=size, single_depth_level=level)
    np.savez_compressed(out, data=data)


def run(cfg: dict) -> None:
    """Convert one recorded day (default: yesterday, UTC) for every exchange and symbol."""
    import numpy as np

    day = cfg.get("date") or (datetime.now(UTC) - timedelta(days=1)).date().isoformat()
    data_dir = Path(cfg["data_dir"])
    for exchange, rows_of in (("binancefutures", binance_rows), ("bybit", bybit_rows)):
        for symbol in cfg["symbols"]:
            raw = data_dir / "raw" / exchange / f"{symbol.lower()}_{day.replace('-', '')}.gz"
            part = f"exchange={exchange}/symbol={symbol}/date={day}"
            out = data_dir / "events" / part / "events.parquet"
            n = write_events(rows_of(read_raw(raw)), exchange, symbol, out)
            log.info("%s: %d rows, %.0f MB", out, n, out.stat().st_size / 1e6)

            hbt = data_dir / "hbt" / f"{exchange}_{symbol}_{day}.npz"
            to_hbt(raw, exchange, hbt, n)
            refs = sorted((data_dir / "snapshots" / part).glob("*.parquet"))
            if refs:  # day file has no snapshot: start the book from our first one
                snap = hbt.with_name(hbt.stem + "_snapshot.npz")
                np.savez_compressed(snap, data=initial_snapshot(refs[0]))
            log.info("%s: %.0f MB", hbt, hbt.stat().st_size / 1e6)
