import sys
from pathlib import Path

import pyarrow.parquet as pq
from hftbacktest.types import BUY_EVENT, DEPTH_SNAPSHOT_EVENT, SELL_EVENT

from l2tape.book import Book
from l2tape.check import load_ref, read_raw
from l2tape.convert import binance_rows, bybit_rows, initial_snapshot, write_events

sys.path.insert(0, str(Path(__file__).parents[1] / "examples"))
from book_by_minute import book_by_minute  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
S = 10**9


def test_bybit_rows_rebuild_the_reference_exactly():
    # the fixture: one orderbook.1000 snapshot, then deltas up to the reference's u
    book = Book()
    for _, _, kind, side, price, qty, *_ in bybit_rows(
        read_raw(FIXTURES / "check_bybit_btcusdt.gz")
    ):
        if kind != "trade":
            book.apply(*(([(price, qty)], []) if side == "bid" else ([], [(price, qty)])))
    _, ref = load_ref(FIXTURES / "check_bybit_btcusdt_ref.parquet")
    assert book.bids == ref.bids and book.asks == ref.asks


def test_binance_trade_side_is_the_aggressor_and_ids_are_kept():
    def depth(T, U, u, pu, b):
        return {"data": {"e": "depthUpdate", "T": T, "U": U, "u": u, "pu": pu, "b": b, "a": []}}

    lines = [
        (
            1,
            {"data": {"e": "trade", "T": 5, "t": 42, "p": "1", "q": "2", "m": True, "X": "MARKET"}},
        ),
        (1, {"data": {"e": "trade", "T": 5, "t": 43, "p": "0", "q": "0", "m": True, "X": "NA"}}),
        (2, {"lastUpdateId": 9, "T": 6, "bids": [["1", "1"]], "asks": []}),
        (3, depth(7, 8, 10, 7, [])),
        (4, depth(8, 11, 12, 10, [["2", "0"]])),
    ]
    rows = list(binance_rows(lines))
    assert rows[0] == (1, 5 * 10**6, "trade", "sell", 1.0, 2.0, None, 42, None, None, 0)
    assert rows[1][2:] == ("snapshot", "bid", 1.0, 1.0, None, 9, None, None, 1)  # conn_id 1
    assert rows[2] == (4, 8 * 10**6, "delta", "bid", 2.0, 0.0, 11, 12, 10, None, 1)


def test_events_file_roundtrip(tmp_path):
    rows = list(bybit_rows(read_raw(FIXTURES / "check_bybit_btcusdt.gz")))
    out = tmp_path / "e.parquet"
    assert write_events(iter(rows), "bybit", "BTCUSDT", out) == len(rows)
    t = pq.read_table(out)
    assert t.num_rows == len(rows) and set(t["symbol"].to_pylist()) == {"BTCUSDT"}
    assert not list(tmp_path.glob("*.tmp"))


def test_initial_snapshot_is_ordered_like_hftbacktest_makes_it():
    snap = initial_snapshot(FIXTURES / "check_bybit_btcusdt_ref.parquet")
    bids = snap[(snap["ev"] & BUY_EVENT) != 0]
    asks = snap[(snap["ev"] & SELL_EVENT) != 0]
    assert len(bids) == len(asks) == 1000
    assert all(snap["ev"] & DEPTH_SNAPSHOT_EVENT == DEPTH_SNAPSHOT_EVENT)
    assert list(bids["px"]) == sorted(bids["px"], reverse=True)  # best first
    assert list(asks["px"]) == sorted(asks["px"])


def test_book_by_minute_drops_levels_of_an_older_connection(tmp_path):
    def row(t, kind, side, price, qty, conn):
        return t * S, t * S, kind, side, price, qty, None, None, None, None, conn

    rows = [
        row(1, "snapshot", "bid", 99.0, 1.0, 1),
        row(1, "snapshot", "bid", 98.0, 3.0, 1),  # gone by the reconnect: must not linger
        row(1, "snapshot", "ask", 101.0, 1.0, 1),
        row(30, "delta", "bid", 100.0, 2.0, 1),
        row(90, "snapshot", "bid", 99.5, 1.0, 2),  # reconnect: this is the whole new book
        row(90, "snapshot", "ask", 100.5, 3.0, 2),
        row(125, "delta", "ask", 100.5, 3.0, 2),  # recording goes past the second minute end
    ]
    events = tmp_path / "events"
    write_events(iter(rows), "x", "Y", events / "e.parquet")
    got = book_by_minute(str(events)).fetchall()
    # minute 1: bids 100 (2) + 99 (1) + 98 (3), ask 101 (1); minute 2: new book only
    assert [(spread, round(imb, 3)) for _, spread, imb in got] == [(1.0, 0.714), (1.0, -0.5)]
