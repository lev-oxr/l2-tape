"""Order book heatmap data: the book sampled at a fixed step over a time window.

The book at the window start comes from data/events in one DuckDB query (the last update of
every price within the current connection); inside the window the updates are replayed
through `Book` and the book is sampled every `step`. A new conn_id means a new snapshot:
the book starts over, so levels of the old connection do not linger.
"""

import heapq
import math
from dataclasses import dataclass, field

import duckdb
import numpy as np

from l2tape.book import Book

STATE_SQL = """
WITH book AS (
    SELECT local_ts, conn_id, side, price, qty FROM read_parquet($events)
    WHERE kind IN ('snapshot', 'delta') AND local_ts <= $t0
)
SELECT side, price, arg_max(qty, local_ts) AS qty, any_value(conn_id) AS conn_id FROM book
WHERE conn_id = (SELECT max(conn_id) FROM book)
GROUP BY side, price
"""

WINDOW_SQL = """
SELECT local_ts, kind, side, price, qty, conn_id FROM read_parquet($events)
WHERE local_ts > $t0 AND local_ts <= $t1
ORDER BY local_ts
"""

RANGE_SQL = """
SELECT min(price), max(price) FROM read_parquet($events)
WHERE kind = 'trade' AND local_ts > $t0 AND local_ts <= $t1
"""


@dataclass
class Window:
    times: np.ndarray  # sample times, ns
    prices: np.ndarray  # lower edge of each price bin
    depth: np.ndarray  # [price bin, sample]: resting quantity, bids and asks together
    best_bid: np.ndarray
    best_ask: np.ndarray
    imbalance10: np.ndarray  # (bid - ask) / (bid + ask) over the top 10 levels per side
    cum_delta: np.ndarray  # aggressive buys minus sells since the window start
    trades: dict = field(default_factory=dict)  # local_ts, price, qty, side (+1 buy, -1 sell)


def window(
    events: str, t0: int, t1: int, step: int, tick: float, pad: float = 0.001, rows: int = 400
) -> Window:
    """Sample the book every `step` ns in (t0, t1]. Prices: the path of the price in the
    window (mid at t0 and every trade) ± pad, binned to whole ticks so that there are about
    `rows` bins (a screen has no more pixels anyway)."""
    params = {"events": events, "t0": t0}
    state = duckdb.sql(STATE_SQL, params=params).fetchall()
    book = Book(
        [(p, q) for s, p, q, _ in state if s == "bid" and q],
        [(p, q) for s, p, q, _ in state if s == "ask" and q],
    )
    if not (book.bids and book.asks):
        raise ValueError("no book at t0: pick a start after the recording began")
    conn = state[0][3]
    mid = (max(book.bids) + min(book.asks)) / 2
    [(low, high)] = duckdb.sql(RANGE_SQL, params=params | {"t1": t1}).fetchall()
    low, high = min(low or mid, mid) * (1 - pad), max(high or mid, mid) * (1 + pad)
    bin_size = max(1, math.ceil((high - low) / rows / tick)) * tick
    lo = math.floor(low / bin_size) * bin_size
    n_bins = math.ceil((high - lo) / bin_size) + 1

    times = np.arange(t0 + step, t1 + 1, step)
    depth = np.zeros((n_bins, len(times)))
    best_bid, best_ask, imbalance, cum_delta = (np.full(len(times), np.nan) for _ in range(4))
    trades: dict[str, list] = {"local_ts": [], "price": [], "qty": [], "side": []}
    delta, k = 0.0, 0

    # ponytail: each sample scans the whole book, O(levels): ~2 ms on Binance BTC, so keep
    # samples ~ screen width; bin incrementally on apply if finer steps are ever needed.
    def record() -> None:
        for side in (book.bids, book.asks):
            p = np.fromiter(side.keys(), float, len(side))
            q = np.fromiter(side.values(), float, len(side))
            idx = np.floor((p - lo) / bin_size + 1e-9).astype(int)
            inside = (idx >= 0) & (idx < n_bins)
            np.add.at(depth[:, k], idx[inside], q[inside])
        if book.bids and book.asks:
            bids, asks = (
                heapq.nlargest(10, book.bids.items()),
                heapq.nsmallest(10, book.asks.items()),
            )
            best_bid[k], best_ask[k] = bids[0][0], asks[0][0]
            b, a = sum(q for _, q in bids), sum(q for _, q in asks)
            imbalance[k] = (b - a) / (b + a)
        cum_delta[k] = delta

    rel = duckdb.sql(WINDOW_SQL, params=params | {"t1": t1}).fetchnumpy()
    columns = (rel[c].tolist() for c in ("local_ts", "kind", "side", "price", "qty", "conn_id"))
    for ts, kind, side, price, qty, c in zip(*columns, strict=True):
        while k < len(times) and ts > times[k]:
            record()
            k += 1
        if kind == "trade":
            sign = 1 if side == "buy" else -1
            delta += sign * qty
            for key, v in zip(trades, (ts, price, qty, sign), strict=True):
                trades[key].append(v)
            continue
        if c != conn:  # snapshot of a new connection: the old book is gone
            book, conn = Book(), c
        book.apply(*(([(price, qty)], []) if side == "bid" else ([], [(price, qty)])))
    while k < len(times):
        record()
        k += 1

    prices = lo + bin_size * np.arange(n_bins)
    arrays = {key: np.array(v) for key, v in trades.items()}
    return Window(times, prices, depth, best_bid, best_ask, imbalance, cum_delta, arrays)
