"""Replay a collector file, detect gaps, and verify the rebuilt book against REST snapshots.

The book is valid from a sync (a snapshot plus the deltas after it) until the first break
in the update-id chain. While valid, every REST reference snapshot that falls inside the
replay is compared level by level with the rebuilt book at the same update id.

The collector writes no snapshot when it rotates to a new day file, so a day file starts
mid-stream: there (and after a gap) the first usable reference becomes the sync point.
"""

import gzip
import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from l2tape.book import Book

log = logging.getLogger(__name__)


@dataclass
class Report:
    events: int = 0  # depth messages seen
    gaps: list[int] = field(default_factory=list)  # local_ts of each break in the update ids
    syncs: list[int] = field(default_factory=list)  # local_ts when the book became valid
    checks: list[tuple[int, int, int]] = field(default_factory=list)  # (ref id, compared, differ)
    unchecked: int = 0  # references that fell while the book was not valid


def read_raw(path: Path) -> Iterator[tuple[int, dict]]:
    """(local_ts, message) per line; stops quietly at a truncated tail (collector was killed)."""
    with gzip.open(path, "rt") as f:
        try:
            for line in f:
                ts, msg = line.split(" ", 1)
                yield int(ts), json.loads(msg)
        except EOFError:
            log.warning("%s: truncated gzip tail, read up to it", path)


def load_ref(path: Path) -> tuple[int, Book]:
    """One snapshotter Parquet file -> (update id, book)."""
    t = pq.read_table(path, columns=["last_update_id", "side", "price", "qty"]).to_pydict()
    rows = list(zip(t["side"], t["price"], t["qty"], strict=True))
    book = Book(
        [(p, q) for s, p, q in rows if s == "bid"], [(p, q) for s, p, q in rows if s == "ask"]
    )
    return t["last_update_id"][0], book


def load_refs(data_dir: str, exchange: str, symbol: str, day: str) -> dict[int, Book]:
    """REST snapshots of one exchange, symbol and day (YYYY-MM-DD), keyed by update id."""
    base = Path(data_dir, "snapshots", f"exchange={exchange}", f"symbol={symbol}", f"date={day}")
    return dict(load_ref(f) for f in base.glob("*.parquet"))


def compare(book: Book, ref: Book, lo: float, hi: float, skip=frozenset()) -> tuple[int, int]:
    """(levels compared, levels that differ) for prices both books cover.

    [lo, hi] is the price range the rebuilt book is known in; it is narrowed to the
    reference's own depth, since levels beyond either depth are simply not measured.
    """
    lo, hi = max(lo, min(ref.bids)), min(hi, max(ref.asks))
    compared = differ = 0
    for mine, theirs in ((book.bids, ref.bids), (book.asks, ref.asks)):
        for p in mine.keys() | theirs.keys():
            if lo <= p <= hi and p not in skip:
                compared += 1
                differ += mine.get(p) != theirs.get(p)
    return compared, differ


def check_binance(lines: Iterable[tuple[int, dict]], refs: dict[int, Book]) -> Report:
    """Binance USDⓈ-M: chain `pu == previous u`; resync from the collector's REST snapshot.

    Update ids are global across symbols, so a reference id usually falls *between* two
    events of this symbol (book is exact there) or inside one event's [U, u] (then the
    levels that event touched are excluded: we cannot tell which of its updates came first).

    The book is only known within the sync snapshot's price range, and price drifts out of it
    within hours. So after each comparison the reference becomes the new base: every check
    covers one interval, reference t0 + deltas = reference t1.
    """
    rep = Report()
    pending = sorted(refs)
    book: Book | None = None
    lo = hi = 0.0
    min_u = 0  # events ending before the sync snapshot are already in it
    last_u = None
    chain: list[dict] = []  # events since the last break, replayed once a snapshot arrives

    def sync(snap: Book, sid: int, ts: int) -> None:
        nonlocal book, lo, hi, min_u, chain
        book, lo, hi, min_u = snap, min(snap.bids), max(snap.asks), sid
        rep.syncs.append(ts)
        for d in chain:
            apply(d)
        chain = []

    def rebase(rid: int) -> None:
        nonlocal book, lo, hi, min_u
        book, lo, hi, min_u = refs[rid].copy(), min(refs[rid].bids), max(refs[rid].asks), rid

    def apply(d: dict) -> None:
        if d["u"] < min_u:
            return
        while pending and pending[0] < d["U"]:
            rid = pending.pop(0)
            if rid >= min_u:
                rep.checks.append((rid, *compare(book, refs[rid], lo, hi)))
                rebase(rid)
            else:
                rep.unchecked += 1
        book.apply(d["b"], d["a"])
        touched = {float(p) for p, _ in d["b"] + d["a"]}
        while pending and pending[0] <= d["u"]:
            rid = pending.pop(0)
            rep.checks.append((rid, *compare(book, refs[rid], lo, hi, touched)))
            rebase(rid)
            book.apply(d["b"], d["a"])  # as on a sync: the straddling event goes on top

    for ts, m in lines:
        if "lastUpdateId" in m:  # REST snapshot the collector fetched at start or after a gap
            sid = m["lastUpdateId"]
            # Usable only if it is not older than the first update we are missing.
            if book is None and (not chain or sid >= chain[0]["pu"]):
                sync(Book(m["bids"], m["asks"]), sid, ts)
            continue
        d = m.get("data", {})
        if d.get("e") != "depthUpdate":
            continue
        rep.events += 1
        if last_u is not None and d["pu"] != last_u:
            rep.gaps.append(ts)
            book, chain = None, []
        last_u = d["u"]
        if book is None:
            chain.append(d)
            while pending and pending[0] < chain[0]["pu"]:  # older than our first update
                pending.pop(0)
                rep.unchecked += 1
            if pending and pending[0] <= d["u"]:
                rid = pending.pop(0)
                sync(refs[rid].copy(), rid, ts)
        else:
            apply(d)

    rep.unchecked += len(pending)
    return rep


def check_bybit(lines: Iterable[tuple[int, dict]], refs: dict[int, Book], depth=1000) -> Report:
    """Bybit linear: `u` of orderbook.<depth> goes +1 per message; a snapshot resets the book.

    REST `u` is on the orderbook.1000 counter, so references are compared exactly at the
    message with the same `u`. The stream keeps the top <depth> levels, as does REST.
    """
    rep = Report()
    pending = sorted(refs)
    book: Book | None = None
    last_u = None
    for ts, m in lines:
        if not m.get("topic", "").startswith(f"orderbook.{depth}."):
            continue
        rep.events += 1
        d = m["data"]
        if m["type"] == "snapshot":  # after (re)subscribe, or an exchange-side reset (u = 1)
            book = Book(d["b"], d["a"])
            rep.syncs.append(ts)
        elif last_u is not None and d["u"] != last_u + 1:
            if book is not None:
                rep.gaps.append(ts)
            book = None  # deltas are useless until the next snapshot
        elif book is not None:
            book.apply(d["b"], d["a"])
        last_u = d["u"]
        while pending and pending[0] <= last_u:
            rid = pending.pop(0)
            if rid != last_u:
                rep.unchecked += 1
            elif book is None:  # day file starts mid-stream, or after a gap
                book = refs[rid].copy()
                rep.syncs.append(ts)
            else:
                rep.checks.append((rid, *compare(book, refs[rid], min(book.bids), max(book.asks))))

    rep.unchecked += len(pending)
    return rep


def run(cfg: dict) -> None:
    """Quality of one recorded day (default: yesterday, UTC) for every exchange and symbol."""
    day = cfg.get("date") or (datetime.now(UTC) - timedelta(days=1)).date().isoformat()
    for exchange, check in (("binancefutures", check_binance), ("bybit", check_bybit)):
        for symbol in cfg["symbols"]:
            raw = Path(
                cfg["data_dir"], "raw", exchange, f"{symbol.lower()}_{day.replace('-', '')}.gz"
            )
            rep = check(read_raw(raw), load_refs(cfg["data_dir"], exchange, symbol, day))
            # compared == 0: price moved beyond both books' depth in one interval, nothing to say
            levels = [compared for _, compared, _ in rep.checks if compared] or [0]
            empty = len(rep.checks) - len(levels) if rep.checks else 0
            bad = sum(1 for _, _, differ in rep.checks if differ)
            hms = lambda tss: " ".join(  # noqa: E731
                datetime.fromtimestamp(t / 1e9, UTC).strftime("%H:%M:%S") for t in tss
            )
            print(
                f"{day} {exchange} {symbol}: {rep.events} events, {len(rep.syncs)} syncs, "
                f"{len(rep.gaps)} gaps; refs {len(rep.checks) - empty - bad} match "
                f"({min(levels)}-{max(levels)} levels), {bad} differ, {empty} out of depth, "
                f"{rep.unchecked} unchecked\n"
                f"  syncs {hms(rep.syncs)}; gaps {hms(rep.gaps)}"
            )
