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
import statistics
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
    # (from, to): last depth message of a valid stretch -> next sync. Overstates each hole by
    # up to one message interval (Binance ~30 ms, Bybit ~200 ms).
    holes: list[tuple[int, int]] = field(default_factory=list)
    first_ts: int | None = None  # first depth message in the file
    last_valid: int | None = None  # last depth message while the book was valid
    latency: list[int] = field(default_factory=list)  # local_ts - exchange time, ns, per message
    lost_trades: int | None = None  # from gaps in trade ids, where the exchange numbers them

    def synced(self, ts: int) -> None:
        if self.last_valid is not None:
            self.holes.append((self.last_valid, ts))
        self.syncs.append(ts)
        self.last_valid = ts


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
    rep = Report(lost_trades=0)
    pending = sorted(refs)
    book: Book | None = None
    lo = hi = 0.0
    min_u = 0  # events ending before the sync snapshot are already in it
    last_u = last_t = None
    chain: list[dict] = []  # events since the last break, replayed once a snapshot arrives

    def sync(snap: Book, sid: int, ts: int) -> None:
        nonlocal book, lo, hi, min_u, chain
        book, lo, hi, min_u = snap, min(snap.bids), max(snap.asks), sid
        rep.synced(ts)
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
        if d.get("e") == "trade":  # trade ids are consecutive per symbol
            if last_t is not None:
                rep.lost_trades += d["t"] - last_t - 1
            last_t = d["t"]
            continue
        if d.get("e") != "depthUpdate":
            continue
        rep.events += 1
        if rep.first_ts is None:
            rep.first_ts = ts
        rep.latency.append(ts - d["E"] * 1_000_000)
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
            rep.last_valid = ts

    rep.unchecked += len(pending)
    return rep


def check_bybit(lines: Iterable[tuple[int, dict]], refs: dict[int, Book], depth=1000) -> Report:
    """Bybit linear: `u` of orderbook.<depth> goes +1 per message; a snapshot resets the book.

    REST `u` is on the orderbook.1000 counter, so references are compared exactly at the
    message with the same `u`. The stream keeps the top <depth> levels, as does REST.

    After a reconnect `u` may even step back (the new connection lands on a node that lags),
    so lost messages cannot be counted from `u`; holes are measured in time only. Trade ids
    are UUIDs, so lost trades are unknown.
    """
    rep = Report()
    pending = sorted(refs)
    book: Book | None = None
    last_u = None
    for ts, m in lines:
        if not m.get("topic", "").startswith(f"orderbook.{depth}."):
            continue
        rep.events += 1
        if rep.first_ts is None:
            rep.first_ts = ts
        rep.latency.append(ts - m["ts"] * 1_000_000)
        d = m["data"]
        if m["type"] == "snapshot":  # after (re)subscribe, or an exchange-side reset (u = 1)
            book = Book(d["b"], d["a"])
            rep.synced(ts)
        elif last_u is not None and d["u"] != last_u + 1:
            if book is not None:
                rep.gaps.append(ts)
            book = None  # deltas are useless until the next snapshot
        elif book is not None:
            book.apply(d["b"], d["a"])
            rep.last_valid = ts
        last_u = d["u"]
        while pending and pending[0] <= last_u:
            rid = pending.pop(0)
            if rid != last_u:
                rep.unchecked += 1
            elif book is None:  # day file starts mid-stream, or after a gap
                book = refs[rid].copy()
                rep.synced(ts)
            else:
                rep.checks.append((rid, *compare(book, refs[rid], min(book.bids), max(book.asks))))

    rep.unchecked += len(pending)
    return rep


def hms(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1e9, UTC).strftime("%H:%M:%S")


def day_holes(rep: Report, day_start: int, day_end: int) -> list[tuple[int, int]]:
    """Every stretch of the day the book was unknown, including before the first sync and
    after the last valid message. The span starts at the first message: before it the
    collector was not deployed yet, which is not a recording fault."""
    if rep.last_valid is None:
        return [(rep.first_ts or day_start, day_end)]
    return [(rep.first_ts, rep.syncs[0]), *rep.holes, (rep.last_valid, day_end)]


def summary(rep: Report, holes: list[tuple[int, int]], day_end: int, raw_mb: float) -> str:
    """One Markdown table row: stream quality for the day."""
    span = day_end - (rep.first_ts or day_end)
    lost = sum(b - a for a, b in holes)
    complete = f"{1 - lost / span:.4%}" if span else "—"
    q = [x / 1e6 for x in statistics.quantiles(rep.latency, n=100)] if len(rep.latency) > 1 else 0
    lat = f"{q[49]:.0f} / {q[98]:.0f} / {max(rep.latency) / 1e6:.0f}" if q else "—"
    # compared == 0: price moved beyond both books' depth in one interval, nothing to say
    levels = [c for _, c, _ in rep.checks if c]
    empty = len(rep.checks) - len(levels)
    bad = sum(1 for _, _, d in rep.checks if d)
    depth = f" ({min(levels)}–{max(levels)} lvl)" if levels else ""
    trades = "—" if rep.lost_trades is None else rep.lost_trades
    return (
        f"| {complete} | {sum(b - a > 1e9 for a, b in holes)} | {lost / 1e9:.1f} | {trades} "
        f"| {lat} | {len(levels) - bad} ok{depth}, {bad} differ, {empty} out of depth, "
        f"{rep.unchecked} unchecked | {raw_mb:.0f} |"
    )


def run(cfg: dict) -> None:
    """Quality report of one recorded day (default: yesterday, UTC) -> data/reports/<day>.md."""
    day = cfg.get("date") or (datetime.now(UTC) - timedelta(days=1)).date().isoformat()
    start = datetime.fromisoformat(day).replace(tzinfo=UTC)
    day_start, day_end = (int(t.timestamp()) * 10**9 for t in (start, start + timedelta(days=1)))
    rows, details = [], []
    for exchange, check in (("binancefutures", check_binance), ("bybit", check_bybit)):
        for symbol in cfg["symbols"]:
            raw = Path(
                cfg["data_dir"], "raw", exchange, f"{symbol.lower()}_{day.replace('-', '')}.gz"
            )
            log.info("checking %s", raw)
            rep = check(read_raw(raw), load_refs(cfg["data_dir"], exchange, symbol, day))
            holes = day_holes(rep, day_start, day_end)
            rows.append(
                f"| {exchange} {symbol} " + summary(rep, holes, day_end, raw.stat().st_size / 1e6)
            )
            details += [
                f"| {exchange} {symbol} | {hms(a)} | {(b - a) / 1e9:.1f} |"
                for a, b in holes
                if b - a > 1e9
            ]
    text = "\n".join(
        [
            f"# l2-tape quality, {day}",
            "",
            f"Latency = local time − exchange time (Binance `E`, Bybit `ts`), "
            f"± {cfg['clock_uncertainty_ms']} ms clock uncertainty (chrony bound).",
            "Complete = share of the day the rebuilt book was valid, from the first message.",
            "",
            "| Stream | Complete | Holes > 1 s | Lost, s | Lost trades "
            "| Latency p50 / p99 / max, ms | REST checks | Raw, MB |",
            "|---|---|---|---|---|---|---|---|",
            *rows,
            "",
            "## Holes > 1 s",
            "",
            "| Stream | From (UTC) | Duration, s |",
            "|---|---|---|",
            *details,
            "",
        ]
    )
    out = Path(cfg["data_dir"], "reports", f"{day}.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    print(text)
