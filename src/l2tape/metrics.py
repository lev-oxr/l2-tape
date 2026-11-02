"""Recorder metrics for Prometheus, read from the tails of the collector's gzip files.

The collector keeps appending to its day file (one gzip member per collector run), so the
newest bytes can be inflated incrementally, like `tail -f`. A gzip stream cannot be entered
in the middle, though: a reader opened on an existing file inflates it from the start once,
without parsing, and counts only what arrives after that.
"""

import json
import statistics
import zlib
from dataclasses import dataclass, field
from pathlib import Path

CHUNK = 4 << 20  # compressed bytes per read: ~35 MB inflated at a time


class GzTail:
    def __init__(self, path: Path, from_start: bool):
        self.path, self.offset, self.rest = path, 0, b""
        self.skip_until = 0 if from_start else path.stat().st_size
        self.z = zlib.decompressobj(wbits=31)

    def lines(self) -> list[bytes]:
        """Complete lines appended since the last call (none while catching up)."""
        out: list[bytes] = []
        with open(self.path, "rb") as f:
            f.seek(self.offset)
            while True:
                skipping = self.offset < self.skip_until
                data = f.read(min(CHUNK, self.skip_until - self.offset) if skipping else CHUNK)
                if not data:
                    return out
                self.offset += len(data)
                text = self.rest + self.inflate(data)
                if skipping:  # keep only the unfinished last line
                    self.rest = text[text.rfind(b"\n") + 1 :]
                else:
                    *new, self.rest = text.split(b"\n")
                    out += new

    def inflate(self, data: bytes) -> bytes:
        parts = []
        while data:
            parts.append(self.z.decompress(data))
            data = b""
            if self.z.eof:  # end of a member: the next collector run starts a new one
                data, self.z = self.z.unused_data, zlib.decompressobj(wbits=31)
        return b"".join(parts)


@dataclass
class Stats:
    messages: int = 0
    resyncs: int = 0  # book snapshots: collector start, reconnect, exchange-side reset
    latency_ms: list[float] = field(default_factory=list)  # since the last exposition


def account(stats: Stats, exchange: str, line: bytes) -> None:
    """Same messages and times as `l2tape check`: Binance depthUpdate `E`, Bybit
    orderbook.1000 `ts`."""
    ts, raw = line.split(b" ", 1)
    m = json.loads(raw)
    stats.messages += 1
    exch_ms = None
    if exchange == "binancefutures":
        stats.resyncs += "lastUpdateId" in m
        if m.get("data", {}).get("e") == "depthUpdate":
            exch_ms = m["data"]["E"]
    elif m.get("topic", "").startswith("orderbook.1000."):
        stats.resyncs += m["type"] == "snapshot"
        exch_ms = m["ts"]
    if exch_ms is not None:
        stats.latency_ms.append(int(ts) / 1e6 - exch_ms)


def exposition(stats: dict[str, Stats], ages: dict[str, float], free_bytes: float) -> str:
    """Prometheus text format. Streams are "<exchange> <SYMBOL>"; latency quantiles cover the
    messages since the previous call, which then starts over."""
    out = [
        "# HELP l2tape_messages_total Raw messages written by the collector.",
        "# TYPE l2tape_messages_total counter",
        "# HELP l2tape_resyncs_total Book snapshots: collector start, reconnect, exchange reset.",
        "# TYPE l2tape_resyncs_total counter",
        "# HELP l2tape_last_message_age_seconds Time since the stream's file last grew.",
        "# TYPE l2tape_last_message_age_seconds gauge",
        "# HELP l2tape_latency_ms Local receive time minus exchange time, recent messages.",
        "# TYPE l2tape_latency_ms gauge",
        "# TYPE l2tape_disk_free_bytes gauge",
        f"l2tape_disk_free_bytes {free_bytes:.0f}",
    ]
    for stream in sorted(stats.keys() | ages.keys()):
        exchange, symbol = stream.split()
        labels = f'exchange="{exchange}",symbol="{symbol}"'
        if stream in ages:
            out.append(f"l2tape_last_message_age_seconds{{{labels}}} {ages[stream]:.1f}")
        if (s := stats.get(stream)) is None:
            continue
        out.append(f"l2tape_messages_total{{{labels}}} {s.messages}")
        out.append(f"l2tape_resyncs_total{{{labels}}} {s.resyncs}")
        if len(s.latency_ms) > 1:
            q = statistics.quantiles(s.latency_ms, n=100)
            for name, v in (("0.5", q[49]), ("0.99", q[98])):
                out.append(f'l2tape_latency_ms{{{labels},quantile="{name}"}} {v:.1f}')
        s.latency_ms.clear()
    return "\n".join(out) + "\n"
