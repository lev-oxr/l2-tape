"""Reading the collector's raw files: `<local_ts ns> <raw JSON>` lines in a gzip file.

The collector appends one gzip member per run. A run that is killed (SIGKILL) leaves its
member cut short, and the next run's member follows right after it. Standard gzip readers
stop at that seam, or worse, decode the next member's bytes as more of the broken block.
So members are split by their header before decoding: the collector writes the same 10-byte
header for every member (a chance match in compressed data is ~2^-80 per position), and each
member is decoded on its own; a cut-short one just ends early.
"""

import json
import logging
import zlib
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger(__name__)

CHUNK = 4 << 20  # compressed bytes per read: ~35 MB inflated at a time
HEADER_LEN = 10


class GzTail:
    """Complete lines of a gzip file that may still be growing, like `tail -f`. A gzip stream
    cannot be entered in the middle: with from_start=False the existing content is inflated
    once without being returned, and only what is appended later comes out."""

    def __init__(self, path: Path, from_start: bool = True):
        self.path, self.offset, self.rest = path, 0, b""
        self.skip_until = 0 if from_start else path.stat().st_size
        self.header = b""  # the collector's member header, read from the file itself
        self.z: zlib._Decompress | None = None  # the member being decoded
        self.fed = False  # the member has had bytes beyond its header
        self.prev = b""  # last bytes already fed: a header may straddle two reads
        self.resyncs = 0  # members found cut short

    @property
    def in_member(self) -> bool:
        """Inside a member that has not ended (yet): still being written, or cut short."""
        return self.z is not None and self.fed and not self.z.eof

    def lines(self) -> Iterator[bytes]:
        with open(self.path, "rb") as f:
            if len(self.header) < HEADER_LEN:
                self.header = f.read(HEADER_LEN)
                if len(self.header) < HEADER_LEN:
                    return
            f.seek(self.offset)
            while True:
                skipping = self.offset < self.skip_until
                data = f.read(min(CHUNK, self.skip_until - self.offset) if skipping else CHUNK)
                if not data:
                    return
                self.offset += len(data)
                for seg in self.inflate(data):
                    if seg is None:  # a cut-short member's unfinished line is lost
                        self.rest = b""
                    elif skipping:  # keep only the unfinished last line
                        text = self.rest + seg
                        self.rest = text[text.rfind(b"\n") + 1 :]
                    else:
                        *new, self.rest = (self.rest + seg).split(b"\n")
                        yield from new

    def inflate(self, data: bytes) -> Iterator[bytes | None]:
        look = self.prev + data  # a header may begin in the bytes already fed
        k, pos, search = len(self.prev), 0, 0
        while (i := look.find(self.header, search)) >= 0:
            j = i - k  # where the header starts in `data`; < 0 if it straddles the reads
            if j > pos:
                yield from self.feed(data[pos:j])
            if self.in_member:  # the previous run was killed before closing its member
                self.resyncs += 1
                yield None
            self.z, self.fed = zlib.decompressobj(wbits=31), False
            yield from self.feed(self.header, header=True)
            pos, search = j + HEADER_LEN, i + 1
        if pos < len(data):
            yield from self.feed(data[max(pos, 0) :])
        self.prev = look[-(HEADER_LEN - 1) :]

    def feed(self, chunk: bytes, header: bool = False) -> Iterator[bytes | None]:
        if self.z is None or self.z.eof:
            return
        self.fed |= not header
        try:
            yield self.z.decompress(chunk)
        except zlib.error:  # corrupt member: skip to the next header
            self.resyncs += 1
            self.z = None
            yield None


def read_raw(path: Path) -> Iterator[tuple[int, dict]]:
    """(local_ts, message) per line."""
    tail = GzTail(path)
    for line in tail.lines():
        if line:
            ts, msg = line.split(b" ", 1)
            yield int(ts), json.loads(msg)
    if tail.resyncs:
        log.warning("%s: %d gzip member(s) cut short (collector killed)", path, tail.resyncs)
    if tail.rest or tail.in_member:
        log.warning("%s: truncated tail (collector killed, or still writing)", path)
