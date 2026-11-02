import logging
import random
import zlib
from pathlib import Path

from l2tape.raw import GzTail, read_raw


def append(path: Path, z, text: bytes, finish: bool = False) -> None:
    """Write like the collector: deflate output as it comes, one gzip member per run."""
    with open(path, "ab") as f:
        f.write(z.compress(text) + (z.flush() if finish else z.flush(zlib.Z_SYNC_FLUSH)))


def test_tail_reads_a_growing_multi_member_gzip(tmp_path):
    f = tmp_path / "btcusdt_20261102.gz"
    run1 = zlib.compressobj(wbits=31)
    append(f, run1, b"1 old\n2 ol")
    tail = GzTail(f, from_start=False)  # health starts now: what is there is not counted
    assert list(tail.lines()) == []

    append(f, run1, b"d\n3 new\n4 half")
    assert list(tail.lines()) == [b"2 old", b"3 new"]  # a line that straddles the start counts
    append(f, run1, b" line\n", finish=True)  # collector stops: member ends

    run2 = zlib.compressobj(wbits=31)  # collector restarts: a new member is appended
    append(f, run2, b"5 next run\n")
    assert list(tail.lines()) == [b"4 half line", b"5 next run"]
    assert list(tail.lines()) == []


def test_a_member_cut_short_keeps_what_it_holds_and_the_next_run(tmp_path, caplog):
    f = tmp_path / "bybit.gz"
    killed = zlib.compressobj(wbits=31)
    append(f, killed, b'1 {"a": 1}\n2 {"a": 2}\n3 {"a": 3')  # flushed so far
    # SIGKILL: what deflate had emitted is on disk, a valid prefix cut mid-block, no trailer
    noise = random.Random(1).randbytes(100_000).hex().encode()  # incompressible, so emitted
    with open(f, "ab") as out:
        out.write(killed.compress(b'}\n4 {"a": ' + noise)[:20_000])
    append(f, zlib.compressobj(wbits=31), b'5 {"a": 5}\n6 {"a": 6}\n', finish=True)

    with caplog.at_level(logging.WARNING):
        got = [m["a"] for _, m in read_raw(f)]
    assert got == [1, 2, 3, 5, 6]  # line 4 was unfinished when the run was killed
    assert "cut short" in caplog.text and "truncated tail" not in caplog.text


def test_a_complete_file_reads_without_warnings(tmp_path, caplog):
    f = tmp_path / "x.gz"
    append(f, zlib.compressobj(wbits=31), b'1 {"a": 1}\n', finish=True)
    append(f, zlib.compressobj(wbits=31), b'2 {"a": 2}\n', finish=True)
    with caplog.at_level(logging.WARNING):
        assert [ts for ts, _ in read_raw(f)] == [1, 2]
    assert caplog.text == ""
