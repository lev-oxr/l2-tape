import os

import httpx

from l2tape import health
from l2tape.health import no_url

NOW = 1_793_400_000.0
BTC = "bybit BTCUSDT"


def test_stream_ages_uses_newest_file_per_stream(tmp_path):
    (tmp_path / "bybit").mkdir()
    old = tmp_path / "bybit" / "btcusdt_20261029.gz"
    new = tmp_path / "bybit" / "btcusdt_20261030.gz"
    for f, age in ((old, 86_400), (new, 10)):
        f.write_bytes(b"x")
        os.utime(f, (NOW - age, NOW - age))

    assert health.stream_ages(tmp_path, NOW) == {BTC: 10}


def test_stall_alerts_once_then_reports_recovery_with_downtime():
    state = {}
    first = health.evaluate(state, {BTC: 6 * 60}, NOW, 50, 15)
    assert len(first) == 1 and first[0].startswith("🔴") and BTC in first[0]

    # Still stalled a minute later: no repeat.
    assert health.evaluate(state, {BTC: 7 * 60}, NOW + 60, 50, 15) == []

    # Data again 10 min after the last one: downtime = 10 min.
    back = health.evaluate(state, {BTC: 0}, NOW + 4 * 60, 50, 15)
    assert len(back) == 1 and back[0].startswith("🟢") and "простій 10 хв" in back[0]
    assert state == {}


def test_healthy_stream_is_silent():
    assert health.evaluate({}, {BTC: 30}, NOW, 50, 15) == []


def test_disk_alerts_once_and_clears():
    state = {}
    assert health.evaluate(state, {}, NOW, 10, 15)[0].startswith("🟠")
    assert health.evaluate(state, {}, NOW + 30, 9, 15) == []
    assert health.evaluate(state, {}, NOW + 60, 20, 15)[0].startswith("🟢")


def test_errors_are_logged_without_the_secret_url():
    req = httpx.Request("POST", "https://api.telegram.org/botSECRET/sendMessage")
    err = httpx.HTTPStatusError(
        "for url .../botSECRET/...", request=req, response=httpx.Response(401, request=req)
    )
    assert no_url(err) == "HTTPStatusError 401"
    assert no_url(httpx.ConnectError("https://hc-ping.com/SECRET")) == "ConnectError"
