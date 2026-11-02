"""Watchdog: a raw stream that stops growing, or a disk running out -> Telegram.

A stream is <exchange> <SYMBOL>; its age is the time since its newest file last changed
(mtime). The collector writes continuously, so 5 minutes without a change means the
recording stopped, whatever the reason.

Who watches the watchdog: every 5 minutes it pings an external dead man's switch
(HEALTHCHECK_URL, e.g. healthchecks.io), which alerts on its own when the pings stop —
the VPS is down, or this process is. Pings are sent only while every alert has been
delivered, so a broken Telegram setup is caught the same way.
"""

import logging
import os
import shutil
import time
from pathlib import Path

import httpx

from l2tape.snapshotter import URLS

log = logging.getLogger(__name__)

EXCHANGES = tuple(URLS)  # collector output dirs are named like the snapshotter's exchanges
STALL_S = 5 * 60
CHECK_EVERY_S = 30
PING_EVERY_S = 5 * 60


def utc(ts: float) -> str:
    return time.strftime("%H:%M:%S UTC", time.gmtime(ts))


def stream_ages(raw_dir: Path, now: float) -> dict[str, float]:
    """Seconds since each stream's newest file in raw/<exchange>/<symbol>_<date>.gz changed."""
    ages: dict[str, float] = {}
    for f in raw_dir.glob("*/*_*.gz"):
        stream = f"{f.parent.name} {f.name.rsplit('_', 1)[0].upper()}"
        ages[stream] = min(ages.get(stream, float("inf")), now - f.stat().st_mtime)
    return ages


def evaluate(
    state: dict, ages: dict[str, float], now: float, free_gb: float, min_free_gb: float
) -> list[str]:
    """One message per incident and one on recovery; `state` remembers open incidents."""
    msgs = []
    for stream, age in sorted(ages.items()):
        if age > STALL_S and stream not in state:
            state[stream] = now - age  # time of the last data
            msgs.append(
                f"🔴 l2-tape: {stream} — немає даних {age / 60:.0f} хв (останнє {utc(now - age)})"
            )
        elif age <= STALL_S and stream in state:
            down_min = (now - age - state.pop(stream)) / 60
            msgs.append(f"🟢 l2-tape: {stream} — запис відновлено, простій {down_min:.0f} хв")

    if free_gb < min_free_gb and "disk" not in state:
        state["disk"] = now
        msgs.append(f"🟠 l2-tape: диск — вільно {free_gb:.1f} ГБ (поріг {min_free_gb} ГБ)")
    elif free_gb >= min_free_gb and "disk" in state:
        del state["disk"]
        msgs.append(f"🟢 l2-tape: диск — вільно {free_gb:.1f} ГБ")
    return msgs


def no_url(e: Exception) -> str:
    """httpx puts the request URL into exception messages; both our URLs carry a secret."""
    status = e.response.status_code if isinstance(e, httpx.HTTPStatusError) else ""
    return f"{type(e).__name__} {status}".strip()


def send(client: httpx.Client, text: str) -> None:
    token, chat_id = os.environ["TELEGRAM_TOKEN"], os.environ["TELEGRAM_CHAT_ID"]
    resp = client.post(
        f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": text}
    )
    resp.raise_for_status()


def run(cfg: dict) -> None:
    # httpx logs every request URL at INFO, and the Telegram URL contains the bot token.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    data_dir = Path(cfg["data_dir"])
    expected = [f"{e} {s}" for e in EXCHANGES for s in cfg["symbols"]]
    started = time.time()
    state: dict = {}
    pending = ["ℹ️ l2-tape: health запущено"]
    ping_url, last_ping = os.environ.get("HEALTHCHECK_URL"), 0.0
    log.info("watchdog ping: %s", "every 5 min" if ping_url else "off, HEALTHCHECK_URL not set")
    with httpx.Client(timeout=10) as client:
        while True:
            now = time.time()
            # A stream with no file yet counts from our own start, so a fresh deploy gets
            # 5 minutes of grace instead of an instant false alarm.
            ages = {s: now - started for s in expected} | stream_ages(data_dir / "raw", now)
            free_gb = shutil.disk_usage(data_dir).free / 1e9
            pending += evaluate(state, ages, now, free_gb, cfg["disk_min_free_gb"])
            # Undelivered messages stay queued and are retried on the next check.
            while pending:
                try:
                    send(client, pending[0])
                except Exception as e:
                    log.warning("telegram send failed (%s), will retry", no_url(e))
                    break
                log.info("sent: %s", pending.pop(0))
            if ping_url and not pending and now - last_ping >= PING_EVERY_S:
                try:
                    client.get(ping_url).raise_for_status()
                    last_ping = now
                except Exception as e:
                    log.warning("watchdog ping failed (%s), will retry", no_url(e))
            time.sleep(CHECK_EVERY_S)
