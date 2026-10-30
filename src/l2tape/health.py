"""Watchdog: a raw stream that stops growing, or a disk running out -> Telegram.

A stream is <exchange> <SYMBOL>; its age is the time since its newest file last changed
(mtime). The collector writes continuously, so 5 minutes without a change means the
recording stopped, whatever the reason.
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
                except Exception:
                    log.exception("telegram send failed, will retry")
                    break
                log.info("sent: %s", pending.pop(0))
            time.sleep(CHECK_EVERY_S)
