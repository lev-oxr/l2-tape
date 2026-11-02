import argparse
import logging
import tomllib

from l2tape import check, convert, health, snapshotter

COMMANDS = {
    "snapshot": snapshotter.run,
    "health": health.run,
    "check": check.run,
    "convert": convert.run,
}


def main() -> None:
    parser = argparse.ArgumentParser(prog="l2tape")
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--date", help="check, convert: day, YYYY-MM-DD (default: yesterday UTC)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with open(args.config, "rb") as f:
        cfg = tomllib.load(f)
    cfg["date"] = args.date

    COMMANDS[args.command](cfg)
