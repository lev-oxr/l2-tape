import argparse
import logging
import tomllib

from l2tape import health, snapshotter

COMMANDS = {"snapshot": snapshotter.run, "health": health.run}


def main() -> None:
    parser = argparse.ArgumentParser(prog="l2tape")
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument("--config", default="config.toml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with open(args.config, "rb") as f:
        cfg = tomllib.load(f)

    COMMANDS[args.command](cfg)
