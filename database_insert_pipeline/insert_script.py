import argparse
from logging import config


import yaml
from pathlib import Path


def read_config(config_path: Path) -> dict:
    with open(config_path, "r") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config


def main(config_path: Path):
    config = read_config(config_path)
    print(config["seat_classes"]["path"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help=True, required=True, type=Path
    )
    args = parser.parse_args()
    main(args.config)
