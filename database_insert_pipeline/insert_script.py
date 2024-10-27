import argparse
import logging


import yaml
from pathlib import Path
from tqdm import tqdm

from normalize_data import FUNCTION

logger = logging.getLogger(name="db insert")
logging.basicConfig(level=logging.INFO)


def read_config(config_path: Path) -> dict:
    with open(config_path, "r") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config


def normalize_data(config: dict) -> None:

    for data_name in tqdm(FUNCTION.keys, desc="Files normalized"):
        if data_name in config:
            FUNCTION[data_name](config[data_name]["path"])
            logger.info(f"{data_name} data was normalized.")
        else:
            logger.info(f"{data_name} data skiped normalization.")


def main(config_path: Path):
    logger.info("Reading config")
    config = read_config(config_path)
    logger.info("Start normalization")
    normalize_data(config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help=True, required=True, type=Path
    )
    args = parser.parse_args()
    main(args.config)
