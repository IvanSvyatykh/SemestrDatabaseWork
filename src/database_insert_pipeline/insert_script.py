import argparse
import asyncio
import logging
import sys


sys.path.append("..")

import numpy as np
import yaml
from pathlib import Path
from tqdm import tqdm

from database.db_config import DatabaseConfig
from load_data import LOAD_FUNCTIONS
from prepare_data import FUNCTION

logger = logging.getLogger(name="db insert")
logging.basicConfig(level=logging.INFO)


def __convert_str_to_path(config: dict, paths_key: list) -> None:
    keys = list(config.keys())
    for key in tqdm(keys, desc="Transform str to path"):
        for path in paths_key:
            config[key][path] = Path(config[key][path])


def __read_config(config_path: Path) -> dict:
    with open(config_path, "r") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config


def __prepare_data(config: dict) -> None:

    for data_name in tqdm(FUNCTION.keys(), desc="Files normalization"):
        if data_name in config:
            Path.mkdir(
                config[data_name]["result_dir"],
                parents=True,
                exist_ok=True,
            )
            FUNCTION[data_name](
                config[data_name]["path"], config[data_name]["result_dir"]
            )


async def __load_data(config: dict) -> None:
    db_config = DatabaseConfig()
    db_config.start_connection("airport")

    directory = Path(config["fare_condition"]["result_dir"])
    table_csv = {
        "airports": "airports.csv",
        "schedule": "schedules.csv",
        "status": "statuses.csv",
        "status_history": "status_history.csv",
        "airline": "airlines.csv",
        "aircraft": "aircrafts.csv",
        "aircraft_number": "aircraft_number.csv",
        "flight": "flights.csv",
        "fair_cond": "fare_condition.csv",
        "passenger": "passengers.csv",
        "ticket": "tickets.csv",
    }

    for key in tqdm(LOAD_FUNCTIONS.keys(), desc="Load data"):
        await LOAD_FUNCTIONS[key](directory / table_csv[key])


async def main(config_path: Path):
    logger.info("Reading config")
    config = __read_config(config_path)
    logger.info("Convert str type to Path type")
    paths_keys = ["path", "result_dir"]
    __convert_str_to_path(config, paths_keys)
    logger.info("Start normalization")
    __prepare_data(config)
    logger.info("Start loading data to database")
    await __load_data(config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help=True, required=True, type=Path
    )
    parser.add_argument(
        "-rs",
        "--random_seed",
        help=True,
        required=False,
        type=int,
        default=42,
    )
    args = parser.parse_args()
    np.random.seed(args.random_seed)
    asyncio.run(main(args.config))
