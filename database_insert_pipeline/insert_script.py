import argparse
import logging


from dotenv import load_dotenv
import numpy as np
import yaml
from pathlib import Path
from tqdm import tqdm

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


def main(config_path: Path):
    load_dotenv()
    logger.info("Reading config")
    config = __read_config(config_path)
    logger.info("Convert str type to Path type")
    paths_keys = ["path", "result_dir"]
    __convert_str_to_path(config, paths_keys)
    logger.info("Start normalization")
    __prepare_data(config)


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
    main(args.config)
