from pathlib import Path

import sys
from typing import List
from numpy import spacing
from pyspark.sql import DataFrame
from data_workers import MongoDbExtractractor
from spark_df_schemas import COLLECTIONS_SCHEMAS

sys.path.append("..")
from config.config import (
    MONGODB_USERNAME,
    MONGODB_PASSWORD,
    MONGODB_PORT,
    MONGODB_DOMAIN,
)


def get_oid_from_spark_struct(df: DataFrame) -> DataFrame:

    columns = df.columns

    for column in columns:
        print(df[column])


def get_data_from_db(
    mongo_db_data_extractor: MongoDbExtractractor, path_to_parent_dir: str
) -> List[Path]:
    assert Path(path_to_parent_dir).exists()
    results_paths = []
    for collection in mongo_db_data_extractor.db_collections:
        spark_df: DataFrame = (
            mongo_db_data_extractor.get_df_from_collection(
                collection, COLLECTIONS_SCHEMAS[collection]
            )
        )
        path = path_to_parent_dir + f"/{collection}.csv.gz"
        spark_df.toPandas().to_csv(path, index=False, compression="gzip")
        results_paths.append(path)


def main():
    mongo_db_data_extractor = MongoDbExtractractor(
        MONGODB_USERNAME,
        MONGODB_PASSWORD,
        MONGODB_DOMAIN,
        "airport",
        MONGODB_PORT,
    )
    mongo_db_data_extractor.start_spark_connection(
        "ETL project", spark_ip="172.18.0.4"
    )
    result_paths = get_data_from_db(
        mongo_db_data_extractor,
        "/home/ivan/ЧелГУ/БД/Курсовая/Семестровая по ХД/data/spark_data",
    )


if __name__ == "__main__":
    main()
