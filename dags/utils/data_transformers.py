from pathlib import Path
from typing import List, Dict
from pyspark.sql.functions import md5, concat_ws
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame
from utils.spark_df_schemas import COLLECTIONS_SCHEMAS


__BUSINESS_KEY_COLUMNS_NAME = {
    "aircraft_ids": ["aircraft_num"],
    "aircrafts": ["iata_name", "name"],
    "airlines": ["name", "icao_name"],
    "airports": ["iata_name", "name"],
    "flights": ["flight_number"],
    "passengers": ["passport"],
    "schedules": ["_id"],
    "seat_classes": ["fare_conditions"],
    "statuses": ["status"],
    "statuses_info": ["_id", "schedule_id"],
    "tickets": ["number"],
}


def get_path_columns_dict(
    paths_to_files: List[Path],
) -> Dict[Path, List[str]]:
    result = {}

    for path in paths_to_files:

        file_name = path.stem.split(".")[0]
        if file_name in __BUSINESS_KEY_COLUMNS_NAME:
            result[path] = __BUSINESS_KEY_COLUMNS_NAME[file_name]
    return result


class PySparkDataTransformer:
    def __init__(
        self,
        spark_session_name: str,
        spark_ip: str = "localhost",
    ):
        self.__spark_session_name = spark_session_name
        self.__spark_ip = spark_ip

    def start_spark_session(
        self,
    ) -> None:
        self.__spark: SparkSession = (
            SparkSession.builder.appName(self.__spark_session_name)
            # .master(f"spark://{self.__spark_ip}:7077")
            .getOrCreate()
        )

    def add_md5_hash_column(self, file_path: Path, columns: List[str]):
        assert file_path.exists()
        schema: StructType = COLLECTIONS_SCHEMAS[
            file_path.stem.split(".")[0]
        ]
        df: DataFrame = self.__spark.read.csv(
            str(file_path), schema, sep=",", header=True
        )
        df_with_hash = df.withColumn(
            "hash_key", md5(concat_ws("", *columns))
        )
        df_with_hash.toPandas().to_csv(
            file_path, index=False, compression="gzip"
        )
