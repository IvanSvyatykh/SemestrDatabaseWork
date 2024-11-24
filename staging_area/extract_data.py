import abc
from pathlib import Path
import pymongo
from typing import List
from pyspark.sql.types import StructType

import sys
from pyspark.sql import SparkSession, DataFrame
from spark_df_schemas import COLLECTIONS_SCHEMAS

sys.path.append("..")
from config.config import (
    MONGODB_USERNAME,
    MONGODB_PASSWORD,
    MONGODB_PORT,
    MONGODB_DOMAIN,
)


class PySparkDataExtractor(abc.ABC):

    @abc.abstractmethod
    def start_spark_connection(
        self, park_session_name: str, spark_ip: str = "localhost"
    ):
        pass

    @property
    @abc.abstractmethod
    def db_objects(self) -> List[str]:
        pass

    @abc.abstractmethod
    def stop_spark_connection(self):
        pass

    @abc.abstractmethod
    def get_db_obj_df(
        self, obj_name: str, obj_schema: StructType
    ) -> DataFrame:
        pass


class MongoDbExtractractor(PySparkDataExtractor):

    def __init__(
        self,
        db_username: str,
        db_password: str,
        db_domain: str,
        db_name: str,
        db_port: str = "27017",
    ):

        self.mongodb_uri = f"mongodb://{db_username}:{db_password}@{db_domain}:{db_port}/{db_name}?authSource=admin"
        client = pymongo.MongoClient(self.mongodb_uri)
        db = client[db_name]
        self.__db_objects = db.list_collection_names()

    @property
    def db_objects(self) -> List[str]:
        return self.__db_objects

    def start_spark_connection(
        self, spark_session_name: str, spark_ip: str = "localhost"
    ) -> None:
        self.spark: SparkSession = (
            SparkSession.builder.appName(spark_session_name)
            .master(f"spark://{spark_ip}:7077")
            .config("spark.mongodb.input.uri", self.mongodb_uri)
            .config(
                "spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
            )
            .getOrCreate()
        )

    def stop_spark_connection(self) -> None:
        self.spark.stop()

    def get_db_obj_df(
        self, obj_name: str, obj_schema: StructType
    ) -> DataFrame:
        return (
            self.spark.read.format("com.mongodb.spark.sql.DefaultSource")
            .schema(obj_schema)
            .option("collection", obj_name)
            .load()
        )


class DataExtractor:

    def __init__(
        self, temp_dir_path: str, spark_extractor: PySparkDataExtractor
    ):
        assert Path(temp_dir_path).exists()
        self.temp_dir_path = temp_dir_path
        self.spark_extractor = spark_extractor

    def get_data_from_db(self) -> List[Path]:

        results_paths = []
        for collection in self.spark_extractor.db_objects:
            spark_df: DataFrame = self.spark_extractor.get_db_obj_df(
                collection, COLLECTIONS_SCHEMAS[collection]
            )
            path = self.temp_dir_path + f"/{collection}.csv.gz"
            spark_df.toPandas().to_csv(
                path, index=False, compression="gzip"
            )
            results_paths.append(path)
        return results_paths
