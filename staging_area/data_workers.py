import pymongo
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf, SparkContext
from pandas import DataFrame as PandasDataFrame
from pyspark.sql.types import StructType


class MongoDbExtractractor:

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
        self.__db_collections = db.list_collection_names()

    @property
    def db_collections(self) -> List[str]:
        return self.__db_collections

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

    def get_df_from_collection(self, collection_name: str) -> DataFrame:
        return (
            self.spark.read.format("com.mongodb.spark.sql.DefaultSource")
            .option("collection", collection_name)
            .load()
        )


class MinioDataLoader:

    def __init__(self, minio_ip: str):
        self.minio_uri = f"http://{minio_ip}:9000"

    def start_spark_connection(
        self,
        spark_session_name: str,
        access_key: str,
        secret_key: str,
        spark_ip: str = "localhost",
    ) -> None:

        self.spark = (
            SparkSession.builder.appName("MinIO Example")
            .master(f"spark://172.18.0.7:7077")
            .config(
                "spark.jars.packages",
                "aws-java-sdk-1.7.4.jar,hadoop-aws-2.7.3",
            )
            .config("fs.s3a.access.key", "dFt5CwCHLkt2TFZljPE3")
            .config(
                "fs.s3a.secret.key",
                "EIuXhLFuaX8KQw3WLPMXm1NUEweyf8dTamURgYav",
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint", "http://172.18.0.3:9000"
            )
            .getOrCreate()
        )

    def get_spark_dataframe(
        self, dataframe: PandasDataFrame, schema: StructType
    ) -> DataFrame:
        dataframe["iata_name"] = dataframe["iata_name"].astype(str)
        dataframe["name"] = dataframe["name"].astype(str)
        dataframe["_id"] = dataframe["_id"].apply(lambda x: x["oid"])
        return self.spark.createDataFrame(dataframe, schema)

    def stop_spark_connection(self) -> None:
        self.spark.stop()

    def write_data_to_bucket(
        self, df: DataFrame, output_path: str, mode: str = "overwrite"
    ) -> None:
        df.write.parquet(output_path, mode=mode)
