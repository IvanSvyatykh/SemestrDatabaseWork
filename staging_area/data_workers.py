from pyspark.sql import SparkSession, DataFrame
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

    def start_spark_connection(
        self, spark_ip: str, spark_session_name: str
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
        spark_ip: str,
        spark_session_name: str,
        access_key: str,
        secret_key: str,
    ) -> None:
        self.spark: SparkSession = (
            SparkSession.builder.appName(spark_session_name)
            .master(f"spark://{spark_ip}:7077")
            .config("fs.s3a.endpoint", self.minio_uri)
            .config(
                "fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config("fs.s3a.access.key", access_key)
            .config("fs.s3a.secret.key", secret_key)
            .getOrCreate()
        )

    def get_spark_dataframe(
        self, dataframe: PandasDataFrame, schema: StructType
    ) -> DataFrame:
        return self.spark.createDataFrame(dataframe, schema)

    def stop_spark_connection(self) -> None:
        self.spark.stop()

    def write_data_to_bucket(df: DataFrame, path: str, mode: str) -> None:
        df.write.format("parquet").mode(mode).save(path)
