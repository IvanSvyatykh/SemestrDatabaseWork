import abc
import datetime
from minio import Minio


from pathlib import Path
import pymongo
from typing import List
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame
from utils.spark_df_schemas import COLLECTIONS_SCHEMAS


class PySparkDataWorker(abc.ABC):

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


class MongoDbExtractractor(PySparkDataWorker):

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


class DataWorker:

    def __init__(
        self,
        temp_dir_path: Path,
        spark_extractor: PySparkDataWorker,
        minio_client: Minio,
    ):
        assert temp_dir_path.exists()
        self.__temp_dir_path = temp_dir_path
        self.spark_extractor = spark_extractor
        self.minio_client = minio_client

    @property
    def temp_dir_path(self) -> Path:
        return self.__temp_dir_path

    @temp_dir_path.setter
    def temp_dir_path(self, new_dir: Path):
        assert new_dir.exists()
        self.__temp_dir_path = new_dir

    def get_data_from_db(self) -> List[Path]:

        results_paths = []
        for collection in self.spark_extractor.db_objects:
            spark_df: DataFrame = self.spark_extractor.get_db_obj_df(
                collection, COLLECTIONS_SCHEMAS[collection]
            )
            path = self.__temp_dir_path / f"{collection}.csv.gz"
            spark_df.toPandas().to_csv(
                path, index=False, compression="gzip"
            )
            results_paths.append(Path(path))
        return results_paths

    def add_data_to_minio(
        self, paths_to_data: List[Path], bucket_name: str
    ) -> None:

        if not self.minio_client.bucket_exists(bucket_name):
            raise ValueError(
                f"Does not exist's bucket with name {bucket_name}"
            )

        current_date = datetime.date.today()

        for path in paths_to_data:
            assert path.exists()
            self.minio_client.fput_object(
                bucket_name=bucket_name,
                object_name=f"{current_date}/{path.name}",
                file_path=path,
                content_type="application/csv",
            )
            path.unlink()

    def get_data_from_minio(
        self, prefix: str, bucket_name: str
    ) -> List[Path]:

        if not self.minio_client.bucket_exists(bucket_name):
            raise ValueError(
                f"Does not exist's bucket with name {bucket_name}"
            )

        minio_files = self.minio_client.list_objects(
            bucket_name=bucket_name, prefix=prefix, recursive=True
        )

        for minio_file in minio_files:
            self.minio_client.fget_object(
                bucket_name=bucket_name,
                object_name=minio_file.object_name,
                file_path=self.__temp_dir_path
                / minio_file.object_name.split("/")[-1],
            )
