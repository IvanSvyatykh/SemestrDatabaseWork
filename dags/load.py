import datetime
from pathlib import Path
from typing import Dict, List
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from minio import Minio
import sys

from utils.data_transformers import (
    PySparkDataTransformer,
    get_path_columns_dict,
)

sys.path.append("..")

from config import (
    MONGODB_USERNAME,
    MONGODB_PASSWORD,
    MONGODB_PORT,
    MONGODB_DOMAIN,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_ENDPOINT,
    CASSANDRA_DOMEN,
)
from utils.data_utils import DataWorker, MongoDbExtractractor

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}


@dag(
    dag_id="Load_data_to_DWH",
    default_args=default_args,
    start_date=datetime.datetime.today(),
    schedule="@daily",
    catchup=False,
)
def dag():

    spark_session_name = "ETL_load"
    load_temp_dir_path = Path("./data/spark_temp")
    bucket_name = "test"
    spark_ip = "spark-master"

    @task()
    def get_data_from_minio(
        data_worker: DataWorker, bucket_name: str, prefix: str
    ) -> List[str]:
        temp = data_worker.get_data_from_minio(prefix, bucket_name)
        return temp

    @task()
    def add_meta_data_to_csvgz(
        transformer: PySparkDataTransformer,
        paths_to_csvgz: Dict[Path, List[str]],
        etl_start_time: datetime,
        record_source: str,
    ) -> None:
        transformer.start_spark_session()
        for k, v in paths_to_csvgz.items():
            if "passengers" in k.name:
                transformer.trasform_passport_to_columns(k)
            if "flights" in k.name:
                transformer.transform_flight_info_to_columns(k)
            transformer.add_md5_hash_column(k, v)
            transformer.add_timestamp_column(k, etl_start_time)
            transformer.add_record_source(k, record_source)

    @task()
    def split_files_on_tables(
        transformer: PySparkDataTransformer, spark_dir: Path
    ) -> Dict[str, Path]:
        return transformer.transform_data(spark_dir)

    @task()
    def load_data_to_cassandra(
        data_worker: DataWorker, files: Dict[str, Path], cassandra_ip: str
    ) -> None:
        data_worker.add_data_to_cassandra(files, cassandra_ip)

    extractor = MongoDbExtractractor(
        db_username=MONGODB_USERNAME,
        db_password=MONGODB_PASSWORD,
        db_domain=MONGODB_DOMAIN,
        db_name="airport",
        db_port=MONGODB_PORT,
    )
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    load_temp_dir_path.mkdir(exist_ok=True)
    data_worker = DataWorker(
        temp_dir_path=load_temp_dir_path,
        spark_worker=extractor,
        minio_client=minio_client,
    )
    paths_to_files = get_data_from_minio(
        data_worker=data_worker,
        bucket_name=bucket_name,
        prefix=str(datetime.date.today()),
    )
    transformer = PySparkDataTransformer(
        spark_session_name=spark_session_name, spark_ip=spark_ip
    )
    columns_dict_path = get_path_columns_dict(paths_to_files)
    add_meta_data = add_meta_data_to_csvgz(
        transformer,
        columns_dict_path=columns_dict_path,
        etl_start_time=datetime.datetime.now(),
        record_source=extractor.mongodb_uri,
    )

    files = split_files_on_tables(transformer, load_temp_dir_path)
    add_data_to_cassandra = load_data_to_cassandra(
        data_worker=data_worker, files=files, cassandra_ip=CASSANDRA_DOMEN
    )
    [item.unlink() for item in load_temp_dir_path.iterdir()]

    (paths_to_files >> add_meta_data >> files >> add_data_to_cassandra)


dag = dag()
