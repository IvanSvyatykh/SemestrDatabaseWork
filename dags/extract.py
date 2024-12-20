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
    dag_id="Extract_data_from_MongoDB",
    description="Dag_for_extracting_data_from_MongoDB_and_load_to_MINIO",
    default_args=default_args,
    start_date=datetime.datetime.today(),
    schedule="@daily",
    catchup=False,
)
def dag():

    spark_session_name = "ETL_extract"
    temp_dir_path = Path("./data")
    load_temp_dir_path = Path("./data/spark_temp")
    bucket_name = "test"
    spark_ip = "spark-master"

    @task()
    def get_data_from_mongo(
        data_worker: DataWorker, spark_session_name: str, spark_ip: str
    ) -> List[str]:
        data_worker.start_extractor(
            spark_session_name=spark_session_name, spark_ip=spark_ip
        )
        return data_worker.get_data_from_db()

    @task()
    def load_data_to_minio(
        data_worker: DataWorker,
        paths_to_data: List[str],
        bucket_name: str,
    ):
        data_worker.add_data_to_minio(
            paths_to_data=paths_to_data, bucket_name=bucket_name
        )

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
    temp_dir_path.mkdir(exist_ok=True)
    load_temp_dir_path.mkdir(exist_ok=True)
    data_worker = DataWorker(
        temp_dir_path=temp_dir_path,
        spark_worker=extractor,
        minio_client=minio_client,
    )
    paths_to_data = get_data_from_mongo(
        data_worker=data_worker,
        spark_session_name=spark_session_name,
        spark_ip=spark_ip,
    )
    load_to_minio = load_data_to_minio(
        data_worker=data_worker,
        paths_to_data=paths_to_data,
        bucket_name=bucket_name,
    )
    (paths_to_data >> load_to_minio)


dag = dag()
