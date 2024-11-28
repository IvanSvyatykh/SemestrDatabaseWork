import datetime
from pathlib import Path
from typing import List
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from minio import Minio
import sys

sys.path.append("..")

from config import (
    MONGODB_USERNAME,
    MONGODB_PASSWORD,
    MONGODB_PORT,
    MONGODB_DOMAIN,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_ENDPOINT,
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
def extract_dag():

    spark_session_name = ("ETL_pipeline",)
    temp_dir_path = ("data/spark_temp",)
    bucket_name = ("test",)
    spark_ip = ("172.18.0.5",)

    @task(multiple_outputs=True)
    def start_spark_session(
        spark_session_name: str, spark_ip: str, db_name: str
    ) -> MongoDbExtractractor:

        extractor = MongoDbExtractractor(
            db_username=MONGODB_USERNAME,
            db_password=MONGODB_PASSWORD,
            db_domain=MONGODB_DOMAIN,
            db_name=db_name,
            db_port=MONGODB_PORT,
        )
        extractor.start_spark_connection(
            spark_session_name=spark_session_name, spark_ip=spark_ip
        )

        return extractor

    @task(multiple_outputs=True)
    def create_data_worker(
        extractor: MongoDbExtractractor,
        minio_client: Minio,
        temp_dir_path: Path,
    ) -> DataWorker:
        temp_dir_path.mkdir(exist_ok=True)
        data_worker = DataWorker(
            temp_dir_path=temp_dir_path,
            spark_extractor=extractor,
            minio_client=minio_client,
        )
        return data_worker

    @task(multiple_outputs=True)
    def get_data_from_mongo(data_worker: DataWorker) -> List[Path]:
        return data_worker.get_data_from_db()

    @task(multiple_outputs=True)
    def load_data_to_minio(
        data_worker: DataWorker,
        paths_to_data: List[Path],
        bucket_name: str,
    ):
        data_worker.add_data_to_minio(
            paths_to_data=paths_to_data, bucket_name=bucket_name
        )

    extractor = start_spark_session(
        spark_session_name=spark_session_name,
        spark_ip=spark_ip,
        db_name="airport",
    )
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    data_worker = create_data_worker(
        extractor=extractor,
        minio_client=minio_client,
        temp_dir_path=temp_dir_path,
    )
    paths_to_data = get_data_from_mongo(data_worker=data_worker)
    load_to_minio = load_data_to_minio(
        data_worker=data_worker,
        paths_to_data=paths_to_data,
        bucket_name=bucket_name,
    )
    extractor >> data_worker >> paths_to_data >> load_to_minio


dag = extract_dag()
