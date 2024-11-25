from pyspark import SparkConf
from staging_area.data_workers import MongoDbExtractractor, MinioDataLoader
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from config.config import (
    MONGODB_USERNAME,
    MONGODB_PASSWORD,
    MONGODB_PORT,
    MONGODB_DOMAIN,
)

extractor = MongoDbExtractractor(
    MONGODB_USERNAME,
    MONGODB_PASSWORD,
    MONGODB_DOMAIN,
    "airport",
    MONGODB_PORT,
)


extractor.start_spark_connection("ETL project", spark_ip="172.18.0.4")
aircrafts_df = extractor.get_df_from_collection("aircrafts")

aircrafts_df.show()
