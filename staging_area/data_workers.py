from pyspark.sql import SparkSession, DataFrame


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
        self.spark = (
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
