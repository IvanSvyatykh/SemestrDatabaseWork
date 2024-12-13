from datetime import datetime
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict
from pyspark.sql.functions import md5, concat_ws
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    TimestampType,
)


FILES_TRANSFORMER = {
    "aircraft_ids": ["aircraft_num"],
    "aircrafts": ["iata_name", "name"],
    "airlines": ["name", "icao_name"],
    "airports": ["iata_name", "name"],
    "flights": ["flight_number", "schedule_id"],
    "passengers": ["series", "number"],
    "schedules": ["_id"],
    "seat_classes": ["fare_conditions"],
    "statuses": ["status"],
    "statuses_info": ["_id", "schedule_id"],
    "tickets": ["number"],
}

TABLES_COLUMNS = {
    "aircrafts_hub": [
        "aircrafts_hash_key",
        "iata_name",
        "load_date",
        "record_source",
    ],
    "aircrafts_sat": [
        "aircrafts_hash_key",
        "name",
        "load_date",
        "record_source",
        "seats_num",
    ],
    "aircrafts_link": [
        "load_date",
        "record_source",
        "aircrafts_hash_key",
        "flights_hash_key",
    ],
    "airlines_hub": [
        "load_date",
        "record_source",
        "airlines_hash_key",
        "icao_name",
        "name",
    ],
    "aircraft_nums_link": [
        "load_date",
        "record_source",
        "aircraft_nums_hash_key",
        "aircraft_hash_key",
        "airlines_hash_key",
        "aircraft_num",
    ],
    "aircraft_nums_sat": [
        "load_date",
        "record_source",
        "aircraft_nums_hash_key",
        "registration_time",
        "deregistartion_time",
    ],
    "tickets_sat": [
        "load_date",
        "record_source",
        "tickets_hash_key",
        "cost",
        "baggage_weight",
        "is_registred",
        "seat_num",
    ],
    "tickets_link": [
        "load_date",
        "record_source",
        "tickets_hash_key",
        "flights_hash_key",
    ],
    "seat_classes_hub": [
        "load_date",
        "record_source",
        "seat_class_hash_key",
        "fare_conditions",
    ],
    "seat_classes_link": [
        "load_date",
        "record_source",
        "seat_class_hash_key",
        "tickets_hash_key",
    ],
    "passengers_link": [
        "passengers_hash_key",
        "load_date",
        "record_source",
        "tickets_hash_key",
    ],
    "passengers_sat": [
        "passengers_hash_key",
        "load_date",
        "record_source",
        "name",
        "surname",
    ],
    "passengers_hub": [
        "passengers_hash_key",
        "load_date",
        "record_source",
        "series",
        "number",
    ],
    "airports_sat": [
        "airport_hash_key",
        "load_date",
        "record_source",
        "name",
        "city",
        "timezone",
    ],
    "airports_hub": [
        "airport_hash_key",
        "load_date",
        "record_source",
        "iata_name",
    ],
    "airports_link": [
        "load_date",
        "record_source",
        "flights_hash_key",
        "arrival_airport_hash_key",
        "departure_airport_hash_key",
    ],
    "flights_hub": [
        "load_date",
        "record_source",
        "flights_hash_key",
    ],
    "passenger_flights_sat": [
        "load_date",
        "record_source",
        "flights_hash_key",
        "registration_time",
        "is_ramp",
        "gate",
        "flight_number",
    ],
    "schedules_sat": [
        "load_date",
        "record_source",
        "schedules_hash_key",
        "arrival_time",
        "departure_time",
        "actual_arrival",
        "actual_departure",
    ],
    "schedules_hub": [
        "load_date",
        "record_source",
        "schedules_hash_key",
    ],
    "schedules_link": [
        "load_date",
        "record_source",
        "schedules_hash_key",
        "flights_hash_key",
    ],
    "statuses_hub": [
        "load_date",
        "record_source",
        "status_hash_key",
        "status",
    ],
    "status_infos_sat": [
        "load_date",
        "record_source",
        "status_info_hash_key",
        "set_status_time",
        "unset_status_time",
    ],
    "statuses_info_link": [
        "load_date",
        "record_source",
        "status_info_hash_key",
        "status_hash_key",
        "schedules_hash_key",
    ],
}


def get_path_columns_dict(
    paths_to_files: List[Path],
) -> Dict[Path, List[str]]:
    result = {}

    for path in paths_to_files:

        file_name = path.stem.split(".")[0]
        if file_name in FILES_TRANSFORMER:
            result[path] = FILES_TRANSFORMER[file_name]
    return result


class AbstractCsvTableTransformer(ABC):

    @abstractmethod
    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        pass


class AircraftsTransformer(AbstractCsvTableTransformer):

    def __init__(self, spark_dir: Path):
        assert spark_dir.exists()
        self.__aircraft_file = spark_dir / "aircrafts.csv.gz"
        self.__flight_file = spark_dir / "flights.csv.gz"

    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        aircraft_df: DataFrame = spark_session.read.csv(
            str(self.__aircraft_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "aircrafts_hash_key")
        aircrafts_hub_path = (
            self.__aircraft_file.parent / "aircrafts_hub.csv"
        )
        aircrafts_sat_path = (
            self.__aircraft_file.parent / "aircrafts_sat.csv"
        )
        aircrafts_link_path = (
            self.__aircraft_file.parent / "aircrafts_link.csv"
        )
        aircraft_df[TABLES_COLUMNS["aircrafts_hub"]].toPandas().to_csv(
            aircrafts_hub_path, index=False
        )
        aircraft_df[TABLES_COLUMNS["aircrafts_sat"]].toPandas().to_csv(
            aircrafts_sat_path, index=False
        )
        flight_df: DataFrame = spark_session.read.csv(
            str(self.__flight_file), sep=",", header=True
        ).withColumnsRenamed(
            {
                "hash_key": "flights_hash_key",
                "load_date": "flights_load_date",
                "record_source": "flights_record_source",
            }
        )
        flight_df.join(
            aircraft_df,
            on=[flight_df.aircraft_id == aircraft_df._id],
            how="inner",
        )[TABLES_COLUMNS["aircrafts_link"]].toPandas().to_csv(
            aircrafts_link_path, index=False
        )
        return {
            "aircrafts_hub_path": aircrafts_hub_path,
            "aircrafts_sat_path": aircrafts_sat_path,
            "aircrafts_link_path": aircrafts_link_path,
        }


class AirlinesTransformer(AbstractCsvTableTransformer):
    def __init__(self, spark_dir: Path):
        assert spark_dir.exists()
        self.__airlines_file = spark_dir / "airlines.csv.gz"

    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        airlines_df: DataFrame = spark_session.read.csv(
            str(self.__airlines_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "airlines_hash_key")
        airlines_hub_path = (
            self.__airlines_file.parent / "airlines_hub.csv"
        )
        airlines_df[TABLES_COLUMNS["airlines_hub"]].toPandas().to_csv(
            airlines_hub_path, index=False
        )
        return {"airlines_hub_path": airlines_hub_path}


class AircraftNumsTransformer(AbstractCsvTableTransformer):
    def __init__(self, spark_dir: Path):
        assert spark_dir.exists()
        self.__aircraft_nums_file = spark_dir / "aircraft_ids.csv.gz"
        self.__aircraft_file = spark_dir / "aircrafts.csv.gz"
        self.__airlines_file = spark_dir / "airlines.csv.gz"

    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        aircraft_df: DataFrame = spark_session.read.csv(
            str(self.__aircraft_file), sep=",", header=True
        ).withColumnsRenamed(
            {
                "hash_key": "aircraft_hash_key",
                "load_date": "aircraft_load_date",
                "record_source": "aircraft_record_source",
            }
        )
        airlines_df: DataFrame = spark_session.read.csv(
            str(self.__airlines_file), sep=",", header=True
        ).withColumnsRenamed(
            {
                "hash_key": "airlines_hash_key",
                "load_date": "airlines_load_date",
                "record_source": "airlines_record_source",
            }
        )
        aircraft_nums_df: DataFrame = spark_session.read.csv(
            str(self.__aircraft_nums_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "aircraft_nums_hash_key")
        aircraft_nums_sat_path = (
            self.__aircraft_file.parent / "aircraft_nums_sat.csv"
        )
        aircraft_nums_link_path = (
            self.__aircraft_file.parent / "aircraft_nums_link.csv"
        )
        aircraft_nums_df[
            TABLES_COLUMNS["aircraft_nums_sat"]
        ].toPandas().to_csv(aircraft_nums_sat_path, index=False)
        aircraft_df.join(
            aircraft_nums_df,
            on=[aircraft_df._id == aircraft_nums_df.aircraft_id],
            how="inner",
        ).join(
            airlines_df,
            on=[aircraft_nums_df.airline_id == airlines_df._id],
            how="inner",
        )[
            TABLES_COLUMNS["aircraft_nums_link"]
        ].toPandas().to_csv(
            aircraft_nums_link_path, index=False
        )
        return {
            "aircraft_nums_sat_path": aircraft_nums_sat_path,
            "aircraft_nums_link_path": aircraft_nums_link_path,
        }


class TicketTransformer(AbstractCsvTableTransformer):
    def __init__(self, spark_dir: Path):
        assert spark_dir.exists()
        self.__ticket_file = spark_dir / "tickets.csv.gz"
        self.__flights_file = spark_dir / "flights.csv.gz"

    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        tickets_df: DataFrame = spark_session.read.csv(
            str(self.__ticket_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "tickets_hash_key")
        flights_df: DataFrame = spark_session.read.csv(
            str(self.__flights_file), sep=",", header=True
        ).withColumnsRenamed(
            {
                "hash_key": "flights_hash_key",
                "load_date": "flights_load_date",
                "record_source": "flights_record_source",
            }
        )
        tickets_sat_path = self.__ticket_file.parent / "tickets_sat.csv"
        tickets_hub_path = self.__ticket_file.parent / "tickets_hub.csv"
        tickets_link_path = self.__ticket_file.parent / "tickets_link.csv"
        tickets_df[TABLES_COLUMNS["tickets_sat"]].toPandas().to_csv(
            tickets_sat_path, index=False
        )
        tickets_df[TABLES_COLUMNS["tickets_sat"]].toPandas().to_csv(
            tickets_hub_path, index=False
        )
        flights_df.join(
            tickets_df,
            on=[flights_df._id == tickets_df.flight_id],
            how="inner",
        )[TABLES_COLUMNS["tickets_link"]].toPandas().to_csv(
            tickets_link_path, index=False
        )
        return {
            "tickets_sat_path": tickets_sat_path,
            "tickets_hub_path": tickets_hub_path,
            "tickets_link_path": tickets_link_path,
        }


class FareCondTransformer(AbstractCsvTableTransformer):
    def __init__(self, spark_dir: Path):
        assert spark_dir.exists()
        self.__ticket_file = spark_dir / "tickets.csv.gz"
        self.__seat_classes_file = spark_dir / "seat_classes.csv.gz"

    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        seat_classes_df: DataFrame = spark_session.read.csv(
            str(self.__seat_classes_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "seat_class_hash_key")
        tickets_df: DataFrame = spark_session.read.csv(
            str(self.__ticket_file), sep=",", header=True
        ).withColumnsRenamed(
            {
                "hash_key": "tickets_hash_key",
                "load_date": "tickets_load_date",
                "record_source": "tickets_record_source",
            }
        )
        seat_classes_hub_path = (
            self.__seat_classes_file.parent / "seat_classes_hub.csv"
        )
        seat_classes_link_path = (
            self.__seat_classes_file.parent / "seat_classes_link.csv"
        )
        seat_classes_df[
            TABLES_COLUMNS["seat_classes_hub"]
        ].toPandas().to_csv(seat_classes_hub_path, index=False)
        tickets_df.join(
            seat_classes_df,
            on=[tickets_df.fare_conditions_id == seat_classes_df._id],
            how="inner",
        )[TABLES_COLUMNS["seat_classes_link"]].toPandas().to_csv(
            seat_classes_link_path, index=False
        )
        return {
            "seat_classes_hub_path": seat_classes_hub_path,
            "seat_classes_link_path": seat_classes_link_path,
        }


class PassengerTransformer(AbstractCsvTableTransformer):
    def __init__(self, spark_dir: Path):
        assert spark_dir.exists()
        self.__ticket_file = spark_dir / "tickets.csv.gz"
        self.__passenger_file = spark_dir / "passengers.csv.gz"

    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        passenger_df: DataFrame = spark_session.read.csv(
            str(self.__passenger_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "passengers_hash_key")
        tickets_df: DataFrame = spark_session.read.csv(
            str(self.__ticket_file), sep=",", header=True
        ).withColumnsRenamed(
            {
                "hash_key": "tickets_hash_key",
                "load_date": "tickets_load_date",
                "record_source": "tickets_record_source",
            }
        )
        passengers_hub_path = (
            self.__passenger_file.parent / "passengers_hub.csv"
        )
        passengers_link_path = (
            self.__passenger_file.parent / "passengers_link.csv"
        )
        passengers_sat_path = (
            self.__passenger_file.parent / "passengers_sat.csv"
        )
        passenger_df[TABLES_COLUMNS["passengers_hub"]].toPandas().to_csv(
            passengers_hub_path, index=False
        )
        passenger_df[TABLES_COLUMNS["passengers_sat"]].toPandas().to_csv(
            passengers_sat_path, index=False
        )
        tickets_df.join(
            passenger_df,
            on=[passenger_df._id == tickets_df.passenger_id],
            how="inner",
        )[TABLES_COLUMNS["passengers_link"]].toPandas().to_csv(
            passengers_link_path, index=False
        )
        return {
            "passengers_sat_path": passengers_sat_path,
            "passengers_link_path": passengers_link_path,
            "passengers_hub_path": passengers_hub_path,
        }


class AirportTransformer(AbstractCsvTableTransformer):
    def __init__(self, spark_dir: Path):
        assert spark_dir.exists()
        self.__flights_file = spark_dir / "flights.csv.gz"
        self.__airport_file = spark_dir / "airports.csv.gz"

    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        airport_df: DataFrame = spark_session.read.csv(
            str(self.__airport_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "airport_hash_key")

        flights_df: DataFrame = spark_session.read.csv(
            str(self.__flights_file), sep=",", header=True
        ).withColumnsRenamed(
            {
                "hash_key": "flights_hash_key",
                "load_date": "load_date",
                "record_source": "record_source",
            }
        )
        airports_hub_path = self.__airport_file.parent / "airports_hub.csv"
        airports_link_path = (
            self.__airport_file.parent / "airports_link.csv"
        )
        airports_sat_path = self.__airport_file.parent / "airports_sat.csv"
        airport_df[TABLES_COLUMNS["airports_sat"]].toPandas().to_csv(
            airports_sat_path, index=False
        )
        airport_df[TABLES_COLUMNS["airports_hub"]].toPandas().to_csv(
            airports_hub_path, index=False
        )
        flights_df.join(
            airport_df.select(col("_id"), col("airport_hash_key")),
            on=[flights_df.arrival_airport_id == airport_df._id],
            how="inner",
        ).withColumnRenamed(
            "airport_hash_key", "arrival_airport_hash_key"
        ).drop(
            "_id"
        ).join(
            airport_df.select(col("_id"), col("airport_hash_key")),
            on=[flights_df.departure_airport_id == airport_df._id],
            how="inner",
        ).withColumnRenamed(
            "airport_hash_key", "departure_airport_hash_key"
        )[
            TABLES_COLUMNS["airports_link"]
        ].toPandas().to_csv(
            airports_link_path, index=False
        )
        return {
            "airports_hub_path": airports_hub_path,
            "airports_link_path": airports_link_path,
            "airports_sat_path": airports_sat_path,
        }


class FlightsTransformer(AbstractCsvTableTransformer):
    def __init__(self, spark_dir: Path):
        assert spark_dir.exists()
        self.__flights_file = spark_dir / "flights.csv.gz"

    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        flights_df: DataFrame = spark_session.read.csv(
            str(self.__flights_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "flights_hash_key")

        flights_hub_path = self.__flights_file.parent / "flights_hub.csv"
        flights_sat_path = self.__flights_file.parent / "flights_sat.csv"
        flights_df[TABLES_COLUMNS["flights_hub"]].toPandas().to_csv(
            flights_hub_path, index=False
        )
        flights_df[
            TABLES_COLUMNS["passenger_flights_sat"]
        ].toPandas().to_csv(flights_sat_path, index=False)
        return {
            "flights_hub_path": flights_hub_path,
            "lights_sat_path": flights_sat_path,
        }


class SchedulesTransformer(AbstractCsvTableTransformer):
    def __init__(self, spark_dir: Path):
        assert spark_dir.exists()
        self.__flights_file = spark_dir / "flights.csv.gz"
        self.__schedules_file = spark_dir / "schedules.csv.gz"

    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        schedules_df: DataFrame = spark_session.read.csv(
            str(self.__schedules_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "schedules_hash_key")
        flights_df: DataFrame = spark_session.read.csv(
            str(self.__flights_file), sep=",", header=True
        ).withColumnsRenamed(
            {
                "hash_key": "flights_hash_key",
                "load_date": "flights_load_date",
                "record_source": "flights_record_source",
            }
        )
        schedules_sat_path = (
            self.__flights_file.parent / "schedules_sat.csv"
        )
        schedules_link_path = (
            self.__flights_file.parent / "schedules_link.csv"
        )
        schedules_hub_path = (
            self.__flights_file.parent / "schedules_hub.csv"
        )
        schedules_df[TABLES_COLUMNS["schedules_sat"]].toPandas().to_csv(
            schedules_sat_path, index=False
        )
        schedules_df[TABLES_COLUMNS["schedules_hub"]].toPandas().to_csv(
            schedules_hub_path, index=False
        )
        flights_df.join(
            schedules_df,
            on=[schedules_df._id == flights_df.schedule_id],
            how="inner",
        )[TABLES_COLUMNS["schedules_sat"]].toPandas().to_csv(
            schedules_sat_path, index=False
        )

        return {
            "schedules_sat_path": schedules_sat_path,
            "schedules_link_path": schedules_link_path,
            "schedules_hub_path": schedules_hub_path,
        }


class StatusInfoTransformer(AbstractCsvTableTransformer):
    def __init__(self, spark_dir: Path):
        assert spark_dir.exists()
        self.__status_info_file = spark_dir / "statuses_info.csv.gz"
        self.__schedules_file = spark_dir / "schedules.csv.gz"
        self.__status_file = spark_dir / "statuses.csv.gz"

    def transform(self, spark_session: SparkSession) -> Dict[str, Path]:
        schedules_df: DataFrame = spark_session.read.csv(
            str(self.__schedules_file), sep=",", header=True
        ).withColumnsRenamed(
            {
                "hash_key": "schedules_hash_key",
                "load_date": "schedules_load_date",
                "record_source": "schedules_record_source",
            }
        )
        status_df: DataFrame = spark_session.read.csv(
            str(self.__status_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "status_hash_key")
        status_info_df: DataFrame = spark_session.read.csv(
            str(self.__status_info_file), sep=",", header=True
        ).withColumnRenamed("hash_key", "status_info_hash_key")

        status_info_sat_path = (
            self.__status_info_file.parent / "status_info_sat.csv"
        )
        status_info_link_path = (
            self.__status_info_file.parent / "status_info_link.csv"
        )
        status_info_hub_path = (
            self.__status_info_file.parent / "status_info_hub.csv"
        )
        status_df[TABLES_COLUMNS["statuses_hub"]].toPandas().to_csv(
            status_info_hub_path, index=False
        )
        status_info_df[
            TABLES_COLUMNS["status_infos_sat"]
        ].toPandas().to_csv(status_info_sat_path, index=False)
        status_info_df.join(
            schedules_df.select(col("_id"), col("schedules_hash_key")),
            on=[schedules_df._id == status_info_df.schedule_id],
            how="inner",
        ).join(
            status_df.select(col("_id"), col("status_hash_key")),
            on=[status_df._id == status_info_df.status_id],
            how="inner",
        )[
            TABLES_COLUMNS["statuses_info_link"]
        ].toPandas().to_csv(
            status_info_link_path, index=False
        )
        return {
            "status_info_sat_path": status_info_sat_path,
            "status_info_link_path": status_info_link_path,
            "status_info_hub_path": status_info_hub_path,
        }


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
        self.__spark_session: SparkSession = (
            SparkSession.builder.appName(self.__spark_session_name)
            # .master(f"spark://{self.__spark_ip}:7077")
            .getOrCreate()
        )

    def transform_flight_info_to_columns(self, file_path: Path) -> None:
        assert file_path.exists()
        df: DataFrame = self.__spark_session.read.csv(
            str(file_path), sep=",", header=True, escape='"'
        )
        schema = StructType(
            [
                StructField("gate", StringType(), False),
                StructField("is_ramp", BooleanType(), False),
                StructField("registration_time", TimestampType(), False),
            ]
        )
        passenger_flight_df = df.withColumn(
            "jsonData",
            from_json(col("info"), schema),
        ).select(col("_id"), "jsonData.*")
        df = passenger_flight_df.join(df, on=["_id"], how="inner")
        df = df.drop("info")
        df.toPandas().to_csv(file_path, index=False)

    def trasform_passport_to_columns(self, file_path: Path) -> None:
        assert file_path.exists()
        df: DataFrame = self.__spark_session.read.csv(
            str(file_path), sep=",", header=True, escape='"'
        )
        schema = StructType(
            [
                StructField("series", StringType(), True),
                StructField("number", StringType(), True),
            ]
        )
        passport_df = df.withColumn(
            "jsonData",
            from_json(col("passport"), schema),
        ).select(col("_id"), "jsonData.*")
        df = passport_df.join(df, on=["_id"], how="inner")
        df = df.drop("passport")
        df.toPandas().to_csv(file_path, index=False)

    def add_md5_hash_column(self, file_path: Path, columns: List[str]):
        assert file_path.exists()
        df: DataFrame = self.__spark_session.read.csv(
            str(file_path), sep=",", header=True
        )
        df_with_hash = df.withColumn(
            "hash_key", md5(concat_ws("", *columns))
        )
        df_with_hash.toPandas().to_csv(
            file_path, index=False, compression="gzip"
        )

    def add_timestamp_column(self, file_path: Path, etl_time: datetime):
        assert file_path.exists()
        df: DataFrame = self.__spark_session.read.csv(
            str(file_path), sep=",", header=True
        )
        df.withColumn("load_date", lit(etl_time)).toPandas().to_csv(
            file_path, index=False, compression="gzip"
        )

    def add_record_source(self, file_path: Path, source: str):
        assert file_path.exists()
        df: DataFrame = self.__spark_session.read.csv(
            str(file_path), sep=",", header=True
        )
        df.withColumn("record_source", lit(source)).toPandas().to_csv(
            file_path, index=False, compression="gzip"
        )

    def transform_data(self, spark_dir: Path) -> Dict[str, Path]:
        assert spark_dir.exists()
        res = {}
        for transformer in AbstractCsvTableTransformer.__subclasses__():
            res = {
                **transformer(spark_dir).transform(self.__spark_session),
                **res,
            }
        return res
