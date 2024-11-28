from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    IntegerType,
    MapType,
    BooleanType,
)

__aircraft_ids = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("aircraft_id", StringType(), False),
        StructField("aircraft_num", StringType(), False),
        StructField("airline_id", StringType(), False),
        StructField("registration_time", DateType(), False),
        StructField("deregistartion_time", DateType(), False),
    ]
)

__aircrafts = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("iata_name", StringType(), False),
        StructField("name", StringType(), False),
        StructField("seats_num", IntegerType(), False),
    ]
)

__airlines = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("icao_name", StringType(), False),
    ]
)

__airports = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("iata_name", StringType(), False),
        StructField("name", StringType(), False),
        StructField("city", StringType(), False),
        StructField("timezone", StringType(), False),
    ]
)

__flights = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("flight_number", StringType(), False),
        StructField("aircraft_id", StringType(), False),
        StructField("arrival_airport_id", StringType(), False),
        StructField("departure_airport_id", StringType(), False),
        StructField("schedule_id", StringType(), False),
        StructField("info", StringType(), False),
    ]
)

__passengers = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("surname", StringType(), False),
        StructField("passport", StringType(), False),
    ]
)

__schedules = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("arrival_time", DateType(), False),
        StructField("departure_time", DateType(), False),
        StructField("actual_arrival", DateType(), False),
        StructField("actual_departure", DateType(), False),
    ]
)

__seat_classes = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("fare_conditions", StringType(), False),
    ]
)

__statuses = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("status", StringType(), False),
    ]
)

__statuses_info = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("status_id", StringType(), False),
        StructField("schedule_id", StringType(), False),
        StructField("set_status_time", DateType(), False),
        StructField("unset_status_time", DateType(), False),
    ]
)

__tickets = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("passenger_id", StringType(), False),
        StructField("fare_conditions_id", StringType(), False),
        StructField("flight_id", StringType(), False),
        StructField("number", StringType(), False),
        StructField("cost", IntegerType(), False),
        StructField("baggage_weight", IntegerType(), False),
        StructField("is_registred", BooleanType(), False),
        StructField("seat_num", StringType(), False),
    ]
)

COLLECTIONS_SCHEMAS = {
    "aircraft_ids": __aircraft_ids,
    "aircrafts": __aircrafts,
    "airlines": __airlines,
    "airports": __airports,
    "flights": __flights,
    "passengers": __passengers,
    "schedules": __schedules,
    "seat_classes": __seat_classes,
    "statuses": __statuses,
    "statuses_info": __statuses_info,
    "tickets": __tickets,
}
