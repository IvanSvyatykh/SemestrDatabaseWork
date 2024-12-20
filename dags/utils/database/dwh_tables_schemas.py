from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd


class StatusHub:

    def __init__(self, row: pd.Series):

        self.__status_hash = row["load_date"]
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__status = row["status"]

    @property
    def status_hash(self) -> str:
        return self.__status_hash

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def status(self) -> str:
        return self.__status


class SchedulesHub:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__schedules_hash = row["schedules_hash_key"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def schedules_hash(self) -> str:
        return self.__schedules_hash


class FlightsHub:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__flights_hash = row["flights_hash_key"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def flights_hash(self) -> str:
        return self.__flights_hash


class AirportsHub:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__airports_hash = row["airport_hash_key"]
        self.__iata_name = row["iata_name"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def airports_hash(self) -> str:
        return self.__airports_hash

    @property
    def iata_name(self) -> str:
        return self.__iata_name


class AircraftsHub:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__aircrafts_hash = row["aircrafts_hash_key"]
        self.__iata_name = row["iata_name"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def aircrafts_hash(self) -> str:
        return self.__aircrafts_hash

    @property
    def iata_name(self) -> str:
        return self.__iata_name


class TicketsHub:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__ticket_hash = row["record_source"]
        self.__number = str(row["number"])

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def ticket_hash(self) -> str:
        return self.__ticket_hash

    @property
    def number(self) -> str:
        return self.__number


class PassengersHub:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__passenger_hash = row["passengers_hash_key"]
        self.__passport_number = "0" * (6 - len(str(row["number"]))) + str(
            row["number"]
        )

        self.__passport_series = "0" * (4 - len(str(row["series"]))) + str(
            row["series"]
        )

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def passenger_hash(self) -> str:
        return self.__passenger_hash

    @property
    def passport_number(self) -> str:
        return self.__passport_number

    @property
    def passport_series(self) -> str:
        return self.__passport_series


class AirlinesHub:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__airline_hash = row["airlines_hash_key"]
        self.__icao_name = row["icao_name"]
        self.__name = row["name"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def airline_hash(self) -> str:
        return self.__airline_hash

    @property
    def icao_name(self) -> str:
        return self.__icao_name

    @property
    def name(self) -> str:
        return self.__name


class SeatClassesHub:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__seat_class_hash = row["seat_class_hash_key"]
        self.__seat_class = row["fare_conditions"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def seat_class_hash(self) -> str:
        return self.__seat_class_hash

    @property
    def seat_class(self) -> str:
        return self.__seat_class


class StatusesInfosLink:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__status_hash = row["status_hash_key"]
        self.__schedules_hash = row["schedules_hash_key"]
        self.__status_info_hash = row["status_info_hash_key"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def status_hash(self) -> str:
        return self.__status_hash

    @property
    def schedules_hash(self) -> str:
        return self.__schedules_hash

    @property
    def status_info_hash(self) -> str:
        return self.__status_info_hash


class SchedulesLink:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__flight_hash = row["flights_hash_key"]
        self.__schedules_hash = row["schedules_hash_key"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def flight_hash(self) -> str:
        return self.__flight_hash

    @property
    def schedules_hash(self) -> str:
        return self.__schedules_hash


class AirportsLink:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__flight_hash = row["flights_hash_key"]
        self.__arrival_airport_hash = row["arrival_airport_hash_key"]
        self.__departure_airport_hash = row["departure_airport_hash_key"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def flight_hash(self) -> str:
        return self.__flight_hash

    @property
    def arrival_airport_hash(self) -> str:
        return self.__arrival_airport_hash

    @property
    def departure_airport_hash(self) -> str:
        return self.__departure_airport_hash


class AircraftNumsLink:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__airline_hash = row["airlines_hash_key"]
        self.__aircraft_hash = row["aircraft_hash_key"]
        self.__aircraft_num_hash = row["aircraft_nums_hash_key"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def airline_hash(self) -> str:
        return self.__airline_hash

    @property
    def aircraft_hash(self) -> str:
        return self.__aircraft_hash

    @property
    def aircraft_num_hash(self) -> str:
        return self.__aircraft_num_hash


class AircraftsLink:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__aircraft_hash = row["aircrafts_hash_key"]
        self.__flight_hash = row["flights_hash_key"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def aircraft_hash(self) -> str:
        return self.__aircraft_hash

    @property
    def flight_hash(self) -> str:
        return self.__flight_hash


class TicketsLink:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__ticket_hash = row["tickets_hash_key"]
        self.__flight_hash = row["flights_hash_key"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def ticket_hash(self) -> str:
        return self.__ticket_hash

    @property
    def flight_hash(self) -> str:
        return self.__flight_hash


class SeatClassesLink:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__ticket_hash = row["tickets_hash_key"]
        self.__seat_class_hash = row["seat_class_hash_key"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def ticket_hash(self) -> str:
        return self.__ticket_hash

    @property
    def seat_class_hash(self) -> str:
        return self.__seat_class_hash


class PassengersLink:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__ticket_hash = row["tickets_hash_key"]
        self.__passenger_hash = row["passengers_hash_key"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def ticket_hash(self) -> str:
        return self.__ticket_hash

    @property
    def passenger_hash(self) -> str:
        return self.__passenger_hash


class StatusInfosSat:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__status_info_hash = row["status_info_hash_key"]
        self.__set_status_time = row["set_status_time"]
        self.__unset_status_time = row["unset_status_time"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def status_info_hash(self) -> str:
        return self.__status_info_hash

    @property
    def set_status_time(self) -> str:
        return self.__set_status_time

    @property
    def unset_status_time(self) -> str:
        return self.__unset_status_time


class SchedulesSat:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__schedules_hash = row["schedules_hash_key"]
        self.__actual_arrival_time = row["actual_arrival"]
        self.__actual_departure_time = row["actual_departure"]
        self.__planned_arrival_time = row["arrival_time"]
        self.__planned_departure_time = row["departure_time"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def schedules_hash(self) -> str:
        return self.__schedules_hash

    @property
    def actual_arrival_time(self) -> datetime:
        return self.__actual_arrival_time

    @property
    def actual_departure_time(self) -> datetime:
        return self.__actual_departure_time

    @property
    def planned_arrival_time(self) -> datetime:
        return self.__planned_arrival_time

    @property
    def planned_departure_time(self) -> datetime:
        return self.__planned_departure_time


class CargoFlightsSat:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__flight_hash = row["flights_hash_key"]
        self.__weight = row["weight"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def flight_hash(self) -> str:
        return self.__flight_hash

    @property
    def weight(self) -> int:
        return self.__weight


class PassengerFlightsSat:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__flight_hash = row["flights_hash_key"]
        self.__registration_time = row["registration_time"]
        self.__is_ramp = row["is_ramp"]
        self.__gate = row["gate"]
        self.__flight_number = row["flight_number"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def flight_hash(self) -> str:
        return self.__flight_hash

    @property
    def registration_time(self) -> datetime:
        return self.__registration_time

    @property
    def is_ramp(self) -> bool:
        return self.__is_ramp

    @property
    def gate(self) -> str:
        return self.__gate

    @property
    def flight_number(self) -> str:
        return self.__flight_number


class AirportsSat:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__airport_hash = row["airport_hash_key"]
        self.__name = row["name"]
        self.__city = row["city"]
        self.__timezone = row["timezone"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def airport_hash(self) -> str:
        return self.__airport_hash

    @property
    def name(self) -> str:
        return self.__name

    @property
    def city(self) -> str:
        return self.__city

    @property
    def timezone(self) -> str:
        return self.__timezone


class AircraftsSat:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__aircraft_hash = row["aircrafts_hash_key"]
        self.__name = row["name"]
        self.__seats_num = row["seats_num"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def aircraft_hash(self) -> str:
        return self.__aircraft_hash

    @property
    def name(self) -> str:
        return self.__name

    @property
    def seats_num(self) -> str:
        return self.__seats_num


class AircraftNumsSat:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__aircraft_num_hash = row["aircraft_nums_hash_key"]
        self.__registration_time = row["registration_time"]
        self.__deregistration_time = row["deregistartion_time"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def aircraft_num_hash(self) -> str:
        return self.__aircraft_num_hash

    @property
    def registration_time(self) -> datetime:
        return self.__registration_time

    @property
    def deregistration_time(self) -> datetime:
        return self.__deregistration_time


class TicketsSat:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__ticket_hash = row["tickets_hash_key"]
        self.__cost = row["cost"]
        self.__baggage_weight = row["baggage_weight"]
        self.__is_registred = row["is_registred"]
        self.__seat_num = row["seat_num"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def ticket_hash(self) -> str:
        return self.__ticket_hash

    @property
    def cost(self) -> datetime:
        return self.__cost

    @property
    def baggage_weight(self) -> datetime:
        return self.__baggage_weight

    @property
    def is_registred(self) -> datetime:
        return self.__is_registred

    @property
    def seat_num(self) -> datetime:
        return self.__seat_num


class PassengersSat:
    def __init__(self, row: pd.Series):
        self.__load_date = row["load_date"]
        self.__record_source = row["record_source"]
        self.__passenger_hash = row["passengers_hash_key"]
        self.__name = row["name"]
        self.__surname = row["surname"]

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def passenger_hash(self) -> str:
        return self.__passenger_hash

    @property
    def name(self) -> str:
        return self.__name

    @property
    def surname(self) -> str:
        return self.__surname


TABLES_CLASS = {
    "aircrafts_hub": AircraftsHub,
    "aircrafts_sat": AircraftsSat,
    "aircrafts_link": AircraftsLink,
    "airlines_hub": AirlinesHub,
    "aircraft_nums_link": AircraftNumsLink,
    "aircraft_nums_sat": AircraftNumsSat,
    "tickets_sat": TicketsSat,
    "tickets_hub": TicketsHub,
    "tickets_link": TicketsLink,
    "seat_classes_hub": SeatClassesHub,
    "seat_classes_link": SeatClassesLink,
    "passengers_link": PassengersLink,
    "passengers_sat": PassengersSat,
    "passengers_hub": PassengersHub,
    "airports_sat": AirportsSat,
    "airports_hub": AirportsHub,
    "airports_link": AirportsLink,
    "flights_hub": FlightsHub,
    "passenger_flights_sat": PassengerFlightsSat,
    "schedules_sat": SchedulesSat,
    "schedules_hub": SchedulesHub,
    "schedules_link": SchedulesLink,
    "status_info_hub": StatusHub,
    "status_infos_sat": StatusInfosSat,
    "status_info_link": StatusesInfosLink,
}


class SchemaManager:

    def __init__(self, csv_path: Path, table_name: str):
        self.__path = csv_path
        self.__table = table_name

    def get_schemas_list(self) -> List:

        df = pd.read_csv(self.__path)
        res = []
        for _, row in df.iterrows():
            res.append(TABLES_CLASS[self.__table](row))

        return res
