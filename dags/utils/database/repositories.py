from typing import List, Tuple
from utils.database.dwh_tables_schemas import (
    AircraftNumsLink,
    AircraftNumsSat,
    AircraftsHub,
    AircraftsLink,
    AircraftsSat,
    AirlinesHub,
    AirportsHub,
    AirportsLink,
    AirportsSat,
    CargoFlightsSat,
    PassengerFlightsSat,
    PassengersLink,
    PassengersHub,
    SchedulesLink,
    SchedulesSat,
    SeatClassesHub,
    SeatClassesLink,
    StatusHub,
    SchedulesHub,
    FlightsHub,
    StatusInfosSat,
    StatusesInfosLink,
    TicketsHub,
    TicketsLink,
    PassengersSat,
    TicketsSat,
)
from abc import ABC, abstractmethod


class AbstractCassandraRepository(ABC):

    @abstractmethod
    def insert(self, schema) -> Tuple[str, List]:
        pass

    @abstractmethod
    def create_table(self) -> str:
        pass


class StatusHubRepository(AbstractCassandraRepository):

    name = "status_info_hub"

    def insert(self, status: StatusHub) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {StatusHubRepository.name} (status_hash_key, load_date, record_source, status)
                VALUES (%s, %s, %s, %s);
                """,
            [
                status.status_hash,
                status.load_date,
                status.record_source,
                status.status,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {StatusHubRepository.name} (
                status_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                status varchar);
            """


class SchedulesHubRepository(AbstractCassandraRepository):

    name = "schedules_hub"

    def insert(self, schedules: SchedulesHub) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {SchedulesHubRepository.name} (schedules_hash_key, load_date, record_source)
                VALUES (%s, %s, %s);""",
            [
                schedules.schedules_hash,
                schedules.load_date,
                schedules.record_source,
            ],
        )

    def create_table(self) -> str:
        return f"""CREATE TABLE IF NOT EXISTS {SchedulesHubRepository.name} (
                schedules_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar);
            """


class FlightsHubRepositoty(AbstractCassandraRepository):

    name = "flights_hub"

    def insert(self, flights: FlightsHub) -> Tuple[str, List]:
        return f"""INSERT INTO {FlightsHubRepositoty.name} (flights_hash_key, load_date, record_source)
                VALUES (%s, %s, %s);
                """, [
            flights.flights_hash,
            flights.load_date,
            flights.record_source,
        ]

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {FlightsHubRepositoty.name} (
                flights_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar);
            """


class AirportsHubRepositoty(AbstractCassandraRepository):

    name = "airports_hub"

    def insert(self, airport: AirportsHub) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {AirportsHubRepositoty.name} (airports_hash_key, load_date, record_source,iata_name)
                VALUES (%s, %s, %s,%s);
                """,
            [
                airport.airports_hash,
                airport.load_date,
                airport.record_source,
                airport.iata_name,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AirportsHubRepositoty.name} (
                airports_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                iata_name varchar);
            """


class AircraftsHubRepositoty(AbstractCassandraRepository):

    name = "aircrafts_hub"

    def insert(self, aircraft: AircraftsHub) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {AircraftsHubRepositoty.name} (aircrafts_hash_key, load_date, record_source,iata_name)
                VALUES (%s, %s, %s, %s);
                """,
            [
                aircraft.aircrafts_hash,
                aircraft.load_date,
                aircraft.record_source,
                aircraft.iata_name,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AircraftsHubRepositoty.name} (
                aircrafts_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                iata_name varchar);
            """


class TicketsHubRepositoty(AbstractCassandraRepository):

    name = "tickets_hub"

    def insert(self, ticket: TicketsHub) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {TicketsHubRepositoty.name} (tickets_hash_key, load_date, record_source,number)
                VALUES (%s, %s, %s, %s);
                """,
            [
                ticket.ticket_hash,
                ticket.load_date,
                ticket.record_source,
                ticket.number,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {TicketsHubRepositoty.name} (
                tickets_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                number varchar);
            """


class PassengersHubRepositoty(AbstractCassandraRepository):

    name = "passengers_hub"

    def insert(self, passenger: PassengersHub) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {PassengersHubRepositoty.name} (passengers_hash_key, load_date, record_source,passport_series,passport_number)
                VALUES (%s, %s, %s, %s, %s);
                """,
            [
                passenger.passenger_hash,
                passenger.load_date,
                passenger.record_source,
                passenger.passport_series,
                passenger.passport_number,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {PassengersHubRepositoty.name} (
                passengers_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                passport_series varchar,
                passport_number varchar);
            """


class AirlinesHubRepositoty(AbstractCassandraRepository):

    name = "airlines_hub"

    def insert(self, airline: AirlinesHub) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {AirlinesHubRepositoty.name} (airlines_hash_key, load_date, record_source,icao_name,name)
                VALUES (%s, %s, %s,%s, %s);
                """,
            [
                airline.airline_hash,
                airline.load_date,
                airline.record_source,
                airline.icao_name,
                airline.name,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AirlinesHubRepositoty.name} (
                airlines_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                icao_name varchar,
                name varchar);
            """


class FareCondsHubRepository(AbstractCassandraRepository):
    name = "seat_classes_hub"

    def insert(self, seat_class: SeatClassesHub) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {FareCondsHubRepository.name} (seat_class_hash_key, load_date, record_source,seat_class)
                VALUES (%s, %s, %s, %s);
                """,
            [
                seat_class.seat_class_hash,
                seat_class.load_date,
                seat_class.record_source,
                seat_class.seat_class,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {FareCondsHubRepository.name} (
                seat_class_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                seat_class varchar);
            """


class StatusesInfosLinkRepositoty(AbstractCassandraRepository):

    name = "status_info_link"

    def insert(self, status_info: StatusesInfosLink) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {StatusesInfosLinkRepositoty.name} (statuses_info_has_key, load_date, record_source,schedules_hash_key,status_hash_key)
                VALUES (%s, %s, %s, %s, %s);
                """,
            [
                status_info.status_info_hash,
                status_info.load_date,
                status_info.record_source,
                status_info.schedules_hash,
                status_info.status_hash,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {StatusesInfosLinkRepositoty.name} (
                statuses_info_has_key varchar,
                load_date timestamp,
                record_source varchar,
                schedules_hash_key varchar,
                status_hash_key varchar,PRIMARY KEY(statuses_info_has_key,status_hash_key));
            """


class SchedulesLinkRepositoty(AbstractCassandraRepository):

    name = "schedules_link"

    def insert(self, schedule_link: SchedulesLink) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {SchedulesLinkRepositoty.name} (schedules_hash_key, load_date, record_source,flight_hash_key)
                VALUES (%s, %s, %s, %s);
                """,
            [
                schedule_link.schedules_hash,
                schedule_link.load_date,
                schedule_link.record_source,
                schedule_link.flight_hash,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {SchedulesLinkRepositoty.name} (
                schedules_hash_key varchar,
                load_date timestamp,
                record_source varchar,
                flight_hash_key varchar,PRIMARY KEY(schedules_hash_key,flight_hash_key));
            """


class AirportsLinkRepositoty(AbstractCassandraRepository):

    name = "airports_link"

    def insert(self, airports_link: AirportsLink) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {AirportsLinkRepositoty.name} (arrival_airport_hash_key,departure_airport_hash_key, load_date, record_source,flight_hash_key)
                VALUES (%s, %s,%s, %s, %s);
                """,
            [
                airports_link.arrival_airport_hash,
                airports_link.departure_airport_hash,
                airports_link.load_date,
                airports_link.record_source,
                airports_link.flight_hash,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AirportsLinkRepositoty.name} (
                arrival_airport_hash_key varchar,
                departure_airport_hash_key varchar,
                load_date timestamp,
                record_source varchar,
                flight_hash_key varchar,PRIMARY KEY(flight_hash_key,arrival_airport_hash_key,departure_airport_hash_key));
            """


class AircraftNumsLinkRepositoty(AbstractCassandraRepository):

    name = "aircraft_nums_link"

    def insert(
        self, aircraft_num_link: AircraftNumsLink
    ) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {AircraftNumsLinkRepositoty.name} (aircraft_num_hash_key, load_date, record_source,airline_hash_key,aircraft_hash_key)
                VALUES (%s, %s, %s, %s, %s);
                """,
            [
                aircraft_num_link.aircraft_num_hash,
                aircraft_num_link.load_date,
                aircraft_num_link.record_source,
                aircraft_num_link.airline_hash,
                aircraft_num_link.aircraft_hash,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AircraftNumsLinkRepositoty.name} (
                aircraft_num_hash_key varchar ,
                load_date timestamp,
                record_source varchar,
                airline_hash_key varchar,
                aircraft_hash_key varchar,PRIMARY KEY(aircraft_hash_key,aircraft_num_hash_key));
            """


class AircraftsLinkRepositoty(AbstractCassandraRepository):

    name = "aircrafts_link"

    def insert(self, aircraft_link: AircraftsLink) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {AircraftsLinkRepositoty.name} (aircraft_hash_key, load_date, record_source,flight_hash_key)
                VALUES (%s, %s, %s, %s);
                """,
            [
                aircraft_link.aircraft_hash,
                aircraft_link.load_date,
                aircraft_link.record_source,
                aircraft_link.flight_hash,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AircraftsLinkRepositoty.name} (
                aircraft_hash_key varchar,
                load_date timestamp,
                record_source varchar,
                flight_hash_key varchar,PRIMARY KEY(aircraft_hash_key,flight_hash_key));
            """


class TicketsLinkRepositoty(AbstractCassandraRepository):

    name = "tickets_link"

    def insert(self, ticket_link: TicketsLink) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {TicketsLinkRepositoty.name} (ticket_hash_key, load_date, record_source,flight_hash_key)
                VALUES (%s, %s, %s, %s);
                """,
            [
                ticket_link.ticket_hash,
                ticket_link.load_date,
                ticket_link.record_source,
                ticket_link.flight_hash,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {TicketsLinkRepositoty.name} (
                ticket_hash_key varchar,
                load_date timestamp,
                record_source varchar,
                flight_hash_key varchar,PRIMARY KEY(ticket_hash_key,flight_hash_key));
            """


class SeatClassesLinkRepositoty(AbstractCassandraRepository):

    name = "seat_classes_link"

    def insert(self, seat_class_link: SeatClassesLink) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {SeatClassesLinkRepositoty.name} (ticket_hash_key, load_date, record_source,seat_class_hash_key)
                VALUES (%s, %s, %s, %s);
                """,
            [
                seat_class_link.ticket_hash,
                seat_class_link.load_date,
                seat_class_link.record_source,
                seat_class_link.seat_class_hash,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {SeatClassesLinkRepositoty.name} (
                ticket_hash_key varchar,
                load_date timestamp,
                record_source varchar,
                seat_class_hash_key varchar,PRIMARY KEY(ticket_hash_key,seat_class_hash_key));
            """


class PassengersLinkRepositoty(AbstractCassandraRepository):

    name = "passengers_link"

    def insert(self, passenger_link: PassengersLink) -> Tuple[str, List]:
        return f"""INSERT INTO {PassengersLinkRepositoty.name} (ticket_hash_key, load_date, record_source,passenger_hash_key)
                VALUES (%s, %s, %s, %s);
                """, [
            passenger_link.ticket_hash,
            passenger_link.load_date,
            passenger_link.record_source,
            passenger_link.passenger_hash,
        ]

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {PassengersLinkRepositoty.name} (
                ticket_hash_key varchar,
                load_date timestamp,
                record_source varchar,
                passenger_hash_key varchar,PRIMARY KEY(ticket_hash_key,passenger_hash_key));
            """


class StatusInfosSatRepositoty(AbstractCassandraRepository):

    name = "status_info_sat"

    def insert(self, status_info_sat: StatusInfosSat) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {StatusInfosSatRepositoty.name} (status_info_hash_key, load_date, record_source,set_status_time,unset_status_time)
                VALUES (%s, %s, %s, %s, %s);
                """,
            [
                status_info_sat.status_info_hash,
                status_info_sat.load_date,
                status_info_sat.record_source,
                status_info_sat.set_status_time,
                status_info_sat.unset_status_time,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {StatusInfosSatRepositoty.name} (
                status_info_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                set_status_time timestamp,
                unset_status_time timestamp);
            """


class SchedulesSatRepositoty(AbstractCassandraRepository):

    name = "schedules_sat"

    def insert(self, schedule_sat: SchedulesSat) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {SchedulesSatRepositoty.name} (schedules_hash_key, load_date, record_source,actual_arrival_time,actual_departure_time,planned_arrival_time,planned_departure_time)
                VALUES (%s, %s, %s,%s,%s,%s,%s);
                """,
            [
                schedule_sat.schedules_hash,
                schedule_sat.load_date,
                schedule_sat.record_source,
                schedule_sat.actual_arrival_time,
                schedule_sat.actual_departure_time,
                schedule_sat.planned_arrival_time,
                schedule_sat.planned_departure_time,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {SchedulesSatRepositoty.name} (
                schedules_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                actual_arrival_time timestamp,
                actual_departure_time timestamp,
                planned_arrival_time timestamp,
                planned_departure_time timestamp);
            """


class CargoFlightsSatRepositoty(AbstractCassandraRepository):

    name = "cargo_flights_sat"

    def insert(self, cargo_sat: CargoFlightsSat) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {CargoFlightsSatRepositoty.name} (flight_hash_key, load_date, record_source,weight)
                VALUES (%s, %s, %s, %s);
                """,
            [
                cargo_sat.flight_hash,
                cargo_sat.load_date,
                cargo_sat.record_source,
                cargo_sat.weight,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {CargoFlightsSatRepositoty.name} (
                flight_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                weight INT);
            """


class PassengerFlightsSatRepositoty(AbstractCassandraRepository):

    name = "passenger_flights_sat"

    def insert(
        self, passenger_sat: PassengerFlightsSat
    ) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {PassengerFlightsSatRepositoty.name} (flight_hash_key, load_date, record_source,registration_time,is_ramp,gate,flight_number)
                VALUES (%s, %s, %s,%s,%s,%s,%s);
                """,
            [
                passenger_sat.flight_hash,
                passenger_sat.load_date,
                passenger_sat.record_source,
                passenger_sat.registration_time,
                passenger_sat.is_ramp,
                passenger_sat.gate,
                passenger_sat.flight_number,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {PassengerFlightsSatRepositoty.name} (
                flight_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                registration_time timestamp,
                is_ramp BOOLEAN,
                gate varchar,
                flight_number varchar);
            """


class AirportsSatRepositoty(AbstractCassandraRepository):

    name = "airports_sat"

    def insert(self, airport_sat: AirportsSat) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {AirportsSatRepositoty.name} (airport_hash_key, load_date, record_source,name,city,timezone)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
            [
                airport_sat.airport_hash,
                airport_sat.load_date,
                airport_sat.record_source,
                airport_sat.name,
                airport_sat.city,
                airport_sat.timezone,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AirportsSatRepositoty.name} (
                airport_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                name varchar,
                city varchar,
                timezone varchar);
            """


class AircraftsSatRepositoty(AbstractCassandraRepository):

    name = "aircrafts_sat"

    def insert(self, aircraft_sat: AircraftsSat) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {AircraftsSatRepositoty.name} (aircraft_hash_key, load_date, record_source,name,seats_num)
                VALUES (%s, %s, %s, %s, %s);
                """,
            [
                aircraft_sat.aircraft_hash,
                aircraft_sat.load_date,
                aircraft_sat.record_source,
                aircraft_sat.name,
                aircraft_sat.seats_num,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AircraftsSatRepositoty.name} (
                aircraft_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                name varchar,
                seats_num INT);
            """


class AircraftNumsSatRepositoty(AbstractCassandraRepository):

    name = "aircraft_nums_sat"

    def insert(
        self, aircraft_num_sat: AircraftNumsSat
    ) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {AircraftNumsSatRepositoty.name} (aircraft_num_hash_key, load_date, record_source,registration_time,deregistration_time)
                VALUES (%s, %s, %s, %s, %s);
                """,
            [
                aircraft_num_sat.aircraft_num_hash,
                aircraft_num_sat.load_date,
                aircraft_num_sat.record_source,
                aircraft_num_sat.registration_time,
                aircraft_num_sat.deregistration_time,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AircraftNumsSatRepositoty.name} (
                aircraft_num_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                registration_time timestamp,
                deregistration_time timestamp);
            """


class TicketsSatRepositoty(AbstractCassandraRepository):

    name = "tickets_sat"

    def insert(self, ticket_sat: TicketsSat) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {TicketsSatRepositoty.name} (ticket_hash_key, load_date, record_source,cost,baggage_weight,is_registred,seat_num)
                VALUES (%s, %s, %s,%s,%s,%s,%s);
                """,
            [
                ticket_sat.ticket_hash,
                ticket_sat.load_date,
                ticket_sat.record_source,
                ticket_sat.cost,
                ticket_sat.baggage_weight,
                ticket_sat.is_registred,
                ticket_sat.seat_num,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {TicketsSatRepositoty.name} (
                ticket_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                cost FLOAT,
                baggage_weight FLOAT,
                is_registred BOOLEAN, 
                seat_num varchar);
            """


class PassengersSatRepositoty(AbstractCassandraRepository):

    name = "passengers_sat"

    def insert(self, passenger_sat: PassengersSat) -> Tuple[str, List]:
        return (
            f"""INSERT INTO {PassengersSatRepositoty.name} (passenger_hash_key, load_date, record_source,name,surname)
                VALUES (%s, %s, %s, %s, %s);
                """,
            [
                passenger_sat.passenger_hash,
                passenger_sat.load_date,
                passenger_sat.record_source,
                passenger_sat.name,
                passenger_sat.surname,
            ],
        )

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {PassengersSatRepositoty.name} (
                passenger_hash_key varchar PRIMARY KEY,
                load_date timestamp,
                record_source varchar,
                name varchar, 
                surname varchar);
            """
