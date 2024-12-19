from dwh_tables_scheamas import (
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
    PassengerLink,
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
    def insert(self, schema) -> str:
        pass

    @abstractmethod
    def create_table(self) -> str:
        pass


class StatusHubRepository(AbstractCassandraRepository):

    name = "statuses_hub"

    def insert(self, status: StatusHub) -> str:
        return f"""INSERT INTO {StatusHubRepository.name} (status_hash_key, load_date, record_source, status)
                VALUES ({status.status_hash}, 
                {status.load_date}, 
                {status.record_source}, 
                {status.status});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {StatusHubRepository.name} (
                status_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                status varchar(10));
            """


class SchedulesHubRepository(AbstractCassandraRepository):

    name = "schedules_hub"

    def insert(self, schedules: SchedulesHub) -> str:
        return f"""INSERT INTO {SchedulesHubRepository.name} (schedules_hash_key, load_date, record_source)
                VALUES ({schedules.schedules_hash}, 
                {schedules.load_date}, 
                {schedules.record_source}, 
                );"""

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {SchedulesHubRepository.name} (
                schedules_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150));
            """


class FlightsHubRepositoty(AbstractCassandraRepository):

    name = "flights_hub"

    def insert(self, flights: FlightsHub) -> str:
        return f"""INSERT INTO {FlightsHubRepositoty.name} (flights_hash_key, load_date, record_source)
                VALUES ({flights.flights_hash}, 
                {flights.load_date}, 
                {flights.record_source});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {FlightsHubRepositoty.name} (
                flights_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150));
            """


class AirportsHubRepositoty(AbstractCassandraRepository):

    name = "airports_hub"

    def insert(self, airport: AirportsHub) -> str:
        return f"""INSERT INTO {AirportsHubRepositoty.name} (airports_hash_key, load_date, record_source,iata_name)
                VALUES ({airport.airports_hash}, 
                {airport.load_date}, 
                {airport.record_source},
                {airport.iata_name});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AirportsHubRepositoty.name} (
                airports_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                iata_name varchar(3));
            """


class AircraftsHubRepositoty(AbstractCassandraRepository):

    name = "aircrafts_hub"

    def insert(self, aircraft: AircraftsHub) -> str:
        return f"""INSERT INTO {AircraftsHubRepositoty.name} (airports_hash_key, load_date, record_source,iata_name)
                VALUES ({aircraft.aircrafts_hash}, 
                {aircraft.load_date}, 
                {aircraft.record_source},
                {aircraft.iata_name});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AircraftsHubRepositoty.name} (
                aircrafts_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                iata_name varchar(3));
            """


class TicketsHubRepositoty(AbstractCassandraRepository):

    name = "tickets_hub"

    def insert(self, ticket: TicketsHub) -> str:
        return f"""INSERT INTO {TicketsHubRepositoty.name} (tickets_hash_key, load_date, record_source,number)
                VALUES ({ticket.ticket_hash}, 
                {ticket.load_date}, 
                {ticket.record_source},
                {ticket.number});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {TicketsHubRepositoty.name} (
                tickets_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                number varchar(10));
            """


class PassengersHubRepositoty(AbstractCassandraRepository):

    name = "passengers_hub"

    def insert(self, passenger: PassengersHub) -> str:
        return f"""INSERT INTO {PassengersHubRepositoty.name} (passengers_hash_key, load_date, record_source,passport_series,passport_number)
                VALUES ({passenger.passenger_hash}, 
                {passenger.load_date}, 
                {passenger.record_source},
                {passenger.passport_series},
                {passenger.passport_number});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {PassengersHubRepositoty.name} (
                passengers_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                passport_series varchar(4)),
                passport_number varchar(6));
            """


class AirlinesHubRepositoty(AbstractCassandraRepository):

    name = "airlines_hub"

    def insert(self, airline: AirlinesHub) -> str:
        return f"""INSERT INTO {AirlinesHubRepositoty.name} (airlines_hash_key, load_date, record_source,icao_name,name)
                VALUES ({airline.airline_hash}, 
                {airline.load_date}, 
                {airline.record_source},
                {airline.icao_name},
                {airline.name});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AirlinesHubRepositoty.name} (
                airlines_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                icao_name varchar(2)),
                name varchar(50));
            """


class FareCondsHubRepository(AbstractCassandraRepository):
    name = "seat_classes_hub"

    def insert(self, seat_class: SeatClassesHub) -> str:
        return f"""INSERT INTO {FareCondsHubRepository.name} (seat_class_hash_key, load_date, record_source,seat_class)
                VALUES ({seat_class.seat_class_hash}, 
                {seat_class.load_date}, 
                {seat_class.record_source},
                {seat_class.seat_class});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {FareCondsHubRepository.name} (
                seat_class_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                seat_class varchar(10));
            """


class StatusesInfosLinkRepositoty(AbstractCassandraRepository):

    name = "statuses_info_link"

    def insert(self, status_info: StatusesInfosLink) -> str:
        return f"""INSERT INTO {StatusesInfosLinkRepositoty.name} (statuses_info_has_key, load_date, record_source,schedules_hash_key,status_hash_key)
                VALUES ({status_info.status_info_hash}, 
                {status_info.load_date}, 
                {status_info.record_source},
                {status_info.schedules_hash},
                {status_info.status_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {StatusesInfosLinkRepositoty.name}_link (
                statuses_info_has_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                schedules_hash_key varchar(32)),
                status_hash_key varchar(32));
            """


class SchedulesLinkRepositoty(AbstractCassandraRepository):

    name = "schedules_link"

    def insert(self, schedule_link: SchedulesLink) -> str:
        return f"""INSERT INTO {SchedulesLinkRepositoty.name} (schedules_hash_key, load_date, record_source,flight_hash_key)
                VALUES ({schedule_link.schedules_hash}, 
                {schedule_link.load_date}, 
                {schedule_link.record_source},
                {schedule_link.flight_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {SchedulesLinkRepositoty.name} (
                schedules_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                flight_hash_key varchar(32));
            """


class AirportsLinkRepositoty(AbstractCassandraRepository):

    name = "airports_link"

    def insert(self, airports_link: AirportsLink) -> str:
        return f"""INSERT INTO {AirportsLinkRepositoty.name} (arrival_airport_hash_key,departure_airport_hash_key, load_date, record_source,flight_hash_key)
                VALUES ({airports_link.arrival_airport_hash}, 
                {airports_link.departure_airport_hash},
                {airports_link.load_date}, 
                {airports_link.record_source},
                {airports_link.flight_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AirportsLinkRepositoty.name} (
                arrival_airport_hash_key varchar(32),
                departure_airport_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                flight_hash_key varchar(32));
            """


class AircraftNumsLinkRepositoty(AbstractCassandraRepository):

    name = "aircraft_nums_link"

    def insert(self, aircraft_num_link: AircraftNumsLink) -> str:
        return f"""INSERT INTO {AircraftNumsLinkRepositoty.name} (aircraft_num_hash_key, load_date, record_source,airline_hash_key,aircraft_hash_key)
                VALUES ({aircraft_num_link.aircraft_num_hash}, 
                {aircraft_num_link.load_date}, 
                {aircraft_num_link.record_source},
                {aircraft_num_link.airline_hash},
                {aircraft_num_link.aircraft_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AircraftNumsLinkRepositoty.name} (
                aircraft_num_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                airline_hash_key varchar(32),
                aircraft_hash_key varchar(32));
            """


class AircraftsLinkRepositoty(AbstractCassandraRepository):

    name = "aircrafts_link"

    def insert(self, aircraft_link: AircraftsLink) -> str:
        return f"""INSERT INTO {AircraftsLinkRepositoty.name} (aircraft_hash_key, load_date, record_source,flight_hash_key)
                VALUES ({aircraft_link.aircraft_hash}, 
                {aircraft_link.load_date}, 
                {aircraft_link.record_source},
                {aircraft_link.flight_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AircraftsLinkRepositoty.name} (
                aircraft_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                flight_hash_key varchar(32));
            """


class TicketsLinkRepositoty(AbstractCassandraRepository):

    name = "tickets_link"

    def insert(self, ticket_link: TicketsLink) -> str:
        return f"""INSERT INTO {TicketsLinkRepositoty.name} (ticket_hash_key, load_date, record_source,flight_hash_key)
                VALUES ({ticket_link.ticket_hash}, 
                {ticket_link.load_date}, 
                {ticket_link.record_source},
                {ticket_link.flight_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {TicketsLinkRepositoty.name} (
                ticket_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                flight_hash_key varchar(32));
            """


class SeatClassesLinkRepositoty(AbstractCassandraRepository):

    name = "seat_classes_link"

    def insert(self, seat_class_link: SeatClassesLink) -> str:
        return f"""INSERT INTO {SeatClassesLinkRepositoty.name} (ticket_hash_key, load_date, record_source,seat_class_hash_key)
                VALUES ({seat_class_link.ticket_hash}, 
                {seat_class_link.load_date}, 
                {seat_class_link.record_source},
                {seat_class_link.seat_class_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {SeatClassesLinkRepositoty.name} (
                ticket_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                seat_class_hash_key varchar(32));
            """


class PassengersLinkRepositoty(AbstractCassandraRepository):

    name = "passengers_link"

    def insert(self, passenger_link: PassengerLink) -> str:
        return f"""INSERT INTO {PassengersLinkRepositoty.name} (ticket_hash_key, load_date, record_source,passenger_hash_key)
                VALUES ({passenger_link.ticket_hash}, 
                {passenger_link.load_date}, 
                {passenger_link.record_source},
                {passenger_link.passenger_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {PassengersLinkRepositoty.name} (
                ticket_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                passenger_hash_key varchar(32));
            """


class StatusInfosSatRepositoty(AbstractCassandraRepository):

    name = "status_infos_sat"

    def insert(self, status_info_sat: StatusInfosSat) -> str:
        return f"""INSERT INTO {StatusInfosSatRepositoty.name} (status_info_hash_key, load_date, record_source,set_status_time,unset_status_time)
                VALUES ({status_info_sat.status_info_hash}, 
                {status_info_sat.load_date}, 
                {status_info_sat.record_source},
                {status_info_sat.set_status_time},
                {status_info_sat.unset_status_time});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {StatusInfosSatRepositoty.name} (
                status_info_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                set_status_time timestamp WITH TIME ZONE,
                unset_status_time timestamp WITH TIME ZONE);
            """


class SchedulesSatRepositoty(AbstractCassandraRepository):

    name = "schedules_sat"

    def insert(self, schedule_sat: SchedulesSat) -> str:
        return f"""INSERT INTO {SchedulesSatRepositoty.name} (schedules_hash_key, load_date, record_source,actual_arrival_time,actual_departure_time,planned_arrival_time,planned_departure_time)
                VALUES ({schedule_sat.schedules_hash}, 
                {schedule_sat.load_date}, 
                {schedule_sat.record_source},
                {schedule_sat.actual_arrival_time},
                {schedule_sat.actual_departure_time},
                {schedule_sat.planned_arrival_time},
                {schedule_sat.planned_departure_time},);
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {SchedulesSatRepositoty.name} (
                schedules_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                actual_arrival_time timestamp WITH TIME ZONE,
                actual_departure_time timestamp WITH TIME ZONE,
                planned_arrival_time timestamp WITH TIME ZONE,
                planned_departure_time timestamp WITH TIME ZONE);
            """


class CargoFlightsSatRepositoty(AbstractCassandraRepository):

    name = "cargo_flights_sat"

    def insert(self, cargo_sat: CargoFlightsSat) -> str:
        return f"""INSERT INTO {CargoFlightsSatRepositoty.name} (flight_hash_key, load_date, record_source,weight)
                VALUES ({cargo_sat.flight_hash}, 
                {cargo_sat.load_date}, 
                {cargo_sat.record_source},
                {cargo_sat.weight},);
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {CargoFlightsSatRepositoty.name} (
                flight_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                weight INTEGER,);
            """


class PassengerFlightsSatRepositoty(AbstractCassandraRepository):

    name = "passenger_flights_sat"

    def insert(self, passenger_sat: PassengerFlightsSat) -> str:
        return f"""INSERT INTO {PassengerFlightsSatRepositoty.name} (flight_hash_key, load_date, record_source,registration_time,is_ramp,gate,flight_number)
                VALUES ({passenger_sat.flight_hash}, 
                {passenger_sat.load_date}, 
                {passenger_sat.record_source},
                {passenger_sat.registration_time},
                {passenger_sat.is_ramp},
                {passenger_sat.gate},
                {passenger_sat.flight_number});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {PassengerFlightsSatRepositoty.name} (
                flight_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                registration_time timestamp WITH TIME ZONE,
                is_ramp BOOLEAN,
                gate varchar(3),
                flight_number varchar(6));
            """


class AirportsSatRepositoty(AbstractCassandraRepository):

    name = "airports_sat"

    def insert(self, airport_sat: AirportsSat) -> str:
        return f"""INSERT INTO {AirportsSatRepositoty.name} (airport_hash_key, load_date, record_source,name,city,timezone)
                VALUES ({airport_sat.airport_hash}, 
                {airport_sat.load_date}, 
                {airport_sat.record_source},
                {airport_sat.name},
                {airport_sat.city},
                {airport_sat.timezone});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AirportsSatRepositoty.name} (
                airport_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                name varchar(50),
                city varchar(50),
                timezone varchar(50));
            """


class AircraftsSatRepositoty(AbstractCassandraRepository):

    name = "aircrafts_sat"

    def insert(self, aircraft_sat: AircraftsSat) -> str:
        return f"""INSERT INTO {AircraftsSatRepositoty.name} (aircraft_hash_key, load_date, record_source,name,seats_num)
                VALUES ({aircraft_sat.aircraft_hash}, 
                {aircraft_sat.load_date}, 
                {aircraft_sat.record_source},
                {aircraft_sat.name},
                {aircraft_sat.seats_num});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AircraftsSatRepositoty.name} (
                aircraft_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                name varchar(50),
                seats_num INTEGER);
            """


class AircraftNumsSatRepositoty(AbstractCassandraRepository):

    name = "aircraft_nums_sat"

    def insert(self, aircraft_num_sat: AircraftNumsSat) -> str:
        return f"""INSERT INTO {AircraftNumsSatRepositoty.name} (aircraft_hash_key, load_date, record_source,registration_time,deregistration_time)
                VALUES ({aircraft_num_sat.aircraft_num_hash}, 
                {aircraft_num_sat.load_date}, 
                {aircraft_num_sat.record_source},
                {aircraft_num_sat.registration_time},
                {aircraft_num_sat.deregistration_time});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {AircraftNumsSatRepositoty.name} (
                aircraft_num_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                registration_time timestamp WITH TIME ZONE,
                deregistration_time timestamp WITH TIME ZONE);
            """


class TicketsSatRepositoty(AbstractCassandraRepository):

    name = "tickets_sat"

    def insert(self, ticket_sat: TicketsSat) -> str:
        return f"""INSERT INTO {TicketsSatRepositoty.name} (ticket_hash_key, load_date, record_source,cost,baggage_weight,is_registred,seat_num)
                VALUES ({ticket_sat.ticket_hash}, 
                {ticket_sat.load_date}, 
                {ticket_sat.record_source},
                {ticket_sat.cost},
                {ticket_sat.baggage_weight},
                {ticket_sat.is_registred},
                {ticket_sat.seat_num});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {TicketsSatRepositoty.name} (
                ticket_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                cost FLOAT,
                baggage_weight FLOAT,
                is_registred BOOLEAN, 
                seat_num varchar(3));
            """


class PassengersSatRepositoty(AbstractCassandraRepository):

    name = "passengers_sat"

    def insert(self, passenger_sat: PassengersSat) -> str:
        return f"""INSERT INTO {PassengersSatRepositoty.name} (passenger_hash_key, load_date, record_source,name,surname)
                VALUES ({passenger_sat.passenger_hash}, 
                {passenger_sat.load_date}, 
                {passenger_sat.record_source},
                {passenger_sat.name},
                {passenger_sat.surname});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {PassengersSatRepositoty.name} (
                passenger_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                name varchar(50), 
                surname varchar(50));
            """
