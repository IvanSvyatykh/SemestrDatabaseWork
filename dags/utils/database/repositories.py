from dwh_tables_scheamas import (
    AircraftNumsLink,
    AircraftsHub,
    AircraftsLink,
    AirlinesHub,
    AirportsHub,
    AirportsLink,
    PassengerLink,
    PassengersHub,
    SchedulesLink,
    SeatClassesHub,
    SeatClassesLink,
    StatusHub,
    SchedulesHub,
    FlightsHub,
    StatusesInfosLink,
    TicketsHub,
    TicketsLink,
)


class StatusHubRepository:

    __name = "statuses_hub"

    def insert(self, status: StatusHub) -> str:
        return f"""INSERT INTO {self.__name} (status_hash_key, load_date, record_source, status)
                VALUES ({status.status_hash}, 
                {status.load_date}, 
                {status.record_source}, 
                {status.status});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                status_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                status varchar(10));
            """


class SchedulesHubRepository:

    __name = "schedules_hub"

    def insert(self, schedules: SchedulesHub) -> str:
        return f"""INSERT INTO {self.__name} (schedules_hash_key, load_date, record_source)
                VALUES ({schedules.schedules_hash}, 
                {schedules.load_date}, 
                {schedules.record_source}, 
                );"""

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                schedules_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150));
            """


class FlightsHubRepositoty:

    __name = "flights_hub"

    def insert(self, flights: FlightsHub) -> str:
        return f"""INSERT INTO {self.__name} (flights_hash_key, load_date, record_source,flight_number)
                VALUES ({flights.flights_hash}, 
                {flights.load_date}, 
                {flights.record_source},
                {flights.flight_num});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                flights_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                flight_number varchar(6));
            """


class AirportsHubRepositoty:

    __name = "airports_hub"

    def insert(self, airport: AirportsHub) -> str:
        return f"""INSERT INTO {self.__name} (airports_hash_key, load_date, record_source,iata_name)
                VALUES ({airport.airports_hash}, 
                {airport.load_date}, 
                {airport.record_source},
                {airport.iata_name});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                airports_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                iata_name varchar(3));
            """


class AircraftsHubRepositoty:

    __name = "aircrafts_hub"

    def insert(self, aircraft: AircraftsHub) -> str:
        return f"""INSERT INTO {self.__name} (airports_hash_key, load_date, record_source,iata_name)
                VALUES ({aircraft.aircrafts_hash}, 
                {aircraft.load_date}, 
                {aircraft.record_source},
                {aircraft.iata_name});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                aircrafts_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                iata_name varchar(3));
            """


class TicketsHubRepositoty:

    __name = "tickets_hub"

    def insert(self, ticket: TicketsHub) -> str:
        return f"""INSERT INTO {self.__name} (tickets_hash_key, load_date, record_source,number)
                VALUES ({ticket.ticket_hash}, 
                {ticket.load_date}, 
                {ticket.record_source},
                {ticket.number});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                tickets_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                number varchar(10));
            """


class PassengersHubRepositoty:

    __name = "passengers_hub"

    def insert(self, passenger: PassengersHub) -> str:
        return f"""INSERT INTO {self.__name} (passengers_hash_key, load_date, record_source,passport_series,passport_number)
                VALUES ({passenger.passenger_hash}, 
                {passenger.load_date}, 
                {passenger.record_source},
                {passenger.passport_series},
                {passenger.passport_number});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                passengers_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                passport_series varchar(4)),
                passport_number varchar(6));
            """


class AirlinesHubRepositoty:

    __name = "airlines_hub"

    def insert(self, airline: AirlinesHub) -> str:
        return f"""INSERT INTO {self.__name} (passengers_hash_key, load_date, record_source,icao_name,name)
                VALUES ({airline.airline_hash}, 
                {airline.load_date}, 
                {airline.record_source},
                {airline.icao_name},
                {airline.name});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                airlines_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                icao_name varchar(2)),
                name varchar(50));
            """


class FareCondsHubRepository:
    __name = "seat_classes_hub"

    def insert(self, seat_class: SeatClassesHub) -> str:
        return f"""INSERT INTO {self.__name} (seat_class_hash_key, load_date, record_source,seat_class)
                VALUES ({seat_class.seat_class_hash}, 
                {seat_class.load_date}, 
                {seat_class.record_source},
                {seat_class.seat_class});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                seat_class_hash_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                seat_class varchar(10));
            """


class StatusesInfosLinkRepositoty:

    __name = "statuses_info_link"

    def insert(self, status_info: StatusesInfosLink) -> str:
        return f"""INSERT INTO {self.__name} (statuses_info_has_key, load_date, record_source,schedules_hash_key,status_hash_key)
                VALUES ({status_info.status_info_hash}, 
                {status_info.load_date}, 
                {status_info.record_source},
                {status_info.schedules_hash},
                {status_info.status_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name}_link (
                statuses_info_has_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                schedules_hash_key varchar(32)),
                status_hash_key varchar(32));
            """


class SchedulesLinkRepositoty:

    __name = "schedules_link"

    def insert(self, schedule_link: SchedulesLink) -> str:
        return f"""INSERT INTO {self.__name} (schedules_hash_key, load_date, record_source,flight_hash_key)
                VALUES ({schedule_link.schedules_hash}, 
                {schedule_link.load_date}, 
                {schedule_link.record_source},
                {schedule_link.flight_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                schedules_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                flight_hash_key varchar(32));
            """


class AirportsLinkRepositoty:

    __name = "airports_link"

    def insert(self, airports_link: AirportsLink) -> str:
        return f"""INSERT INTO {self.__name} (airport_hash_key, load_date, record_source,flight_hash_key)
                VALUES ({airports_link.airport_hash}, 
                {airports_link.load_date}, 
                {airports_link.record_source},
                {airports_link.flight_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                airport_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                flight_hash_key varchar(32));
            """


class AircraftNumsLinkRepositoty:

    __name = "aircraft_nums_link"

    def insert(self, aircraft_num_link: AircraftNumsLink) -> str:
        return f"""INSERT INTO {self.__name} (aircraft_num_hash_key, load_date, record_source,airline_hash_key,aircraft_hash_key)
                VALUES ({aircraft_num_link.aircraft_num_hash}, 
                {aircraft_num_link.load_date}, 
                {aircraft_num_link.record_source},
                {aircraft_num_link.airline_hash},
                {aircraft_num_link.aircraft_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                aircraft_num_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                airline_hash_key varchar(32),
                aircraft_hash_key varchar(32));
            """


class AircraftsLinkRepositoty:

    __name = "aircrafts_link"

    def insert(self, aircraft_link: AircraftsLink) -> str:
        return f"""INSERT INTO {self.__name} (aircraft_hash_key, load_date, record_source,flight_hash_key)
                VALUES ({aircraft_link.aircraft_hash}, 
                {aircraft_link.load_date}, 
                {aircraft_link.record_source},
                {aircraft_link.flight_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                aircraft_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                flight_hash_key varchar(32));
            """


class TicketsLinkRepositoty:

    __name = "tickets_link"

    def insert(self, ticket_link: TicketsLink) -> str:
        return f"""INSERT INTO {self.__name} (ticket_hash_key, load_date, record_source,flight_hash_key)
                VALUES ({ticket_link.ticket_hash}, 
                {ticket_link.load_date}, 
                {ticket_link.record_source},
                {ticket_link.flight_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                ticket_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                flight_hash_key varchar(32));
            """


class SeatClassesLinkRepositoty:

    __name = "seat_classes_link"

    def insert(self, seat_class_link: SeatClassesLink) -> str:
        return f"""INSERT INTO {self.__name} (ticket_hash_key, load_date, record_source,seat_class_hash_key)
                VALUES ({seat_class_link.ticket_hash}, 
                {seat_class_link.load_date}, 
                {seat_class_link.record_source},
                {seat_class_link.seat_class_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                ticket_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                seat_class_hash_key varchar(32));
            """


class PassengersLinkRepositoty:

    __name = "passengers_link"

    def insert(self, passenger_link: PassengerLink) -> str:
        return f"""INSERT INTO {self.__name} (ticket_hash_key, load_date, record_source,passenger_hash_key)
                VALUES ({passenger_link.ticket_hash}, 
                {passenger_link.load_date}, 
                {passenger_link.record_source},
                {passenger_link.passenger_hash});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                ticket_hash_key varchar(32),
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                passenger_hash_key varchar(32));
            """
