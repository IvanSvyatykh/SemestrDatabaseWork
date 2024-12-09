from dwh_tables_scheamas import (
    AircraftsHub,
    AirlinesHub,
    AirportsHub,
    PassengersHub,
    StatusHub,
    SchedulesHub,
    FlightsHub,
    TicketsHub,
)


class StatusHubRepository:

    __name = "statuses"

    def insert(self, status: StatusHub) -> str:
        return f"""INSERT INTO {self.__name} (status_has_key, load_date, record_source, status)
                VALUES ({status.status_hash}, 
                {status.load_date}, 
                {status.record_source}, 
                {status.status});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                status_has_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                status varchar(10));
            """


class SchedulesHubRepository:

    __name = "schedules"

    def insert(self, schedules: SchedulesHub) -> str:
        return f"""INSERT INTO {self.__name} (schedules_has_key, load_date, record_source)
                VALUES ({schedules.schedules_hash}, 
                {schedules.load_date}, 
                {schedules.record_source}, 
                );"""

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                schedules_has_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150));
            """


class FlightsHubRepositoty:

    __name = "flights"

    def insert(self, flights: FlightsHub) -> str:
        return f"""INSERT INTO {self.__name} (flights_has_key, load_date, record_source,flight_number)
                VALUES ({flights.flights_hash}, 
                {flights.load_date}, 
                {flights.record_source},
                {flights.flight_num});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                flights_has_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                flight_number varchar(6));
            """


class AirportsHubRepositoty:

    __name = "airports"

    def insert(self, airport: AirportsHub) -> str:
        return f"""INSERT INTO {self.__name} (airports_has_key, load_date, record_source,iata_name)
                VALUES ({airport.airports_hash}, 
                {airport.load_date}, 
                {airport.record_source},
                {airport.iata_name});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                airports_has_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                iata_name varchar(3));
            """


class AircraftsHubRepositoty:

    __name = "aircrafts"

    def insert(self, aircraft: AircraftsHub) -> str:
        return f"""INSERT INTO {self.__name} (airports_has_key, load_date, record_source,iata_name)
                VALUES ({aircraft.aircrafts_hash}, 
                {aircraft.load_date}, 
                {aircraft.record_source},
                {aircraft.iata_name});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                aircrafts_has_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                iata_name varchar(3));
            """


class TicketsHubRepositoty:

    __name = "tickets"

    def insert(self, ticket: TicketsHub) -> str:
        return f"""INSERT INTO {self.__name} (tickets_has_key, load_date, record_source,number)
                VALUES ({ticket.ticket_hash}, 
                {ticket.load_date}, 
                {ticket.record_source},
                {ticket.number});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                tickets_has_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                number varchar(10));
            """


class PassengersHubRepositoty:

    __name = "passengers"

    def insert(self, passenger: PassengersHub) -> str:
        return f"""INSERT INTO {self.__name} (passengers_has_key, load_date, record_source,passport_series,passport_number)
                VALUES ({passenger.passenger_hash}, 
                {passenger.load_date}, 
                {passenger.record_source},
                {passenger.passport_series},
                {passenger.passport_number});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                passengers_has_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                passport_series varchar(4)),
                passport_number varchar(6));
            """


class AirlinesHubRepositoty:

    __name = "airlines"

    def insert(self, airline: AirlinesHub) -> str:
        return f"""INSERT INTO {self.__name} (passengers_has_key, load_date, record_source,icao_name,name)
                VALUES ({airline.airline_hash}, 
                {airline.load_date}, 
                {airline.record_source},
                {airline.icao_name},
                {airline.name});
                """

    def create_table(self) -> str:
        return f""" CREATE TABLE IF NOT EXISTS {self.__name} (
                airlines_has_key varchar(32) PRIMARY KEY,
                load_date timestamp WITH TIME ZONE,
                record_source varchar(150),
                icao_name varchar(2)),
                name varchar(50));
            """
