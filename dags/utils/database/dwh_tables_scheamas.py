from datetime import datetime


class StatusHub:

    def __init__(
        self,
        status_hash: str,
        load_date: datetime,
        record_source: str,
        status: str,
    ):
        self.__status_hash = status_hash
        self.__load_date = load_date
        self.__record_source = record_source
        self.__status = status

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        schedules_hash: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__schedules_hash = schedules_hash

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        flights_hash: str,
        flight_num: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__flights_hash = flights_hash
        self.__flight_num = flight_num

    @property
    def load_date(self) -> str:
        return self.__load_date

    @property
    def record_source(self) -> str:
        return self.__record_source

    @property
    def flights_hash(self) -> str:
        return self.__flights_hash

    @property
    def flight_num(self) -> str:
        return self.__flight_num


class AirportsHub:
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        airports_hash: str,
        iata_name: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__airports_hash = airports_hash
        self.__iata_name = iata_name

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        aircrafts_hash: str,
        iata_name: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__aircrafts_hash = aircrafts_hash
        self.__iata_name = iata_name

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        ticket_hash: str,
        number: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__ticket_hash = ticket_hash
        self.__number = number

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        passenger_hash: str,
        passport_number: str,
        passport_series: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__passenger_hash = passenger_hash
        self.__passport_number = passport_number
        self.__passport_series = passport_series

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        airline_hash: str,
        icao_name: str,
        name: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__airline_hash = airline_hash
        self.__icao_name = icao_name
        self.__name = name

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
