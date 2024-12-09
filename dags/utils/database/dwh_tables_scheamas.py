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


class SeatClassesHub:
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        seat_class_hash: str,
        seat_class: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__seat_class_hash = seat_class_hash
        self.__seat_class = seat_class

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        status_hash: str,
        schedules_hash: str,
        status_info_hash: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__status_hash = status_hash
        self.__schedules_hash = schedules_hash
        self.__status_info_hash = status_info_hash

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        flight_hash: str,
        schedules_hash: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__flight_hash = flight_hash
        self.__schedules_hash = schedules_hash

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        flight_hash: str,
        airport_hash: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__flight_hash = flight_hash
        self.__airport_hash = airport_hash

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
    def airport_hash(self) -> str:
        return self.__airport_hash


class AircraftNumsLink:
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        airline_hash: str,
        aircraft_hash: str,
        aircraft_num_hash: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__airline_hash = airline_hash
        self.__aircraft_hash = aircraft_hash
        self.__aircraft_num_hash = aircraft_num_hash

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        aircraft_hash: str,
        flight_hash: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__aircraft_hash = aircraft_hash
        self.__flight_hash = flight_hash

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        ticket_hash: str,
        flight_hash: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__ticket_hash = ticket_hash
        self.__flight_hash = flight_hash

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        ticket_hash: str,
        seat_class_hash: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__ticket_hash = ticket_hash
        self.__seat_class_hash = seat_class_hash

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


class PassengerLink:
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        ticket_hash: str,
        passenger_hash: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__ticket_hash = ticket_hash
        self.__passenger_hash = passenger_hash

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        status_info_hash: str,
        set_status_time: datetime,
        unset_status_time: datetime,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__status_info_hash = status_info_hash
        self.__set_status_time = set_status_time
        self.__unset_status_time = unset_status_time

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        schedules_hash: str,
        actual_arrival_time: datetime,
        actual_departure_time: datetime,
        planned_arrival_time: datetime,
        planned_departure_time: datetime,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__schedules_hash = schedules_hash
        self.__actual_arrival_time = actual_arrival_time
        self.__actual_departure_time = actual_departure_time
        self.__planned_arrival_time = planned_arrival_time
        self.__planned_departure_time = planned_departure_time

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        flight_hash: str,
        weight: int,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__flight_hash = flight_hash
        self.__weight = weight

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        flight_hash: str,
        registration_time: datetime,
        is_ramp: bool,
        gate: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__flight_hash = flight_hash
        self.__registration_time = registration_time
        self.__is_ramp = is_ramp
        self.__gate = gate

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


class AirportsSat:
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        airport_hash: str,
        name: str,
        city: str,
        timezone: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__airport_hash = airport_hash
        self.__name = name
        self.__city = city
        self.__timezone = timezone

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        aircraft_hash: str,
        name: str,
        seats_num: int,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__aircraft_hash = aircraft_hash
        self.__name = name
        self.__seats_num = seats_num

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        aircraft_num_hash: str,
        registration_time: datetime,
        deregistration_time: datetime,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__aircraft_num_hash = aircraft_num_hash
        self.__registration_time = registration_time
        self.__deregistration_time = deregistration_time

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        ticket_hash: str,
        cost: float,
        baggage_weight: float,
        is_registred: bool,
        seat_num: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__ticket_hash = ticket_hash
        self.__cost = cost
        self.__baggage_weight = baggage_weight
        self.__is_registred = is_registred
        self.__seat_num = seat_num

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
    def __init__(
        self,
        load_date: datetime,
        record_source: str,
        passenger_hash: str,
        name: str,
        surname: str,
    ):
        self.__load_date = load_date
        self.__record_source = record_source
        self.__passenger_hash = passenger_hash
        self.__name = name
        self.__surname = surname

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
