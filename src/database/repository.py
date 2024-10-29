from ast import List
import asyncio
from datetime import datetime
import re
from zoneinfo import ZoneInfo
from .documents import (
    AircraftDocument,
    AircraftNumberDocument,
    AirlineDocument,
    AirportDocument,
    CargoFlightDocument,
    FlightDocument,
    PassengerDocument,
    PassengerFlightDocument,
    PassportDocument,
    ScheduleDocument,
    SeatClassDocument,
    StatusDocument,
    StatusInfoDocument,
)
from server.schemas import (
    Aircraft,
    AircraftNumber,
    Airline,
    Airport,
    Flight,
    Passenger,
    PassengerFlightInfo,
    Schedule,
    SeatClass,
    Status,
    CargoFlightInfo,
    StatusInfo,
)
from mongoengine import Q
from mongoengine.fields import ObjectId

MAX_DB_DATETIME = datetime.datetime(
    year=9999,
    month=12,
    day=31,
    hour=23,
    minute=59,
    second=59,
    tzinfo=ZoneInfo("Asia/Yekaterinburg"),
)


class PassengerRepository:

    def __init__(self):
        self.passenger = PassengerDocument()

    async def add(self, passenger: Passenger) -> ObjectId:
        passenger_db_document = PassengerDocument.objects(
            Q(passport__number=passenger.passport_num)
            & Q(passport__series=passenger.passport_ser)
        ).first()

        if passenger_db_document is not None:
            raise ValueError("Person with those passport already exist!")

        self.passenger.name = passenger.name
        self.passenger.surname = passenger.surname
        self.passenger.passport = PassportDocument(
            series=passenger.passport_ser,
            number=passenger.passport_num,
        )
        self.passenger.save()
        return ObjectId(str(self.passenger.pk))

    async def delete(self, passenger: Passenger) -> None:
        passenger_db_document = PassengerDocument.objects(
            Q(passport__number=passenger.passport_num)
            & Q(passport__series=passenger.passport_ser)
        ).first()

        if passenger_db_document is not None:
            raise ValueError("Person with those passport already exist!")
        passenger_db_document.delete()

    async def update(self, passenger: Passenger) -> None:
        passenger_db_document = PassengerDocument.objects(
            Q(passport__number=passenger.passport_num)
            & Q(passport__series=passenger.passport_ser)
        ).first()

        if passenger_db_document is not None:
            raise ValueError("Person with those passport already exist!")
        passenger_db_document.update(
            set__name=passenger.name,
            set__surname=passenger.surname,
            set__passport=PassportDocument(
                series=passenger.passport_ser,
                number=passenger.passport_num,
            ),
        )

    async def get_by_id(self, oid: ObjectId) -> Passenger:
        passenger_db_document = PassengerDocument.objects(id=oid).first()
        if passenger_db_document is None:
            raise ValueError("There is not person with these ObjectId!")
        return Passenger(
            id=str(passenger_db_document.pk),
            name=passenger_db_document.name,
            surname=passenger_db_document.surname,
            passport_num=passenger_db_document.passport.number,
            passport_ser=passenger_db_document.passport.series,
        )

    async def get_by_passport(
        self, pass_series: str, pass_num: str
    ) -> Passenger:
        passenger_db_document = PassengerDocument.objects(
            Q(passport__number=pass_num) & Q(passport__series=pass_series)
        ).first()
        if len(passenger_db_document) == 0:
            raise ValueError("There is not person with these passport!")

        return Passenger(
            id=str(passenger_db_document.pk),
            name=passenger_db_document.name,
            surname=passenger_db_document.surname,
            passport_num=passenger_db_document.passport.number,
            passport_ser=passenger_db_document.passport.series,
        )


class SeatClassRepository:

    def __init__(self):
        self.seat_class = SeatClassDocument()

    async def add(self, s_class: SeatClass) -> ObjectId:
        seat_class_db_document = SeatClassDocument.objects(
            fare_conditions=s_class.fare_condition
        ).first()

        if seat_class_db_document is None:
            self.seat_class.fare_conditions = s_class.fare_condition
            self.seat_class.save()
            return ObjectId(str(self.seat_class.pk))
        raise ValueError("Seat class with this name already exist!")

    async def delete(self, s_class: SeatClass) -> None:
        seat_class_db_document = SeatClassDocument.objects(
            fare_conditions=s_class.fare_condition
        ).first()

        if seat_class_db_document is None:
            raise ValueError("There is not class with this name!")
        seat_class_db_document.delete()

    async def get_by_class_name(self, class_name: str) -> SeatClass:
        seat_class_db_document = SeatClassDocument.objects(
            fare_conditions=class_name
        ).first()

        if seat_class_db_document is None:
            raise ValueError("There is not class with this name!")
        return SeatClass(
            id=str(seat_class_db_document.pk),
            fare_condition=seat_class_db_document.fare_conditions,
        )

    async def get_by_id(self, oid: ObjectId) -> SeatClass:
        seat_class_db_document = SeatClassDocument.objects(id=oid).first()

        if seat_class_db_document is None:
            raise ValueError("There is not class with this name!")
        return SeatClass(
            id=str(seat_class_db_document.pk),
            fare_condition=seat_class_db_document.fare_conditions,
        )


class AirportRepository:

    def __init__(self):
        self.airport_document = AirportDocument()

    async def add(self, airport: Airport) -> ObjectId:

        airport_document = AirportDocument.objects(
            name=airport.airport_name
        ).first()

        if airport_document is not None:
            raise ValueError("Airport with this name alredy exist!")

        self.airport_document.icao_name = airport.airport_name
        self.airport_document.name = airport.airport_name
        self.airport_document.city = airport.city
        self.airport_document.save()
        return ObjectId(str(self.airport_document.pk))

    async def delete(self, airport: Airport) -> None:

        airport_document = AirportDocument.objects(
            name=airport.airport_name
        ).first()

        if airport_document is None:
            raise ValueError("There is no airport with this icao name!")

        airport_document.delete()

    async def get_by_name(self, name: str) -> Airport:
        airport_document = AirportDocument.objects(name=name).first()

        if airport_document is None:
            raise ValueError("There is no airport with this name!")

        return Airport(
            id=str(airport_document.pk),
            iata_name=airport_document.iata_name,
            city=airport_document.city,
        )

    async def get_by_id(self, oid: ObjectId) -> Airport:
        airport = AircraftDocument.objects(id=oid).first()
        if airport is None:
            raise ValueError("There is not airport with this id!")

        return Airport(
            id=str(airport.pk),
            icao_name=airport.icao_name,
            city=airport.city,
            airport_name=airport.name,
        )


class AircraftRepository:

    def __init__(self):
        self.aircraft = AircraftDocument()

    async def add(self, aircraft: Aircraft) -> ObjectId:

        self.aircraft.icao_name = aircraft.icao_name
        self.aircraft.name = aircraft.aircraft_name
        self.aircraft.seats_num = aircraft.seats_num
        self.aircraft.save()
        return ObjectId(str(self.aircraft.pk))

    async def delete(self, oid: ObjectId) -> None:
        aircraft_document = AircraftDocument.objects(id=oid).first()

        if aircraft_document is None:
            raise ValueError("There is no aircraft wiht this object id!")

        aircraft_document.delete()

    async def update(self, aircraft: Aircraft, oid: ObjectId) -> None:
        aircraft_document = AircraftDocument.objects(id=oid).first()

        if aircraft_document is None:
            raise ValueError("There is no aircraft wiht this object id!")

        aircraft_document.update(
            set__icao_name=aircraft.icao_name,
            set__name=aircraft.aircraft_name,
            set__seats_num=aircraft.seats_num,
        )

    async def get_by_id(self, oid: ObjectId) -> Aircraft:
        aircraft_document = AircraftDocument.objects(id=oid).first()

        if aircraft_document is None:
            raise ValueError("There is no aircraft wiht this object id!")

        return Aircraft(
            id=str(aircraft_document.pk),
            icao_name=aircraft_document.icao_name,
            aircraft_name=aircraft_document.name,
            seats_num=aircraft_document.seat_num,
        )

    async def get_by_aircraft_number(self, aircraft_num: str) -> Aircraft:

        aircraft_num_rep = AircraftNumberRepository()
        get_aircraft_task = asyncio.create_task(
            aircraft_num_rep.get_by_aircraft_number(aircraft_num)
        )
        aircraft = await get_aircraft_task

        return aircraft


class AircraftNumberRepository:
    def __init__(self):
        self.aircraft_num = AircraftNumberDocument()

    async def add(self, aircraft_number: AircraftNumber) -> ObjectId:

        aircraft_number_document = AircraftNumberDocument.objects(
            Q(aircraft_id=aircraft_number.aircraft_id)
            & Q(deregistartion_time__lt=aircraft_number.registration_time)
        ).first()

        if aircraft_number_document is not None:
            raise ValueError(
                "Aircraft number can not be add, because it was not deregistred!"
            )

        self.aircraft_num.aircraft_number = aircraft_number.aircraft_number
        self.aircraft_num.aircraft_id = aircraft_number.aircraft_id
        self.aircraft_num.registration_time = (
            aircraft_number.registration_time
        )
        self.aircraft_num.deregistartion_time = (
            aircraft_number.derigistration_time
        )
        self.aircraft_num.save()
        return ObjectId(str(self.aircraft_num.pk))

    async def update_deregistartion_time(
        self, aircraft_number: AircraftNumber
    ) -> None:
        aircraft_number_document = AircraftNumberDocument.objects(
            Q(aircraft_num=aircraft_number.aircraft_num)
            & Q(registration_time=aircraft_number.registration_time)
            & Q(deregistartion_time=MAX_DB_DATETIME)
        ).first()

        if aircraft_number_document is None:
            raise ValueError(
                "Derigistred time can not be set, because aircraft with this number does not exist or alredy deregistred!"
            )

        aircraft_number_document.update(
            set__deregistartion_time=aircraft_number.derigistration_time
        )

    async def get_by_aircraft_number(self, number: str) -> AircraftNumber:

        if not re.match(
            pattern=r"^[A-Z]-[A-Z]{4}|[A-Z]{2}-[A-Z]{3}|N[0-9]{1,5}[A-Z]{0,2}$",
            string=number,
        ):
            raise ValueError("Aircraft id is not correct !")

        aircraft_number_document = AircraftNumberDocument.objects(
            Q(aircraft_num=number)
        ).first()

        if aircraft_number_document is None:
            raise ValueError("Aircraft with this number does not exist!")

        return AircraftNumber(
            id=aircraft_number_document.pk,
            aircraft_id=aircraft_number_document.aircraft_id,
            aircraft_num=aircraft_number_document.aircraft_num,
            registration_time=aircraft_number_document.registration_time,
            derigistration_time=aircraft_number_document.derigistration_time,
        )


class AirlineRepository:

    def __init__(self):
        self.airline = AirlineDocument()

    async def add(self, airline: Airline) -> None:
        airline_document = AirlineDocument.objects(
            icao_name=airline.icao_name
        ).first()

        if airline_document is not None:
            raise ValueError(
                "There is alredy exists airline with this icao code!"
            )

        self.airline.icao_name = airline.icao_name
        self.airline.name = airline.name
        self.airline.save()
        return ObjectId(str(self.airline.pk))

    async def delete(self, airline: Airline) -> None:
        airline_document = AirlineDocument.objects(
            icao_name=airline.icao_name
        ).first()

        if airline_document is None:
            raise ValueError("There is no airline with this icao code!")

        airline_document.delete()

    async def update(self, airline: Airline) -> None:
        airline_document = AirlineDocument.objects(
            icao_name=airline.icao_name
        ).first()

        if airline_document is None:
            raise ValueError("There is no airline with this icao code!")

        airline_document.update(
            set__icao_name=airline.icao_name, set__name=airline.name
        )

    async def get_by_id(self, oid: ObjectId) -> Airline:
        airline_document = AirlineDocument.objects(id=oid).first()

        if airline_document is None:
            raise ValueError("There is no airline with this id!")

        return Airline(
            id=str(airline_document.pk),
            name=airline_document.name,
            icao_name=airline_document.icao_name,
        )

    async def get_by_name(self, airline_name: str) -> Airline:
        airline_document = AirlineDocument.objects(
            name=airline_name
        ).first()

        if airline_document is None:
            raise ValueError("There is no airline with this id!")

        return Airline(
            id=str(airline_document.pk),
            name=airline_document.name,
            icao_name=airline_document.icao_name,
        )


class StatusRepositiry:

    def __init__(self):
        self.status = StatusDocument()

    async def add(self, status: Status) -> ObjectId:
        status_document = StatusDocument.objects(
            status=status.status
        ).first()

        if status_document is not None:
            raise ValueError(
                "There is alredy exist status with this name!"
            )

        self.status.status = status.status
        self.status.save()
        return ObjectId(str(self.status.pk))

    async def delete(self, status: Status) -> None:
        status_document = StatusDocument.objects(
            status=status.status
        ).first()

        if status_document is None:
            raise ValueError("There is no status with this name!")

        status_document.delete()

    async def get_by_id(self, oid: ObjectId) -> Status:
        status_document = StatusDocument.objects(id=oid).first()

        if status_document is None:
            raise ValueError("There is no status with this name!")

        return Status(
            id=str(status_document.pk), status=status_document.status
        )

    async def get_by_status_name(self, status_name: str) -> Status:
        status_document = StatusDocument.objects(
            status=status_name
        ).first()

        if status_document is None:
            raise ValueError("There is no status with this name!")

        return Status(
            id=str(status_document.pk), status=status_document.status
        )


class StatusInfoRepository:

    def __init__(self):
        self.status_info = StatusInfoDocument()

    async def add(self, status_info: StatusInfo) -> ObjectId:
        status_info_document = StatusInfoDocument.objects(
            Q(schedule=status_info.schedule_id)
            & Q(unset_status_time__lt=status_info.set_status_time)
        ).first()

        if status_info_document is not None:
            raise ValueError("Previous status was not unseted!")

        self.status_info.status = status_info.status_id
        self.status_info.schedule = status_info.schedule_id
        self.status_info.set_status_time = status_info.set_status_time
        self.status_info.unset_status_time = status_info.unset_status_time

        self.status_info.save()
        return ObjectId(str(self.status_info.pk))

    async def update_unset_time(self, status_info: StatusInfo) -> None:
        status_info_document = StatusInfoDocument.objects(
            Q(schedule=status_info.schedule_id)
            & Q(set_status_time=status_info.set_status_time)
        ).first()

        if status_info_document is None:
            raise ValueError(
                "There is not status information with this schedule and set_time!"
            )

        status_info_document.update(
            set__unset_status_time=status_info.unset_status_time
        )

    async def get_schedule_statuses(
        self, schedule: ObjectId
    ) -> List[StatusInfo]:
        status_info_documents = StatusInfoDocument.objects(
            schedule=schedule
        )
        if len(status_info_documents) == 0:
            raise ValueError(f"There is not statuses with {schedule} id !")

        return status_info_documents


class ScheduleRepository:
    def __init__(self):
        self.schedule = ScheduleDocument()

    async def add(self, schedule: Schedule) -> ObjectId:

        self.schedule.arrival_time = schedule.arrival_time
        self.schedule.actual_arrival = schedule.actual_arrival
        self.schedule.actual_departure = schedule.actual_departure
        self.schedule.departure_time = schedule.departure_time
        self.schedule.save()

        return ObjectId(str(self.schedule.pk))

    async def delete(self, obj_id: ObjectId) -> None:

        schedule_document = ScheduleDocument.objects(oid=obj_id).first()

        if schedule_document is None:
            raise ValueError("There is no schedule with this Object id!")

        schedule_document.delete()

    async def update(self, schedule: Schedule) -> None:

        schedule_document = ScheduleDocument.objects(
            oid=schedule.id
        ).first()

        if schedule_document is None:
            raise ValueError("There is no schedule with this Object id!")

        schedule_document.update(
            set__arrival_time=schedule.arrival_time,
            set__actual_arrival=schedule.actual_arrival,
            set__actual_departure=schedule.actual_departure,
            set__departure_time=schedule.departure_time,
        )


class FlightRepositiry:

    def __init__(self):
        self.flight = FlightDocument()

    async def add(self, flight: Flight) -> ObjectId:
        flight_document = FlightDocument.objects(
            flight_number=flight.flight_number
        ).first()

        if flight_document is not None:
            raise ValueError(
                "There is already exists flight with this number!"
            )

        self.flight.flight_number = flight.flight_number
        self.flight.aircraft = flight.aircraft
        self.flight.airline = flight.airline
        self.flight.arrival_airport = flight.arrival_airport
        self.flight.departure_airport = flight.departure_airport
        self.flight.schedule = flight.schedule
        if isinstance(flight.info, PassengerFlightInfo):
            self.flight.info = PassengerFlightDocument(
                gate=flight.info.gate,
                is_ramp=flight.info.is_ramp,
                registration_time=flight.info.registration_time,
            )
        elif isinstance(flight.info, CargoFlightInfo):
            self.flight.info = CargoFlightDocument(
                weight=flight.info.weight
            )
        else:
            raise TypeError(
                f"flight.info can not be type of {type(flight.info)}"
            )

        self.flight.save()
        return ObjectId(str(self.flight.pk))

    async def delete(self, flight: Flight) -> None:
        flight_document = FlightDocument.objects(
            flight_number=flight.flight_number
        ).first()

        if flight_document is None:
            raise ValueError("There is no flight with this number!")

        flight_document.delete()

    async def update(self, flight: Flight) -> None:
        flight_document = FlightDocument.objects(
            flight_number=flight.flight_number
        ).first()

        if flight_document is None:
            raise ValueError("There is no flight with this number!")

        flight_document.update(
            set__aircraft=flight.aircraft,
            set__airline=flight.airline,
            set__arrival_airport=flight.arrival_airport,
            set__departure_airport=flight.departure_airport,
            set__schedule=flight.schedule,
        )

    async def get_by_flight_num(self, flight_num: str) -> Flight:
        flight_document = FlightDocument.objects(
            flight_number=flight_num
        ).first()

        if flight_document is None:
            raise ValueError("There is no flight with this number!")

        return Flight(
            id=str(flight_document.pk),
            flight_number=flight_document.flight_number,
            airline=flight_document.airline,
            aircraft=flight_document.aircraft,
            arrival_airport=flight_document.arrival_airport,
            departure_airport=flight_document.departure_airport,
            schedule=flight_document.schedule,
            info=flight_document.info,
        )
