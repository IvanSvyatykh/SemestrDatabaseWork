import datetime

import os
from typing import Optional, Annotated
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pydantic import (
    BaseModel,
    ValidationInfo,
    field_validator,
    ValidationError,
    Field,
    model_validator,
)
from bson import ObjectId
from pydantic.functional_validators import AfterValidator


def validate_object_id_field(
    object_id: str, can_be_none: bool = True
) -> str:

    if object_id is None and can_be_none:
        return object_id
    elif not can_be_none:
        ObjectId(object_id)
    return object_id


class Passenger(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    name: str
    surname: str
    passport_ser: str = Field(max_length=4, pattern=r"^([0-9]{4})$")
    passport_num: str = Field(max_length=6, pattern=r"^([0-9]{6})$")

    @field_validator("name")
    @classmethod
    def transform_name(cls, name: str) -> str:
        return name.upper()

    @field_validator("surname")
    @classmethod
    def transform_surname(cls, surname: str) -> str:
        return surname.upper()


class FairCondition(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    fare_condition: str = Field(max_length=10)


class Airport(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    iata_name: str = Field(
        max_length=3,
        pattern=r"/^[A-Z]{3}$/",
    )
    airport_name: str = Field(max_length=50)
    city: str = Field(max_length=50)
    timezone: str = Field(max_length=50)

    @field_validator("airport_name")
    @classmethod
    def transform_name(cls, name: str) -> str:
        return name.upper()

    @field_validator("city")
    @classmethod
    def transform_surname(cls, surname: str) -> str:
        return surname.upper()


class Aircraft(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    iata_name: str = Field(max_length=3, pattern=r"^[A-Z0-9]{3}$")
    aircraft_name: str = Field(max_length=50)
    seats_num: int = Field(gt=0, le=853)


class AircraftNumber(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    aircraft_id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)

    aircraft_num: str = Field(
        max_length=6,
        min_length=6,
        pattern=r"^[A-Z]-[A-Z]{4}|[A-Z]{2}-[A-Z]{3}|N[0-9]{3}[A-Z]{3}$",
    )
    airline: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24)
    registration_time: datetime.datetime = Field(
        default=datetime.datetime.today()
    )
    load_dotenv()
    derigistration_time: datetime.datetime = Field(
        default=datetime.datetime(
            year=9999,
            month=12,
            day=31,
            hour=23,
            minute=59,
            second=59,
            tzinfo=ZoneInfo(os.getenv("TIMEZONE")),
        )
    )

    @model_validator(mode="after")
    def validate(self) -> str:
        if self.registration_time >= self.derigistration_time:
            raise ValidationError(
                "Registred time can not be more or equal to derigisrated time"
            )

        return self


class Airline(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    name: str = Field(max_length=50)
    iata_name: str = Field(max_length=2, pattern=r"^[A-Z]{2}$")


class PassengerFlightInfo(BaseModel):
    gate: str = Field(
        max_length=4, pattern=r"^[A-Z]{2}[0-9]{1,2}|[A-Z]{1}[0-9]{1,2}$"
    )
    is_ramp: bool
    registration_time: datetime.datetime


class CargoFlightInfo(BaseModel):
    weight: int = Field(gt=0, le=640000)


class Flight(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    flight_number: str = Field(max_length=6)
    aircraft: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24)
    arrival_airport: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24)
    departure_airport: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24)
    schedule: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24)
    info: PassengerFlightInfo | CargoFlightInfo


class Status(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    status: str = Field(max_length=10)


class Schedule(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    arrival_time: datetime.datetime
    departure_time: datetime.datetime
    actual_arrival: datetime.datetime
    actual_departure: datetime.datetime

    @model_validator(mode="after")
    def validate(self) -> str:
        if self.departure_time >= self.arrival_time:
            raise ValidationError(
                "Departure time can not be more or equal to arrival time"
            )

        if self.actual_departure >= self.actual_arrival:
            raise ValidationError(
                "Actual departure time can not be more or equal to actual arrival time"
            )

        return self


class StatusHistory(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    status_id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    schedule_id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)
    set_status_time: datetime.datetime = Field(
        default=datetime.datetime.today()
    )
    unset_status_time: datetime.datetime = Field(
        default=datetime.datetime(
            year=9999,
            month=12,
            day=31,
            hour=23,
            minute=59,
            second=59,
            tzinfo=ZoneInfo("Asia/Yekaterinburg"),
        )
    )

    @model_validator(mode="after")
    def validate(self) -> str:
        if self.set_status_time >= self.unset_status_time:
            raise ValidationError(
                "Registred time can not be more or equal to derigisrated time"
            )

        return self


class Ticket(BaseModel):
    id: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24, default=None)

    passenger: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24)

    fare_condition: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24)

    flight: Annotated[
        Optional[str], AfterValidator(validate_object_id_field)
    ] = Field(max_length=24)

    number: str = Field(max_length=13, pattern=r"^([0-9]{13})$")

    cost: float = Field(ge=0)

    baggage_weight: int = Field(ge=0)

    is_registred: bool = Field(default=False)

    seat_num: str = Field(max_length=4, pattern=r"^([1-9]{1,3}[A-Z]{1})$")
