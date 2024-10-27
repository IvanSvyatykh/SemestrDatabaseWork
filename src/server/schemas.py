import datetime

from typing import Optional, Annotated
from zoneinfo import ZoneInfo
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


def validate_object_id(object_id: str) -> str:

    if object_id is None:
        return object_id

    ObjectId(object_id)
    return object_id


class Passenger(BaseModel):
    id: Annotated[Optional[str], AfterValidator(validate_object_id)] = (
        Field(max_length=24, default=None)
    )
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


class SeatClass(BaseModel):
    id: Annotated[Optional[str], AfterValidator(validate_object_id)] = (
        Field(max_length=24, default=None)
    )
    fare_condition: str = Field(max_length=10)


class Airport(BaseModel):
    id: Annotated[Optional[str], AfterValidator(validate_object_id)] = (
        Field(max_length=24, default=None)
    )
    icao_name: str = Field(
        max_length=3,
        pattern=r"/^[A-Z]{4}$/",
    )
    airport_name: str = Field(max_length=50)
    city: str = Field(max_length=50)

    @field_validator("airport_name")
    @classmethod
    def transform_name(cls, name: str) -> str:
        return name.upper()

    @field_validator("city")
    @classmethod
    def transform_surname(cls, surname: str) -> str:
        return surname.upper()


class Aircraft(BaseModel):
    id: Annotated[Optional[str], AfterValidator(validate_object_id)] = (
        Field(max_length=24, default=None)
    )
    icao_name: str = Field(
        max_length=4, pattern=r"^[A-Z]{1}[A-Z0-9]{1,3}$"
    )
    aircraft_name: str = Field(max_length=50)
    seats_num: int = Field(gt=0, le=853)


class AircraftNumber(BaseModel):
    id: Annotated[Optional[str], AfterValidator(validate_object_id)] = (
        Field(max_length=24, default=None)
    )

    aircraft_id: Annotated[
        Optional[str], AfterValidator(validate_object_id)
    ] = Field(max_length=24, default=None)

    aircraft_num: str = Field(
        max_length=6,
        min_length=6,
        pattern=r"^[A-Z]-[A-Z]{4}|[A-Z]{2}-[A-Z]{3}|N[0-9]{1,5}[A-Z]{0,2}$",
    )
    registration_time: datetime.datetime = Field(
        default=datetime.datetime.today()
    )
    derigistration_time: datetime.datetime = Field(
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
        if self.registration_time >= self.derigistration_time:
            raise ValidationError(
                "Registred time can not be more or equal to derigisrated time"
            )

        return self


class Airline(BaseModel):
    id: Annotated[Optional[str], AfterValidator(validate_object_id)] = (
        Field(max_length=24, default=None)
    )
    name: str = Field(max_length=50)
    icao_name: str = Field(max_length=3, pattern=r"/^[A-Z]{3}$/")


class Status(BaseModel):
    id: Annotated[Optional[str], AfterValidator(validate_object_id)] = (
        Field(max_length=24, default=None)
    )
    status: str = Field(max_length=10)
