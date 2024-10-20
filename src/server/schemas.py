from typing import Optional, Annotated
from pydantic import BaseModel, field_validator, ValidationError, Field
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
    passport_ser: str
    passport_num: str

    @field_validator("passport_ser")
    @classmethod
    def validate_passport_ser(cls, pass_ser: str) -> str:
        if len(pass_ser) == 4 and pass_ser.isdigit():
            return pass_ser
        raise ValidationError(
            "The passport series must be a 4-line long and consist only of digits."
        )

    @field_validator("passport_num")
    @classmethod
    def validate_passport_num(cls, pass_num: str) -> str:
        if len(pass_num) == 6 and pass_num.isdigit():
            return pass_num
        raise ValidationError(
            "The passport number must be a 6-line long and consist only of digits."
        )

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
        max_length=4, pattern=r"/^[A-Z]{1}[A-Z0-9]{1,3}$/"
    )
    aircraft_id: str = Field(
        max_length=6,
        pattern=r"/^[A-Z]-[A-Z]{4}|[A-Z]{2}-[A-Z]{3}|N[0-9]{1,5}[A-Z]{0,2}$/",
    )
    aircraft_name: str = Field(max_length=50)
    seats_num: int = Field(gt=0, le=853)


class Airline(BaseModel):
    id: Annotated[Optional[str], AfterValidator(validate_object_id)] = (
        Field(max_length=24, default=None)
    )
    name: str = Field(max_length=50)
    icao_name: str = Field(max_length=3, pattern=r"/^[A-Z]{3}$/")
