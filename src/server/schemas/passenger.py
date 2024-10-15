from pydantic import (
    BaseModel,
    field_validator,
    ValidationError,
)


class Passenger(BaseModel):
    name: str
    surname: str
    passport_ser: str
    passport_num: str

    @field_validator("passport_ser")
    @classmethod
    def validate_passport_ser(cls, pass_ser: str) -> str:
        if len(pass_ser) == 4 and pass_ser.isdigit():
            return pass_ser
        return ValidationError(
            "The passport series must be a 4-line long and consist only of digits."
        )

    @field_validator("passport_num")
    @classmethod
    def validate_passport_num(cls, pass_num: str) -> str:
        if len(pass_num) == 6 and pass_num.isdigit():
            return pass_num
        return ValidationError(
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
