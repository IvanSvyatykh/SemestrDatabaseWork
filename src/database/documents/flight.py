from mongoengine import (
    Document,
    StringField,
    ReferenceField,
    EmbeddedDocument,
    DynamicField,
    BooleanField,
    DateTimeField,
    FloatField,
)
from pydantic import ValidationError


class PassengerFlight(EmbeddedDocument):
    gate = StringField(max_length=4)
    is_ramp = BooleanField(required=True)
    registration_time = DateTimeField()


class CargoFlight(EmbeddedDocument):
    weight = FloatField(min_value=0, max_value=253.8)


def validate_types(object):
    if not isinstance(PassengerFlight, CargoFlight):
        raise ValidationError(
            "Flight information should be instance of PassengerFlight or CargoFlight "
        )


class Flight(Document):
    meta = {"db_alias": "airport", "collection": "flights"}
    flight_number = StringField(max_length=6, required=True)
    airline = ReferenceField("airlines", required=True)
    aircraft = ReferenceField("aircrafts", required=True)
    arrival_airport = ReferenceField("airports", required=True)
    departure_airport = ReferenceField("airports", required=True)
    shedule = ReferenceField("schedules", required=True)
    info = DynamicField(required=True, validation=validate_types)
