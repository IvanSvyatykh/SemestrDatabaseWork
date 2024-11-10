from mongoengine import (
    Document,
    StringField,
    ReferenceField,
    EmbeddedDocument,
    DynamicField,
    BooleanField,
    DateTimeField,
    FloatField,
    IntField,
    EmbeddedDocumentField,
)
from pydantic import ValidationError


def validate_types(object):
    if not isinstance(
        object, (PassengerFlightDocument, CargoFlightDocument)
    ):
        raise ValidationError(
            "Flight information should be instance of PassengerFlight or CargoFlight "
        )


class AirportDocument(Document):
    meta = {"db_alias": "airport", "collection": "airports"}
    iata_name = StringField(
        max_length=3, required=True, unique=True, regex=r"^[A-Z]{3}"
    )
    name = StringField(max_length=50, required=True, unique=True)
    city = StringField(max_length=50, required=True)
    timezone = StringField(max_length=50, required=True)


class PassportDocument(EmbeddedDocument):
    meta = {"db_alias": "airport"}
    series = StringField(max_length=4, unique_with="number")
    number = StringField(max_length=6)


class PassengerDocument(Document):
    meta = {"db_alias": "airport", "collection": "passengers"}
    name = StringField(required=True, max_length=50)
    surname = StringField(max_length=50)
    passport = EmbeddedDocumentField(PassportDocument)


class FairCondDocument(Document):
    meta = {"db_alias": "airport", "collection": "seat_classes"}
    fare_conditions = StringField(max_length=10, unique=True)


class AircraftDocument(Document):
    meta = {"db_alias": "airport", "collection": "aircrafts"}
    iata_name = StringField(
        max_length=3, required=True, regex=r"^[A-Z0-9]{3}$"
    )
    name = StringField(max_length=50, required=True)
    seats_num = IntField(min_value=1, max_value=853, required=True)


class AircraftNumberDocument(Document):
    meta = {"db_alias": "airport", "collection": "aircraft_ids"}
    aircraft_id = ReferenceField("AircraftDocument", required=True)
    aircraft_num = StringField(
        min_length=6,
        max_length=6,
        required=True,
        unique=True,
        regex=r"^[A-Z]-[A-Z]{4}|[A-Z]{2}-[A-Z]{3}|N[0-9]{2}[A-Z]{3}$",
    )
    airline = ReferenceField("AirlineDocument", required=True)
    registration_time = DateTimeField(required=True)
    deregistartion_time = DateTimeField(required=True)


class AirlineDocument(Document):
    meta = {"db_alias": "airport", "collection": "airlines"}
    name = StringField(max_length=50, required=True, unique=True)
    icao_name = StringField(
        max_length=2, required=True, unique=True, regex=r"^[A-Z]{2}$"
    )


class StatusDocument(Document):
    meta = {"db_alias": "airport", "collection": "statuses"}
    status = StringField(max_length=10, required=True, unique=True)


class StatusHistoryDocument(Document):
    meta = {"db_alias": "airport", "collection": "statuses_info"}
    status = ReferenceField("StatusDocument", required=True)
    schedule = ReferenceField("ScheduleDocument", required=True)
    set_status_time = DateTimeField(required=True)
    unset_status_time = DateTimeField(required=True)


class ScheduleDocument(Document):
    meta = {"db_alias": "airport", "collection": "schedules"}
    arrival_time = DateTimeField(required=True)
    departure_time = DateTimeField(required=True)
    actual_arrival = DateTimeField()
    actual_departure = DateTimeField()


class CargoFlightDocument(EmbeddedDocument):
    weight = FloatField(min_value=0, max_value=253.8)


class PassengerFlightDocument(EmbeddedDocument):
    gate = StringField(
        max_length=4, regex=r"^[A-Z]{2}[0-9]{1,2}|[A-Z]{1}[0-9]{1,2}$"
    )
    is_ramp = BooleanField(required=True)
    registration_time = DateTimeField()


class FlightDocument(Document):
    meta = {"db_alias": "airport", "collection": "flights"}
    flight_number = StringField(
        max_length=6, required=True, unique_with="schedule"
    )
    aircraft = ReferenceField("AircraftDocument", required=True)
    arrival_airport = ReferenceField("AirportDocument", required=True)
    departure_airport = ReferenceField("AirportDocument", required=True)
    schedule = ReferenceField("ScheduleDocument", required=True)
    info = DynamicField(required=True, validation=validate_types)


class TicketDocument(Document):
    meta = {"db_alias": "airport", "collection": "tickets"}
    passenger = ReferenceField("PassengerDocument", required=True)
    fare_conditions = ReferenceField("FairCondDocument", required=True)
    flight = ReferenceField(
        "FlightDocument", unique_with="seat_num", required=True
    )
    number = StringField(
        max_length=10,
        required=True,
        unique=True,
        regex=r"^[0-9]{10}$",
    )
    cost = FloatField(min_value=0, required=True)
    baggage_weight = IntField(min_value=0, required=True)
    is_registred = BooleanField(required=True)
    seat_num = StringField(
        required=True,
        regex=r"^[1-9]{1}[0-9]{1,2}[A-Z]{1}|[1-9]{1}[A-Z]{1}$",
        unique_with="flight",
    )
