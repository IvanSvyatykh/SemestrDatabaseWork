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


class AirportDocument(Document):
    meta = {"db_alias": "airport", "collection": "airports"}
    icao_name = StringField(
        max_length=4, required=True, unique=True, regex=r"^[A-Z]{4}"
    )
    name = StringField(max_length=50, required=True, unique=True)
    city = StringField(max_length=50, required=True)


class PassportDocument(EmbeddedDocument):
    meta = {"db_alias": "airport"}
    series = StringField(max_length=4, unique_with="number")
    number = StringField(max_length=6)


class PassengerDocument(Document):
    meta = {"db_alias": "airport", "collection": "passengers"}
    name = StringField(required=True, max_length=50)
    surname = StringField(max_length=50)
    passport = EmbeddedDocumentField(PassportDocument)


class SeatClassDocument(Document):
    meta = {"db_alias": "airport", "collection": "seat_classes"}
    fare_conditions = StringField(max_length=10, unique=True)


class AircraftDocument(Document):
    meta = {"db_alias": "airport", "collection": "aircrafts"}
    icao_name = StringField(
        max_length=4, required=True, regex=r"/^[A-Z]{1}[A-Z0-9]{1,3}$/"
    )
    aircraft_id = StringField(
        max_length=6,
        required=True,
        unique=True,
        regex=r"/^[A-Z]-[A-Z]{4}|[A-Z]{2}-[A-Z]{3}|N[0-9]{1,5}[A-Z]{0,2}$/",
    )
    name = StringField(max_length=50, required=True)
    seats_num = IntField(min_value=1, max_value=853, required=True)


class AirlineDocument(Document):
    meta = {"db_alias": "airport", "collection": "airlines"}
    name = StringField(max_length=50, required=True, unique=True)
    icao_name = StringField(
        max_length=4, required=True, unique=True, regex=r"/^[A-Z]{3}$/"
    )


class StatusDocuments(Document):
    meta = {"db_alias": "airport", "collection": "statuses"}
    status = StringField(max_length=10, required=True, unique=True)


class ScheduleDocument(Document):
    meta = {"db_alias": "airport", "collection": "schedules"}
    arrival_time = DateTimeField(required=True)
    departure_time = DateTimeField(required=True)
    actual_arrival = DateTimeField()
    actual_departure = DateTimeField()
    status = ReferenceField("statuses")


class PassengerFlightDocument(EmbeddedDocument):
    gate = StringField(max_length=4)
    is_ramp = BooleanField(required=True)
    registration_time = DateTimeField()


class CargoFlightDocument(EmbeddedDocument):
    weight = FloatField(min_value=0, max_value=253.8)


def validate_types(object):
    if not isinstance(PassengerFlightDocument, CargoFlightDocument):
        raise ValidationError(
            "Flight information should be instance of PassengerFlight or CargoFlight "
        )


class FlightDocument(Document):
    meta = {"db_alias": "airport", "collection": "flights"}
    flight_number = StringField(max_length=6, required=True, unique=True)
    airline = ReferenceField("airlines", required=True)
    aircraft = ReferenceField("aircrafts", required=True)
    arrival_airport = ReferenceField("airports", required=True)
    departure_airport = ReferenceField("airports", required=True)
    shedule = ReferenceField("schedules", required=True)
    info = DynamicField(required=True, validation=validate_types)


class RunwayConditionDocument(Document):
    meta = {"db_alias": "airport", "collection": "runway_conditions"}
    runway_condition = StringField(
        max_length=50, required=True, unique=True
    )


class TicketDocument(Document):
    meta = {"db_alias": "airport", "collection": "tickets"}
    passenger = ReferenceField(
        "passengers", required=True, unique_with="flight"
    )
    fare_conditions = ReferenceField("seat_classes", required=True)
    flight = ReferenceField("flights", required=True)
    cost = FloatField(min_value=0, required=True)
    baggage_weight = IntField(min_value=0, required=True)
    is_registred = BooleanField(required=True)


class WeatherDocument(Document):
    meta = {"db_alias": "airport", "collection": "weathers"}
    runway_condition = ReferenceField("runway_conditions", required=True)
    wind_speed = IntField(min_value=0, required=True)
    rainfall_amount = IntField(min_value=0, required=True)
    temperature = IntField(min_value=-100, max_value=100, required=True)
    started_time = DateTimeField(required=True)
    ended_time = DateTimeField()
