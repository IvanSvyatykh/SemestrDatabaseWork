from mongoengine import (
    Document,
    StringField,
    ReferenceField,
    FloatField,
    IntField,
    BooleanField,
)


class Ticket(Document):
    meta = {"db_alias": "airport", "collection": "tickets"}
    passenger = ReferenceField("passengers", required=True)
    fare_conditions = ReferenceField("seat_classes", required=True)
    flight = ReferenceField("flights", required=True)
    cost = FloatField(min_value=0, required=True)
    baggage_weight = IntField(min_value=0, required=True)
    is_registred = BooleanField(required=True)
