from mongoengine import Document, DateTimeField, ReferenceField


class Schedule(Document):
    meta = {"db_alias": "airport", "collection": "schedules"}
    arrival_time = DateTimeField(required=True)
    departure_time = DateTimeField(required=True)
    actual_arrival = DateTimeField()
    actual_departure = DateTimeField()
    status = ReferenceField("statuses")
