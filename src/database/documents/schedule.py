from mongoengine import Document, DateTimeField, ReferenceField


class Schedule(Document):
    meta = {"db_alias": "airport", "collection": "schedules"}
    arrival_time = DateTimeField(required=True)
    departure_time = DateTimeField(required=True)
    arrival_time_real = DateTimeField()
    departure_time_real = DateTimeField()
    status = ReferenceField("statuses")
