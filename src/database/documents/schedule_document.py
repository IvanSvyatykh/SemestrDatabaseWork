from mongoengine import Document, DateTimeField, ReferenceField


class ScheduleDocument(Document):
    meta = {"db_alias": "airport", "collection": "schedules"}
    arrival_time = DateTimeField(required=True)
    departure_time = DateTimeField(required=True)
    actual_arrival = DateTimeField()
    actual_departure = DateTimeField()
    status = ReferenceField("statuses")
