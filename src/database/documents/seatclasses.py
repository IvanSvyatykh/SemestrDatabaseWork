from mongoengine import Document, StringField


class SeatClass(Document):
    meta = {"db_alias": "airport", "collection": "seat_classes"}
    category = StringField(max_length=10, unique=True)
