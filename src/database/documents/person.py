from mongoengine import Document, StringField


class Person(Document):
    meta = {"db_alias": "airport", "collection": "person"}
    name = StringField(max_length=50)
    surname = StringField(max_length=50)
