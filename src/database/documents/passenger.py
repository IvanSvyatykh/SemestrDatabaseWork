from mongoengine import (
    EmbeddedDocument,
    Document,
    StringField,
    EmbeddedDocumentField,
)


class Passport(EmbeddedDocument):
    meta = {"db_alias": "airport"}
    series = StringField(max_length=4, unique_with="number")
    number = StringField(max_length=6)


class Passenger(Document):
    meta = {"db_alias": "airport", "collection": "passengers"}
    name = StringField(required=True, max_length=50)
    surname = StringField(max_length=50)
    lastname = StringField(required=True, max_length=50)
    passport = EmbeddedDocumentField(Passport)
