from mongoengine import Document, StringField


class Airport(Document):
    meta = {"db_alias": "airport", "collection": "airports"}
    iata_name = StringField(max_length=3, required=True, unique=True, primary_key=True)
    name = StringField(max_length=50, required=True, unique=True, primary_key=True)
    city = StringField(max_length=50, required=True, unique=True, primary_key=True)
