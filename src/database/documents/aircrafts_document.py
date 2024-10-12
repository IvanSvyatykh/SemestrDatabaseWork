from mongoengine import Document, StringField, IntField


class AircraftDocument(Document):
    meta = {"db_alias": "airport", "collection": "aircrafts"}
    icao_name = StringField(max_length=4, required=True, unique=True)
    aircraft_id = StringField(
        max_length=6, required=True, unique=True, primary_key=True
    )
    name = StringField(max_length=50, required=True, unique=True)
    seats_num = IntField(min_value=1, max_value=555, required=True)
