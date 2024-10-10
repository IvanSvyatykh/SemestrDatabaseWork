from mongoengine import Document, IntField, ReferenceField, DateTimeField


class Weather(Document):
    meta = {"db_alias": "airport", "collection": "weathers"}
    runway_condition = ReferenceField("runway_conditions", required=True)
    wind_speed = IntField(min_value=0, required=True)
    rainfall_amount = IntField(min_value=0, required=True)
    temperature = IntField(min_value=-100, max_value=100, required=True)
    started_time = DateTimeField(required=True)
    ended_time = DateTimeField()
