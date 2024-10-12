from mongoengine import StringField, Document


class RunwayConditionDocument(Document):
    meta = {"db_alias": "airport", "collection": "runway_conditions"}
    runway_condition = StringField(max_length=50, required=True, unique=True)
