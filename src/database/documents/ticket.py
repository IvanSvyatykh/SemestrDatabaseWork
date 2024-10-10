from mongoengine import Document, StringField , ReferenceField


class Ticket(Document):
    meta = {"db_alias": "airport", "collection": "tickets"}
    passenger = ReferenceField("passengers")
    
