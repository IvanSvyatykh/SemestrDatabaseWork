from tkinter.tix import Tree
from mongoengine import Document, StringField


class Airline(Document):
    meta = {"db_alias": "airport", "collection": "airlines"}
    name = StringField(max_length=50, required=True, unique=True)
    iata_name = StringField(max_length=3, required=True, unique=True, primary_key=True)
