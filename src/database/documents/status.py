from mongoengine import Document, StringField


class Status(Document):
    meta = {"db_alias":"airport","collection":"statuses"}
    status = StringField(max_length=10, required=True, unique=True)
