import os
from mongoengine import connect
from mongoengine.connection import disconnect
from dotenv import dotenv_values
from config.config import (
    MONGODB_USERNAME,
    MONGODB_PASSWORD,
    MONGODB_PORT,
    MONGODB_DOMAIN,
)


class DatabaseConfig:
    def __init__(self):
        self.db_client = None
        self.url = f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_DOMAIN}:{MONGODB_PORT}"

    def start_connection(self, db_name: str):
        if self.db_client is None:
            self.db_client = connect(
                db=db_name,
                # username=self.config["MONGO_INITDB_ROOT_USERNAME"],
                # password=self.config["MONGO_INITDB_ROOT_PASSWORD"],
                host=self.url,
                alias="airport",
            )

    def disconnect(self):
        disconnect(self.url)
