import os
from mongoengine import connect
from mongoengine.connection import disconnect
from dotenv import dotenv_values
from config import config


class DatabaseConfig:
    def __init__(self):
        self.db_client = None
        self.url = f"mongodb://{config.MONGO_ROOT_USERNAME}:{config.ME_CONFIG_BASICAUTH_PASSWORD}@{config.MONGODB_DOMAIN}:{config.MONGODB_PORT}"

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
