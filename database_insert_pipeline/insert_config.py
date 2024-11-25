import os
from dotenv import load_dotenv

load_dotenv()


MONGODB_USERNAME = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGODB_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGODB_DOMAIN = os.getenv("DOMAIN")
MONGODB_PORT = os.getenv("MONGODB_PORT")
DEFAULT_TIMEZONE = os.getenv("TIMEZONE")
