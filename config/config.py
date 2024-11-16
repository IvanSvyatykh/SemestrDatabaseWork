import os
from dotenv import load_dotenv

load_dotenv()

MINIO_MONGODB_USERNAME = os.getenv("S3_ADMIN")
MINIO_PASSWORD = os.getenv("S3_PASSWORD")
MONGODB_USERNAME = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGODB_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGODB_DOMAIN = os.getenv("DOMAIN")
MONGODB_PORT = os.getenv("MONGODB_PORT")
DEFAULT_TIMEZONE = os.getenv("TIMEZONE")
