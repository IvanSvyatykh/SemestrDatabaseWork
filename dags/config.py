import os
from dotenv import load_dotenv

load_dotenv()

MINIO_MONGODB_USERNAME = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MONGODB_USERNAME = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGODB_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGODB_DOMAIN = os.getenv("DOMAIN")
MONGODB_PORT = os.getenv("MONGODB_PORT")
DEFAULT_TIMEZONE = os.getenv("TIMEZONE")
MINIO_ACCESS_KEY = os.getenv("ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("SECRET_KEY")
MINIO_API_PORT = os.getenv("MINIO_API_PORT")
MINIO_WEB_UI_PORT = os.getenv("MINIO_WEB_UI_PORT")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
CASSANDRA_DOMEN = os.getenv("CASSNDRA_DOMEN")
