from database.config import DatabaseConfig
from database.documents.person import Person


db_config = DatabaseConfig()
db_config.start_connection()
person = Person(name="Ivan", surname="Svyatykh")
person.save()
print(db_config)
