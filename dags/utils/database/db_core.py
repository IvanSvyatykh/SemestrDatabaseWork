from contextlib import contextmanager
from cassandra.cluster import Cluster


class CassandraConfig:
    def __init__(self, keyspace: str, contact_points=["127.0.0.1"]):
        self.contact_points = contact_points
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        cluster = Cluster(contact_points=self.contact_points)
        session = cluster.connect()
        session.set_keyspace(self.keyspace)
        self.session = session
        return session

    def create_keyspace(self) -> None:
        create_keyspace_query = f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
            AND DURABLE_WRITES = true;
            """

        self.session.execute(create_keyspace_query)

    def execute_query(self, query):
        if not self.session:
            raise Exception("Не установлено соединение с базой данных.")
        try:
            rows = self.session.execute(query)
            return rows
        except Exception as e:
            print(f"Произошла ошибка при выполнении запроса: {e}")
            return None

    def close_connection(self):
        if self.session:
            self.session.shutdown()


class CassandraUnitOfWork:
    def __init__(self, session):
        self._session = session
        self._queries = []

    @contextmanager
    def begin_transaction(self):
        """Контекстный менеджер для выполнения группы запросов в рамках одной транзакции."""
        try:
            yield self
            for query in self._queries:
                self._session.execute(query)
        finally:
            self._queries.clear()

    def add_query(self, query):
        """Добавляет запрос в очередь для последующего выполнения."""
        self._queries.append(query)

    def commit(self):
        """Выполнение всех накопленных запросов."""
        for query in self._queries:
            self._session.execute(query)
        self._queries.clear()

    def rollback(self):
        """Очистка очереди запросов без их выполнения."""
        self._queries.clear()
