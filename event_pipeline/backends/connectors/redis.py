from redis import Redis
from event_pipeline.backends.connection import BackendConnectorBase


class RedisConnector(BackendConnectorBase):

    def __init__(self, host: str, port: int, db: int = 0):
        super().__init__(host=host, port=port, db=db, username=None, password=None)
        self._cursor = Redis(host=host, port=port, db=db)

    def connect(self):
        if self._cursor is None:
            self._cursor = Redis(host=self.host, port=self.port, db=self.database)
        return self._cursor

    def disconnect(self):
        self._cursor.close()

    def is_connected(self):
        return self.cursor.ping()
