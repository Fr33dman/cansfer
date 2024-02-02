import logging

import psycopg2
from pandas import DataFrame

from base import DriverAdapterAbstract

log = logging.getLogger('adapter')


class Psycopg2Adapter(DriverAdapterAbstract):
    def __init__(
            self,
            host: str,
            port: int,
            username: str,
            password: str,
            database: str,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database,
        )

    def execute(self, query: str, *args, **kwargs) -> DataFrame:
        log.info(f'%s\n%s\n%s', query, args, kwargs)
        connection = self.get_connection()
        try:
            with connection:
                with connection.cursor() as cur:
                    cur.execute(query, *args, **kwargs)
                    queryset = cur.fetchall()
                    return DataFrame(queryset, columns=list(i[0] for i in cur.description))
        finally:
            connection.close()
