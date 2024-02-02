import logging

import clickhouse_connect
from pandas import DataFrame

from base import DriverAdapterAbstract

log = logging.getLogger('adapter')


class ClickhouseConnectAdapter(DriverAdapterAbstract):
    def __init__(
            self,
            host: str,
            port: int,
            username: str,
            password: str,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def get_client(self):
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )

    def execute(self, query: str, *args, **kwargs) -> DataFrame:
        log.info(f'%s\n%s\n%s', query, args, kwargs)
        client = self.get_client()
        try:
            queryset = client.query(query, *args, **kwargs)
            return DataFrame(queryset.result_set, columns=list(queryset.column_names))
        finally:
            client.close()
