import logging

import pytest

from clickhouse import SourceClickhouse, DestinationClickhouse, ClickhouseConnectAdapter
from postgres import SourcePostgres, Psycopg2Adapter
from transfer import transfer

if __name__ == '__main__':
    import sys
    sys.path.append('../sberPM')


def test_clickhouse_to_clickhouse(clickhouse_test_data):
    context = {}

    log = logging.getLogger('adapter')
    log.setLevel('DEBUG')
    driver_ch = ClickhouseConnectAdapter('localhost', 8123, 'sberpm', 'sberpm')
    source = SourceClickhouse(driver_ch, table_name='test_table', table_schema='default')
    destination = DestinationClickhouse(driver_ch, table_name='transfer_from_clickhouse', table_schema='default')

    transfer(context, source, destination, 1)


def test_postgres_to_clickhouse(postgres_test_data):
    context = {}

    log = logging.getLogger('adapter')
    log.setLevel('DEBUG')
    driver_pg = Psycopg2Adapter('localhost', 5432, 'sberpm', 'sberpm', 'sberpm')
    driver_ch = ClickhouseConnectAdapter('localhost', 8123, 'sberpm', 'sberpm')
    source = SourcePostgres(driver_pg, table_name='test_table', table_schema='public')
    destination = DestinationClickhouse(driver_ch, table_name='transfer_from_postgres', table_schema='default')

    transfer(context, source, destination, 1)


