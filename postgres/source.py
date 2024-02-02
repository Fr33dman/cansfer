import decimal
import uuid
from pandas._libs.tslibs.timestamps import Timestamp

import numpy as np
import pandas as pd
import datetime
from typing import Dict, Any, Iterable

from ddl import DDL, Field
from base import SourceBase


conversation_table = {
    'bigserial': np.int64,
    # 'bit': ' [ (n) ]	 	fixed-length bit string',
    # 'bit': ' varying [ (n) ]	varbit [ (n) ]	variable-length bit string',
    'box': tuple,
    # 'character': ' [ (n) ]	char [ (n) ]	fixed-length character string',
    # 'character': ' varying [ (n) ]	varchar [ (n) ]	variable-length character string',
    'cidr': np.str_,
    'circle': np.str_,
    'inet': np.str_,
    'interval': np.str_,
    'json': np.str_,
    'jsonb': np.str_,
    'line': tuple,
    'lseg': tuple,
    'macaddr': np.str_,
    'macaddr8': np.str_,
    'path': np.str_,
    'pg_lsn': np.str_,
    'pg_snapshot': np.int64,
    'point': tuple,
    'polygon': tuple,
    'tsquery': np.str_,
    'tsvector': np.str_,
    'txid_snapshot': np.int64,
    'uuid': np.str_,
    'xml': np.str_,
    'integer': np.int64,
    'char': np.str_,
    'varchar': np.str_,
    'date': datetime.date,
    'timestamp': datetime.datetime,
    'timestamp without time zone': datetime.datetime,
    'real': decimal.Decimal,
    'double': np.float64,
    'decimal': np.int64,
    'numeric': np.int64,
    'smallint': np.int16,
    'bigint': np.longlong,
    'bit': np.bool_,
    'boolean': np.bool_,
    'bytea': bytes,
    'character': np.str_,
    'character varying': np.str_,
    'double precision': np.float64,
    'money': decimal.Decimal,
    'serial': np.int8,
    'smallserial': np.int32,
    'text': np.str_,
    'time': datetime.time,
}


def memview_to_bytes(val: memoryview):
    return val.tobytes()


def timestamp_to_datetime(val: Timestamp):
    return val.to_datetime64()


def date_to_date(val):
    return val


def to_str(val: object):
    return str(val)


post_process_functions = {
    'bytea': memview_to_bytes,
    'timestamp': timestamp_to_datetime,
    'timestamp without time zone': timestamp_to_datetime,
    'date': date_to_date,
    'uuid': to_str,
}


class SourcePostgres(SourceBase):
    milestone_chunk_index = None

    def read_ddl(self, ctx: Dict[str, Any]) -> DDL:
        query = '''
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s
                AND table_schema = %s
                '''
        table_schema = self.kwargs.get('table_schema')
        table_name = self.kwargs.get('table_name')

        table_metadata = self.driver.execute(query, (table_name, table_schema))

        fields = list()
        for _, column_name, data_type, is_nullable in table_metadata.itertuples():
            f = Field()
            f.name = column_name
            f.type = data_type
            f.required = True if is_nullable == 'YES' else False
            fields.append(f)

        ddl = DDL()
        ddl.fields = fields

        return ddl

    def read(self, ctx: Dict[str, Any]) -> pd.DataFrame:
        # Done
        table_schema = self.kwargs.get('table_schema')
        table_name = self.kwargs.get('table_name')
        query = f'''
                SELECT * 
                FROM {table_schema}.{table_name}
                '''
        return self.driver.execute(query)

    def post_process_data(
            self,
            ctx: Dict[str, Any],
            ddl: DDL,
            data: pd.DataFrame,
    ) -> pd.DataFrame:
        for field in ddl.fields:
            _type = field.type
            if _type in post_process_functions:
                data[field.name] = data[field.name].map(post_process_functions[_type])
            field.type = conversation_table.get(_type)
        return data
