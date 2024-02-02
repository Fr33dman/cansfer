import decimal
from typing import Dict, Any
import numpy as np
import pandas as pd
import datetime

from ddl import DDL
from base import DestinationBase

default_type = 'String'

conversation_table = {
    np.str_: 'String',
    np.bool_: 'UInt8',
    np.int8: 'Int8',
    np.int16: 'Int16',
    np.int32: 'Int32',
    np.int64: 'Int64',
    np.longlong: 'Int256',
    np.uint8: 'UInt8',
    np.uint16: 'UInt16',
    np.uint32: 'UInt32',
    np.uint64: 'UInt64',
    np.ulonglong: 'UInt256',
    np.float32: 'Float32',
    np.float64: 'Float64',
    datetime.date: 'Date32',
    datetime.datetime: 'DateTime64',
    decimal.Decimal: 'Decimal(40, 0)',
    bytes: 'String',
    dict: 'Array(String)',
    tuple: 'Tuple(Int64, Int64)',
}


def datetime_to_str(val: datetime.datetime):
    return repr(val.strftime('%Y-%m-%d %H:%M:%S.%f'))


def date_to_str(val: datetime.date):
    return repr(val.strftime('%Y-%m-%d'))


def to_str(val: object):
    return str(val)


def bool_to_uint(val: bool):
    return str(int(val))


def get_str(val):
    val = val.replace('\'', r'\'')
    val = val.replace('\\', r'\\')
    val = val.replace(r"\\'", r'\'')
    res = "'" + val + "'"
    return res


def bytes_to_str(val: bytes):
    return get_str(val.decode(errors='ignore'))


pre_process_functions = {
    np.str_: get_str,
    np.bool_: bool_to_uint,
    np.int8: to_str,
    np.int16: to_str,
    np.int32: to_str,
    np.int64: to_str,
    np.longlong: to_str,
    np.uint8: to_str,
    np.uint16: to_str,
    np.uint32: to_str,
    np.uint64: to_str,
    np.ulonglong: to_str,
    np.float32: to_str,
    np.float64: to_str,
    datetime.date: date_to_str,
    datetime.datetime: datetime_to_str,
    decimal.Decimal: to_str,
    bytes: bytes_to_str,
    dict: to_str,
    tuple: to_str,
}


def compile_type(_type: Any, nullable: bool) -> str:
    clickhouse_type = conversation_table.get(_type)
    if clickhouse_type is None:
        return default_type
    if nullable:
        clickhouse_type = f'Nullable({clickhouse_type})'
    return clickhouse_type


class DestinationClickhouse(DestinationBase):

    def create_table(self, ctx: Dict[str, Any], ddl: DDL):
        table_schema = self.kwargs.get('table_schema')
        table_name = self.kwargs.get('table_name')
        columns = (
            (field.name, compile_type(field.type, field.required))
            if not isinstance(field.type, str) else (field.name, field.type)  # if type in DDL was transferred
                                                                              # in pure clickhouse type (str)
            for field in ddl.fields
        )
        columns = ', '.join((f'`{column[0]}` {column[1]}' for column in columns))
        query = f'''
                CREATE TABLE IF NOT EXISTS `{table_schema}`.`{table_name}` 
                ({columns})
                ENGINE = MergeTree
                ORDER BY tuple();
                '''
        self.driver.execute(query)

    def write(self, ctx: Dict[str, Any], data: pd.DataFrame):
        table_schema = self.kwargs.get('table_schema')
        table_name = self.kwargs.get('table_name')
        column_names = data.columns.values
        columns = ', '.join(map(lambda s: '`' + s + '`', column_names))

        values = ','.join(
            '(' + ', '.join((row[column_name] for column_name in column_names)) + ')'
            for _, row in data.iterrows()
        )
        query = f'''
                INSERT INTO `{table_schema}`.`{table_name}` ({columns})
                VALUES {values}
                '''
        self.driver.execute(query)

    def pre_process_data(
            self,
            ctx: Dict[str, Any],
            ddl: DDL,
            data: pd.DataFrame,
    ) -> pd.DataFrame:
        for field in ddl.fields:
            _type = field.type
            if _type in pre_process_functions:
                data[field.name] = data[field.name].map(pre_process_functions[_type])
        return data
