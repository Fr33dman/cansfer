import re
import datetime
import pandas as pd
from typing import Dict, Any

from ddl import DDL, Field
from base import SourceBase


def get_string(val):
    val = val.replace('\'', r'\'')
    val = val.replace(r"\\'", r'\'')
    val = val.replace('\"', r'\"')
    return "'" + val + "'"


def get_string_2(val):
    val = val.replace('\'', r'\'')
    val = val.replace('\\', r'\\')
    val = val.replace(r"\\'", r'\'')
    res = "'" + val + "'"
    return res


def get_str(val: object):
    return str(val)


def get_str_and_repr(val):
    return repr(str(val))


def datetime_to_str(val: datetime.datetime):
    return repr(val.strftime('%Y-%m-%d %H:%M:%S'))


def datetime64_to_str(val: datetime.datetime):
    return repr(val.strftime('%Y-%m-%d %H:%M:%S.%f'))


def date_to_str(val: datetime.datetime):
    return repr(val.strftime('%Y-%m-%d'))


def bytes_to_str(val: bytes):
    return get_string_2(val.decode(errors='ignore'))


post_process_functions = {
    'IPv4': get_str_and_repr,
    'IPv6': get_str_and_repr,
    'UUID': get_str_and_repr,
    'FixedString': bytes_to_str,
    'DateTime': datetime_to_str,
    'DateTime64': datetime64_to_str,
    'Date': date_to_str,
    'Date32': date_to_str,
    'String': get_string_2,
    'Enum8': get_string,
    'Enum16': get_string,
    'Int8': get_str,
    'Int16': get_str,
    'Int32': get_str,
    'Int64': get_str,
    'UInt8': get_str,
    'UInt16': get_str,
    'UInt32': get_str,
    'UInt64': get_str,
    'Int128': get_str,
    'Int256': get_str,
    'UInt128': get_str,
    'UInt256': get_str,
    'Float32': get_str,
    'Float64': get_str,
    'Decimal': get_str,
    'Tuple': get_str,
    'Map': get_str,
    'Nested': get_str,
}


def clear_clickhouse_type(dtype) -> str:
    pattern = re.compile(r'(?<=\().*(?=\))')
    cleared_dtype = dtype
    while True:
        match = pattern.search(cleared_dtype)
        if not match:
            break
        detected_match = match.group()
        if (
                not detected_match[0].isupper()
                or ' ' in detected_match
        ):
            break
        cleared_dtype = detected_match
    dtype_parameters = cleared_dtype.find('(')
    if dtype_parameters != -1:
        cleared_dtype = cleared_dtype[:dtype_parameters]
    return cleared_dtype


class SourceClickhouse(SourceBase):

    milestone_chunk_index = None

    def read_ddl(self, ctx: Dict[str, Any]) -> DDL:
        # Done
        query = '''
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s
                AND table_schema = %s
                ORDER BY ordinal_position
                '''
        table_schema = self.kwargs.get('table_schema')
        table_name = self.kwargs.get('table_name')

        table_metadata = self.driver.execute(query, (table_name, table_schema))

        fields = list()
        for _, row in table_metadata.iterrows():
            # Prepare data from clickhouse before creating DDL
            column_name = row['column_name']
            column_type = row['data_type']
            required = bool(row['is_nullable'])

            # Create DDL
            f = Field()
            f.name = column_name
            f.type = column_type
            f.required = required
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
                FROM `{table_schema}`.`{table_name}`
                '''
        return self.driver.execute(query)

    def post_process_data(
            self,
            ctx: Dict[str, Any],
            ddl: DDL,
            data: pd.DataFrame,
    ) -> pd.DataFrame:
        for field in ddl.fields:
            _type = clear_clickhouse_type(field.type)
            if _type in post_process_functions:
                data[field.name] = data[field.name].map(post_process_functions[_type])
        return data
