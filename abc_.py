import abc
import pandas as pd
from typing import Iterable, Dict, Any

from ddl import DDL


class DriverAdapterAbstract(abc.ABC):

    @abc.abstractmethod
    def execute(self, query: str, *args, **kwargs) -> pd.DataFrame:
        ...


class SourceAbstract(abc.ABC):

    @abc.abstractmethod
    def read_ddl(
            self,
            ctx: Dict[str, Any],
    ) -> DDL:
        ...

    @abc.abstractmethod
    def read(
            self,
            ctx: Dict[str, Any],
    ) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def post_process_data(
            self,
            ctx: Dict[str, Any],
            ddl: DDL,
            data: pd.DataFrame,
    ) -> pd.DataFrame:
        ...


class DestinationAbstract(abc.ABC):

    @abc.abstractmethod
    def create_table(
            self,
            ctx: Dict[str, Any],
            ddl: DDL,
    ):
        ...

    @abc.abstractmethod
    def write(
            self,
            ctx: Dict[str, Any],
            data: pd.DataFrame,
    ):
        ...

    @abc.abstractmethod
    def pre_process_data(
            self,
            ctx: Dict[str, Any],
            ddl: DDL,
            data: pd.DataFrame,
    ) -> pd.DataFrame:
        ...
