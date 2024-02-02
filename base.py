from abc import ABC

from abc_ import SourceAbstract, DestinationAbstract, DriverAdapterAbstract


class SourceBase(SourceAbstract, ABC):
    def __init__(self, driver: DriverAdapterAbstract, **kwargs):
        self.driver = driver
        self.kwargs = kwargs


class DestinationBase(DestinationAbstract, ABC):
    def __init__(self, driver: DriverAdapterAbstract, **kwargs):
        self.driver = driver
        self.kwargs = kwargs
