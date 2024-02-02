class TransferException(Exception):
    ...


class CreateTableException(TransferException):
    ...


class ReadDDLException(TransferException):
    ...


class ReadDataException(TransferException):
    ...


class WriteDataException(TransferException):
    ...
