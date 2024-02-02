from __future__ import annotations

import datetime
import numpy as np


class Field:
    name: str = ''
    type: np.dtype | datetime.date | datetime.datetime | bytes | dict = ''
    required: bool = False
    key: bool = False


class DDL:
    fields: list[Field]
