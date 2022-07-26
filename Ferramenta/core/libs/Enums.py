# -*- encoding: utf-8 -*-
#!/usr/bin/python
import datetime
from datetime import datetime
from enum import Enum


class FieldType(Enum):
    _str: str = 'TEXT'
    _float: float = 'FLOAT'
    _double: float = 'DOUBLE'
    _short: int = 'SHORT'
    _long: int = 'LONG'
    _date: datetime = 'DATE'
