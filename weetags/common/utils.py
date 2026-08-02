from __future__ import annotations

from pathlib import Path
from typing import Type, Any


OP = {
    "=": "__eq__",
    "!=": "__ne__",
    ">": "__gt__",
    ">=": "__ge__",
    "<": "__lt__",
    "<=": "__le__",
    "like": "like",
    "ilike": "ilike",
    "not_like": "notlike",
    "not_ilike": "notilike",
    "in": "in_",
    "not_in": "not_in",
    "is": "is_",
    "is_not": "is_not",
    "json_each": "json_each"
}


class Json:
    pass

def translate_sqlalchemy_sqltype(dtype: Any) -> Type:
    match str(dtype):
        case "INTEGER":
            return int
        case "TEXT":
            return str
        case "BOOLEAN":
            return bool
        case "JSON":
            return Json
        case _:
            raise ValueError("Non handled sql type")
        

def translate_keys(data: dict[str, Any], keymap: dict[str, Any] | None = None) -> dict[str, Any]:
    if keymap is None:
        return data
    
    for k,v in keymap.items():
        if k in data.keys():
            data[v] = data.pop(k)
    return data

def convert_buffer(value: Any) -> str | None:
    if value is None:
        return None
    elif isinstance(value, str):
        return value
    elif isinstance(value, bytes):
        return value.decode()
    else:
        raise TypeError("buffer must be a str or utf-8 encoded bytes.")

def path_converter(value: Any) -> Path | None:
    if value is None:
        return None
    elif isinstance(value, str):
        return Path(value)
    elif isinstance(value, Path):
        return value
    else:
        raise TypeError("path must be a str or a Path")
