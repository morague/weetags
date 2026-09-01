from __future__ import annotations


import json as j
from decimal import Decimal
from sqlalchemy import Row
from datetime import datetime, date
from abc import ABC, abstractmethod

from typing import Any




class BaseSerializer(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def serialize_value(self, value: Any) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def serialize_list(self, value: list[Any]) -> list[Any]:
        raise NotImplementedError()

    @abstractmethod
    def serialize_record(self, row: Row) -> dict[str, Any]:
        raise NotImplementedError()

    @abstractmethod
    def serialize_records(self, rows: list[Row]) -> list[dict[str, Any]]:
        raise NotImplementedError()

class DefaultSerializer(BaseSerializer):
    def __init__(self) -> None:
        super().__init__()

    def serialize_value(self, value: Any) -> Any:
        return value

    def serialize_list(self, value: list[Any]) -> list[Any]:
        return value

    def serialize_record(self, row: Row) -> dict[str, Any]:
        return row._asdict()

    def serialize_records(self, rows: list[Row]) -> list[dict[str, Any]]:
        return [r._asdict() for r in rows]

class HttpSerializer(BaseSerializer):
    def __init__(self) -> None:
        super().__init__()

    def serialize_value(self, value: Any) -> Any:
        return self._serialize(value)

    def serialize_list(self, value: list[Any]) -> list[Any]:
        return [self._serialize(v) for v in value]

    def serialize_record(self, row: Row) -> dict[str, Any]:
        return {k:self._serialize(v) for k,v in row._asdict()}

    def serialize_records(self, rows: list[Row]) -> list[dict[str, Any]]:
        return [self.serialize_record(r) for r in rows]

    def _serialize(self, value: Any) -> Any:
        _is_json = False
        if isinstance(value, str):
            _is_json = self._probably_json(value)

        if isinstance(value, str) and _is_json:
            return j.loads(value)
        elif isinstance(value, str):
            return value
        elif isinstance(value, int):
            return value
        elif isinstance(value, bool):
            return value
        elif value is None:
            return value
        elif isinstance(value, list):
            return value
        elif isinstance(value, dict):
            return value
        elif isinstance(value, float):
            return value
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, datetime):
            return datetime.strftime(value, "%d-%m-%Y %H:%M:%S")
        elif isinstance(value, date):
            return date.strftime(value, "%d-%m-%Y")
        else:
            return "BLOB"

    def _probably_json(self, value: str) -> bool:
        json_list = value.startswith("[") and value.endswith("]")
        json_dict = value.startswith("{") and value.endswith("}")
        return any([json_dict, json_list])