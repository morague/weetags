from __future__ import annotations

from enum import Enum
from pathlib import Path
from attrs import define, field, validators
from sqlalchemy.types import TypeEngine
from sqlalchemy import (
    Column,
    Integer,
    Text,
    JSON,
    DateTime,
    Boolean,
)

from typing import Any, Type

from weetags.common.loaders import ConfigLoader, Loader, select_loader
from weetags.common.utils import path_converter


class FieldType(str, Enum):
    INTEGER = "integer"
    TEXT = "text"
    BOOL = "bool"
    JSON = "json"
    DATETIME = "datetime"
    UNKNOWN = "unknown"

    @classmethod
    def from_value(cls, value: str) -> FieldType:
        for e in cls:
            if e.value == value:
                return e
        raise ValueError(f"Unknown `{cls}` value: {value}")

    def into_sqlalchemy(self) -> Type[TypeEngine]:
        match self.value:
            case "integer":
                return Integer
            case "text":
                return Text
            case "datetime":
                return DateTime
            case "bool":
                return Boolean
            case "json":
                return JSON
            case "unknown":
                raise ValueError("Unknown Field type")

    @classmethod
    def from_sqlalchemy(cls, value: TypeEngine) -> FieldType:
        match type(value).__name__:
            case "INTEGER":
                return FieldType.INTEGER
            case "TEXT":
                return FieldType.TEXT
            case "BOOLEAN":
                return FieldType.BOOL
            case "DATETIME":
                return FieldType.DATETIME
            case "JSON":
                return FieldType.JSON
            case _:
                return FieldType.UNKNOWN

@define(frozen=True)
class FieldDefinition:
    name: str = field(validator=[validators.instance_of(str)])
    type: FieldType = field(converter=FieldType.from_value)
    primary_key: bool = field(default=False, validator=[validators.instance_of(bool)])
    unique: bool = field(default=False, validator=[validators.instance_of(bool)])
    nullable: bool = field(default=False, validator=[validators.instance_of(bool)])
    index: bool = field(default=False, validator=[validators.instance_of(bool)])
    default: Any = field(default=None)

    def into_column(self) -> Column:
        if self.primary_key:
            return Column(self.name, self.type.into_sqlalchemy(), primary_key=self.primary_key)
        else:
            return Column(self.name, self.type.into_sqlalchemy(), unique=self.unique, nullable=self.nullable, index=self.index, default=self.default)


@define(frozen=True)
class DataDefinition:
    path: Path | None = field(default=None, converter=path_converter)
    data: list[dict[str, Any]] | None = field(default=None)
    keymap: dict[str, str] | None = field(default=None)

    @keymap.validator
    def validate_indexes(self, attribute, value) -> None:
        if value is None:
            return
        if not isinstance(value, dict):
            raise ValueError("Keymap must be of type dict[str, str].")
        for k,v in value.items():
            if not isinstance(k, str):
                raise ValueError("Keymap key must be of type str.")
            if not isinstance(v, str):
                raise ValueError("Keymap value must be of type str.")

    @path.validator
    def validate_path(self, attribute, value) -> None:
        if value is None:
            return
        elif not isinstance(value, Path):
            raise TypeError("path must be a str or a Path.")

    @data.validator
    def validate_data(self, attribute, value) -> None:
        if value is None:
            return
        elif not isinstance(value, list):
            raise TypeError("path must be a list[dict[str, Any]].")

    def __attrs_post_init__(self) -> None:
        if (self.path is not None and self.data is not None) or (self.path is None and self.data is None):
            raise ValueError("DataDefinition must define either a path or data")


@define(frozen=True)
class TreeConfig:
    name: str = field(validator=[validators.instance_of(str)])
    fields: list[FieldDefinition] = field()
    data: DataDefinition | None = field()
    indexes: list[list[str]] | None = field()
    unique_constraints: list[list[str]] | None = field()

    def __attrs_post_init__(self) -> None:
        if self.indexes is None:
            return
        
        field_names = [f.name for f in self.fields]
        for index in self.indexes:
            for key in index:
                if key not in field_names:
                    raise ValueError("indexes keys must be field names.")

    @indexes.validator
    def validate_indexes(self, attribute, value) -> None:
        if value is None:
            return
        
        for elm in value:
            if not isinstance(elm, list):
                raise ValueError("Indexes must be of type list[list[str]].")
            for v in elm:
                if not isinstance(v, str):
                    raise ValueError("Indexes must be of type list[list[str]].")

    @classmethod
    def parse_file(cls, path: str | Path, loader: Loader | None = None) -> TreeConfig:
        if loader is None:
            loader = select_loader(path)
        configs = ConfigLoader().load(path, loader=loader)
        return cls.parse(**configs)

    @classmethod
    def parse(
        cls, 
        name: str, 
        fields: list[dict[str, Any]],
        data: dict[str, Any] | None = None,
        indexes: list[list[str]] | None = None
    ) -> TreeConfig:
        field_definitions = [FieldDefinition(**c) for c in fields]
        data_definition = None
        if data is not None:
            data_definition = DataDefinition(**data)
        return TreeConfig(name, field_definitions, data_definition, indexes)


