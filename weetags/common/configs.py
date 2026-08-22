from __future__ import annotations

from enum import Enum
from pathlib import Path
from attrs import define, field, validators, asdict
from sqlalchemy.types import TypeEngine
from sqlalchemy import (
    Column,
    Integer,
    Text,
    JSON,
    DateTime,
    Boolean,
    false,
)

from typing import Any, Type, get_args

from weetags.common.loaders import ConfigLoader, Loader, YamlLoader
from weetags.common.utils import path_converter
from weetags.common.types import TreeTypes, OnChange, OnCollision, CacheType
from weetags.common.uri import EngineURI

def validate_tree_type(instance, attribute, value) -> None:
    accepted = get_args(TreeTypes.__value__)
    if isinstance(value, str) is False or value not in accepted:
        raise ValueError(f"Tree types must be either one of the following: {accepted}.")

def validate_on_change(instance, attribute, value) -> None:
    accepted = get_args(OnChange.__value__)
    if isinstance(value, str) is False or value not in accepted:
        raise ValueError(f"change strategytypes must be either one of the following: {accepted}.")

def validate_on_collision(instance, attribute, value) -> None:
    accepted = get_args(OnCollision.__value__)
    if isinstance(value, str) is False or value not in accepted:
        raise ValueError(f"Collision strategy must be either one of the following: {accepted}.")

def validate_cache_type(instance, attribute, value) -> None:
    accepted = get_args(CacheType.__value__)
    if isinstance(value, str) is False or value not in accepted:
        raise ValueError(f"Cache type must be either one of the following: {accepted}.")


def validate_nested_str_list_or_none(instance, attribute, value) -> None:
    if value is None:
        return
    
    if isinstance(value, list) is False:
        raise ValueError(f"{attribute.name} must be a list[list[str]] | None")

    for inner in value:
        if isinstance(inner, list) is False:
            raise ValueError(f"{attribute.name} must be a list[list[str]] | None")
        for elm in inner:
            if isinstance(elm, str) is False:
                raise ValueError(f"{attribute.name} must be a list[list[str]] | None")

def validate_list_field_def(instance, attribute, value) -> None:
    if isinstance(value, list) is False:
            raise ValueError(f"{attribute.name} must be a list[FieldDefinition]")
    for inner in value:
        if isinstance(inner, FieldDefinition) is False:
                raise ValueError(f"{attribute.name} must be a list[FieldDefinition]")

def validate_path_or_none(instance, attribute, value) -> None:
    if value is None:
        return
    if isinstance(value, Path) is False:
        raise ValueError(f"{attribute.name} must be of type: Path | None")

def validate_payload_or_none(instance, attribute, value) -> None:
    if value is None:
        return
    if isinstance(value, list) is False:
        raise ValueError(f"{attribute.name} must be of type: list[dict[str, Any]] | None")
    if len(value) > 0 and isinstance(value[0], dict) is false:
        raise ValueError(f"{attribute.name} must be of type: list[dict[str, Any]] | None")

def validate_keymap(instance, attribute, value) -> None:
    if value is None:
        return
    if isinstance(value, dict) is False:
        raise ValueError(f"{attribute.name} must be of type: dict[str, str]] | None")
    for k,v in value.items():
        if isinstance(k, str) or isinstance(v, str):
            raise ValueError(f"{attribute.name} must be of type: dict[str, str]] | None")

class DefinitionValidator:
    def __init__(self, cls: Type) -> None:
        self.cls = cls

    def validate(self, instance, attribute, value) -> None:
        if value is None:
            return

        if isinstance(value, self.cls) is False:
            raise ValueError(f"{attribute.name} must be of type: {self.cls}")

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
class PermissionsDefinition:
    ...

    @classmethod 
    def from_configs(cls, configs: dict[str, Any] | None = None) -> PermissionsDefinition | None:
        if configs is None:
            return
        return PermissionsDefinition(**configs)

@define(frozen=True)
class CacheDefinition:
    cache: CacheType | str = field(default="local", validator=validate_cache_type)
    opts: dict[str, Any] = field(factory=dict, validator=validators.instance_of(dict))

    @classmethod 
    def from_configs(cls, configs: dict[str, Any] | None = None) -> CacheDefinition | None:
        if configs is None:
            return
        return CacheDefinition(**configs)


@define(frozen=True)
class DataDefinition:
    path: Path | None = field(default=None, converter=path_converter, validator=validate_path_or_none)
    data: list[dict[str, Any]] | None = field(default=None, validator=validate_payload_or_none)
    keymap: dict[str, str] | None = field(default=None, validator=validate_keymap)

    def __attrs_post_init__(self) -> None:
        if (self.path is not None and self.data is not None) or (self.path is None and self.data is None):
            raise ValueError("DataDefinition must define either a path or data")

    @classmethod 
    def from_configs(cls, configs: dict[str, Any] | None = None) -> DataDefinition | None:
        if configs is None:
            return
        return DataDefinition(**configs)

@define(frozen=True)
class TreeStructureDefinition:
    fields: list[FieldDefinition] = field(factory=list, validator=validate_list_field_def)
    indexes: list[list[str]] | None = field(default=None, validator=validate_nested_str_list_or_none)
    unique_constraints: list[list[str]] | None = field(default=None, validator=validate_nested_str_list_or_none)

    @classmethod 
    def from_configs(cls, configs: dict[str, Any] | None = None) -> TreeStructureDefinition | None:
        if configs is None:
            return

        fields =  [FieldDefinition(**f) for f in configs.pop("fields", [])]        
        return TreeStructureDefinition(fields, **configs)

@define(frozen=True)
class BuilderDefinition:
    not_exist: bool = field(default=True, validator=validators.instance_of(bool))
    recreate: bool = field(default=False, validator=validators.instance_of(bool))
    on_change: OnChange | str = field(default="raise", validator= validate_on_change)
    on_collision: OnCollision | str = field(default="raise", validator=validate_on_collision) 
    skip_init: bool = field(default=False, validator=validators.instance_of(bool))
    skip_import: bool = field(default=False, validator=validators.instance_of(bool))

    @classmethod 
    def from_configs(cls, configs: dict[str, Any] | None = None) -> BuilderDefinition | None:
        if configs is None:
            return
        return BuilderDefinition(**configs)

@define(frozen=True)
class TreeConfig:
    name: str = field(validator=[validators.instance_of(str)])
    uri: EngineURI = field(validator=validators.instance_of(EngineURI))
    type: TreeTypes | str = field(default="tree", validator=validate_tree_type)
    cache: CacheDefinition | None = field(default=None, validator=DefinitionValidator(CacheDefinition).validate)
    structure: TreeStructureDefinition | None = field(default=None, validator=DefinitionValidator(TreeStructureDefinition).validate)
    data: DataDefinition | None = field(default=None, validator=DefinitionValidator(DataDefinition).validate)
    builder: BuilderDefinition | None = field(default=None, validator=DefinitionValidator(BuilderDefinition).validate)
    permissions: PermissionsDefinition | None = field(default=None, validator=DefinitionValidator(PermissionsDefinition).validate)
    
    @classmethod
    def parse_file(cls, path: str | Path, loader: Type[Loader] = YamlLoader) -> TreeConfig:
        configs = ConfigLoader().load(path, loader=loader)
        return cls.parse_configs(**configs)

    @classmethod
    def parse_configs(
        cls, 
        name: str,
        uri: dict[str, Any], 
        type: str = "tree",
        cache: dict[str, Any] | None = None,
        structure: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        builder: dict[str, Any] | None = None,
        permissions: dict[str, Any] | None = None,
    ) -> TreeConfig:
        e = EngineURI(**uri)
        c = CacheDefinition.from_configs(cache)
        s = TreeStructureDefinition.from_configs(structure)
        b = BuilderDefinition.from_configs(builder)
        d = DataDefinition.from_configs(data)
        p = PermissionsDefinition.from_configs(permissions)
        return cls(name, e, type, c, s, d, b, p)

    @property
    def builder_inline(self) -> dict[str, Any]:
        inline = {"name": self.name, "uri": self.uri}
        inline.update(asdict(self.structure))
        inline.update(asdict(self.builder))
        inline.update(asdict(self.data))
        return inline

    @property
    def tree_inline(self) -> dict[str, Any]:
        inline = {"name": self.name, "uri": self.uri, "cache": asdict(self.cache)}
        return inline
        

        