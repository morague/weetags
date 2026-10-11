from __future__ import annotations

import json
from hashlib import sha1
from types import SimpleNamespace 

from typing import Any

import weetags.common.exceptions as excp
from weetags.common.base import TreeTopologyDefinition



class Metadata(SimpleNamespace):
    @property
    def keys(self) -> list[str]:
        return list(self.__dict__.keys())

    @property
    def values(self) -> list[Any]:
        return list(self.__dict__.values())

    @property
    def as_dict(self) -> dict[str, Any]:
        return self.__dict__

    def get(self, key: str, default: Any = None) -> Any:
        return self.__dict__.get(key, default)

    def pop(self, key: str) -> Any:
        return self.__dict__.pop(key)

class Node:
    id: int
    name: str
    path: str
    parent: str
    children: list[str]
    level: int
    metadata: Metadata

    def __init__(
        self,
        id: int,
        name: str,
        path: str,
        parent: str,
        children: list[str],
        level: int,
        **metadata: Any,
    ) -> None:
        self.id = id
        self.name = name
        self.path = path
        self.parent = parent
        self.children = children
        self.level = level
        self.metadata = Metadata(metadata)

    def __repr__(self) -> str:
        return f"<Node: {self.name}>"

    @property
    def m(self) -> Metadata:
        return self.metadata

    @property
    def degree(self) -> int:
        return len(self.children)
    
    @property
    def is_leaf(self) -> bool:
        return not bool(self.children)

    @property
    def is_root(self) -> bool:
        return not bool(self.parent)

    @property
    def topology_keys(self) -> list[str]:
        return TreeTopologyDefinition().namespace

    @property
    def metadata_keys(self) -> list[str]:
        return self.m.keys

    @property
    def keys(self) -> list[str]:
        return self.topology_keys + self.metadata_keys

    @property
    def topology(self) -> dict[str, Any]:
        return {k:v for k,v in self.__dict__.items() if k in self.topology_keys}

    @property
    def values(self) -> dict[str, Any]:
        return self.topology | self.metadata.as_dict

    @property
    def digest(self) -> str:
        return sha1(json.dumps(self.values).encode()).hexdigest()

    @property
    def metadata_digest(self) -> str:
        return sha1(json.dumps(self.metadata.as_dict).encode()).hexdigest()

    def payload(self, fields: list[str] | None) -> dict[str, Any]:
        if fields is None:
            return self.values
        for f in fields:
            if f not in self.keys:
                raise excp.FieldError(f)
        return {k:v for k,v in self.__dict__.items() if k in fields}

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

