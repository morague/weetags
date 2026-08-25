from __future__ import annotations

import json
from hashlib import sha1
from types import SimpleNamespace 

from typing import Any

from weetags.common.base import TreeTopologyDefinition



class Node:
    id: int
    name: str
    path: str
    parent: str
    children: list[str]
    level: int

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
        self.__dict__.update(metadata)
        self._modified = False
        self.__setattr__ = self._inhibitor

    def __repr__(self) -> str:
        return f"<Node: {self.name}>"

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
    def keys(self) -> list[str]:
        return [k for k in self.__dict__.keys() if not k.startswith("_") or k in ("self")]

    @property
    def topology_keys(self) -> list[str]:
        return TreeTopologyDefinition().namespace

    @property
    def topology(self) -> dict[str, Any]:
        return {k:v for k,v in self.__dict__.items() if k in self.topology_keys}

    @property
    def metadata_keys(self) -> list[str]:
        return [k for k in self.keys if k not in self.topology_keys]

    @property
    def metadata(self) -> dict[str, Any]:
        return {k:v for k,v in self.__dict__.items() if k in self.metadata_keys}

    @property
    def data(self) -> dict[str, Any]:
        return self.topology | self.metadata

    @property
    def digest(self) -> str:
        return sha1(json.dumps(self.data).encode()).hexdigest()

    @property
    def metadata_digest(self) -> str:
        return sha1(json.dumps(self.metadata).encode()).hexdigest()

    def payload(self, fields: list[str] | None) -> dict[str, Any]:
        if fields is None:
            return self.data
        for f in fields:
            if f not in self.keys:
                raise KeyError(f"Unknown field: {f}")
        return {k:v for k,v in self.__dict__.items() if k in fields}

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def _inhibitor(self, name: str, value: Any) -> None:
        """block update of topology keys, allow other updates"""
        if name in self.topology_keys:
            raise AttributeError("topology keys are not modifiable.")
        self.__dict__[name] = value

        if name in self.metadata_keys:
            self.__dict__["_modified"] = True