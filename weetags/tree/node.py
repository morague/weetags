from __future__ import annotations

import json
from hashlib import sha1
from functools import wraps
from collections import defaultdict

from typing import Any

from weetags.tree.tree_engine import TreeEngine
from weetags.common.types import TraversalOrder
from weetags.common.base import TreeTopologyDefinition


def connected(f):
    @wraps(f)
    def decorator(instance: Node, *args: Any, **kwargs: Any) -> Any:
        engine = getattr(instance, "_engine", None)
        tree = getattr(instance, "_tree", None)
        if engine is None or tree is None:
            raise ValueError(
                f"{instance} must be connected to the datastore to run this method.",
                "Please use `Node.connect` method to link the node to the datastore"
            )
        return f(instance, *args, **kwargs)
    return decorator

class Node:
    # TODO: _in _auto_commit mode, built in methods like `append` or `extend`, ... doesn't trigger __setitem__.
    id: int
    name: str
    path: str
    parent: str
    children: list[str]

    depth: int

    _tree: str
    _engine: TreeEngine
    _auto_commit: bool = False

    """
    missing engine, tree, ... to do operations from a node ...


    
    node attr modification are allowed, however they are not automatically synced to the database, unless sync mode in activated. 
    """

    def __init__(
        self,
        _auto_commit: bool = False,
        **kwargs: Any,
    ) -> None:
        self.__dict__.update(kwargs)

        self._auto_commit = _auto_commit
        self._modified = False

    def __repr__(self) -> str:
        return f"<Node: {self.name}>"

    def __setattr__(self, name: str, value: Any) -> None:
        """block update of topology keys, allow other updates"""
        if name in self.topology_keys:
            raise AttributeError("topology keys are not modifiable.")
        self.__dict__[name] = value

        if name in self.metadata_keys and self._auto_commit:
            self.push(name)
        elif name in self.metadata_keys:
            self.__dict__["_modified"] = True

    @property
    def level(self) -> int:
        return len(self.path.split("."))

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

    def connect(self, engine: TreeEngine, tree: str) -> Node:
        self._engine = engine
        self._tree = tree
        return self

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)
    
    @connected
    def push(self, key: str | None = None) -> None:
        """write node into the engine == update metadata of the node."""
        if key is None:
            values = {k:self.get(k) for k in self.diff().values()}
        else:
            values = {key: self.get(key)}
        self._engine._update_metadata([self.id], values)

    @connected
    def sync(self, metadata_only: bool = False, topology_only: bool = False) -> None:
        remote = self._fetch_self()

        if (
            (metadata_only is False and topology_only is False) 
            or (metadata_only and topology_only)
        ):
            self.__dict__.update(remote.data)

        elif metadata_only:
            self.__dict__.update(remote.metadata)

        elif topology_only:
            self.__dict__.update(remote.topology)

    @connected
    def is_modified(self) -> int:
        """
        0: no modification
        1: at least local metadata modifications ( cannot tell about remote modifications)
        2: remote metadata modifications
        3: remote topology modifications ( local cannot impl topoogy modifications)
        """
        remote = self._fetch_self()

        if self.digest == remote.digest:
            return 0
        elif self.metadata_digest != remote.metadata_digest and self._modified:
            return 1
        elif self.metadata_digest != remote.metadata_digest:
            return 2
        else:
            return 3

    @connected
    def diff(self) -> dict[str, Any]:
        """return diff between instance and stored node."""
        diffs = defaultdict(dict)
        remote = self._fetch_self()
        for k,v in self.data:
            rvalue = remote.get(k)
            if v != rvalue:
                diffs[k].update({"local": v, "remote": rvalue})
        return diffs

    @connected
    def parent_node(self) -> Node | None:
        return self._into_node(self._engine.parent_node(self.name))

    @connected
    def children_node(self) -> list[Node]:
        return self._into_nodes(self._engine.children_nodes(self.name))

    @connected
    def sibling_node(self, include_self: bool = False) -> list[Node]:
        return self._into_nodes(self._engine.sibling_nodes(self.name, include_self))

    @connected
    def descendant_node(self, order: TraversalOrder = "level") -> list[Node]:
        return self._into_nodes(self._engine.descendant_nodes(self.name, order))

    @connected
    def ancestor_node(self) -> list[Node]:
        return self._into_nodes(self._engine.ancestor_nodes(self.name))

    @connected
    def branch_nodes(self, order: TraversalOrder = "level") -> list[Node]:
        return self._into_nodes(self._engine.branch_nodes(self.name, order))

    @connected
    def _fetch_self(self) -> Node:
        node = self._into_node(self._engine._node_from_name(self.name))
        if node is None:
            raise ValueError(f"Topology error: unable to retrieve node {self.name}")
        return node
        
    def _into_node(self, data: dict[str, Any] | None) -> Node | None:
        if data is None:
            return None
        return Node(**data).connect(self._engine, self.name)

    def _into_nodes(self, data: list[dict[str, Any]]) -> list[Node]:
        return [Node(**d).connect(self._engine, self.name) for d in data]


