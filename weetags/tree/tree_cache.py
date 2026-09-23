from __future__ import annotations

from logging.config import valid_ident
from platform import node
from typing import Any, Literal

from sqlalchemy import desc

from weetags.common import BoundEngine
from weetags.common.path_utils import NodePath, NodePathCollection
from weetags.common.cache import (
    CacheEngine, 
    LocalCacheEngine, 
    MemcachedCacheEngine
)


def create_cache_engine(cache: Literal["local", "memcached"], **opts) -> CacheEngine:
    match cache:
        case "local":
            return LocalCacheEngine(**opts)
        case "memcached":
            return MemcachedCacheEngine(**opts)
        case _:
            raise ValueError("Unknown cache engine name.")

class TreeCache:
    cache_engine: CacheEngine

    def __init__(self, cache: Literal["local", "memcached"], **opts):
        self.cache_engine = create_cache_engine(cache, **opts)
        self.__dict__.update(**opts)

    def bind(self, tree_name: str, engine: BoundEngine) -> TreeCache:
        self.tree_name = tree_name
        self._engine = engine
        self.build()
        return self

    def set(self, key: str, value: Any) -> None:
        self.cache_engine.set(key, value)

    def set_node_reference(self, name: str, values: list[str]) -> None:
        self.set(f"node_{self.tree_name}_{name}", values)


    def get(self, key: str, default: Any = None) -> Any:
        return self.cache_engine.get(key, default)

    def get_node_reference(self, name: str) -> list[str]:
        paths = self.get(f"node_{self.tree_name}_{name}")
        if paths is None:
            raise KeyError(f"Unknown node name: {name}")
        return paths

    def pop(self, key: str) -> None:
        self.cache_engine.pop(key)

    def flush(self) -> None:
        ...

    def build(self) -> None:
        paths = NodePathCollection.from_generator(self._engine.non_ordered_walk())
        references = paths.node_references()
        for name, paths in references.items():
            self.set_node_reference(name, paths)


    def siblings(self, name: str, include_self: bool = False, *args, **kwargs) -> list[str]:
        p = self.parent(name)
        if len(p) <= 1:
            return []
        parent = p[0]
        
        siblings = self.children(parent)
        if include_self is False:
            index = siblings.index(name)
            siblings.pop(index)
        return siblings

    def children(self, name: str, *args, **kwargs) -> list[str]:
        children = set()
        paths = self.get_node_reference(name)
        for p in paths:
            path = NodePath(NodePath(p).subpath(name, node_is="root"))
            if len(path.nodes) == 1:
                continue

            child = path.nodes[1]
            children.add(child)
        return list(children)

    def parent(self, name: str, *args, **kwargs) -> list[str]:
        paths = self.get_node_reference(name)
        nodes = NodePath(paths[0]).subpath(name, node_is="leaf").split(".")
        if len(nodes) <= 1:
            return []
        return [nodes[-2]]

    def ancestors(self, name: str, include_self: bool = False, *args, **kwargs) -> list[str]:
        paths = self.get_node_reference(name)
        ancestors = NodePath(paths[0]).subpath(name, node_is="leaf").split(".")
        if include_self is False:
            ancestors.pop()
        return ancestors

    def descendants(self, name: str, include_self: bool = False, *args, **kwargs) -> list[str]:
        descendants = set()
        paths = self.get_node_reference(name)
        for p in paths:
            nodes = NodePath(NodePath(p).subpath(name, node_is="root")).nodes
            if include_self is False:
                nodes.pop(0)
            descendants.update(nodes)
        return list(descendants)

    def branch(self, name: str, *args, **kwargs) -> list[str]:
        ancestors = self.ancestors(name, include_self=True)
        descendants = self.descendants(name)
        return ancestors + descendants