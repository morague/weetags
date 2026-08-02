from __future__ import annotations

from logging.config import valid_ident
from platform import node
from typing import Any, Literal

from sqlalchemy import desc

from weetags.common import Engine
from weetags.common.path_utils import NodePath
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

    def bind(self, tree_name: str, engine: Engine) -> TreeCache:
        self.tree_name = tree_name
        self._engine = engine
        self.reflect()
        return self

    def reflect(self) -> None:
        self._engine.get_tree(self.tree_name)
        self._set_topology()
        self._set_references()

    def _set_topology(self) -> None:
        roots = self._engine._roots(self.tree_name)
        if len(roots) > 1:
            raise ValueError("too many roots")
        if len(roots) == 0:
            raise ValueError("No root node found")
        root = roots[0]
        topology = self._engine.sub_tree_topology_from_name(root["name"])
        self.set(f"{self.tree_name}_topology", topology)

    def _set_references(self) -> None:
        references = {
            n["name"]:type("N", (), {"id": n["id"], "name": n["name"], "path": n["path"]}) 
            for n in self._engine.non_ordered_walk()
        }
        self.set(f"{self.tree_name}_references", references)


    def set(self, key: str, value: Any) -> None:
        self.cache_engine.set(key, value)

    def get(self, key: str, default: Any = None) -> Any:
        return self.cache_engine.get(key, default)

    def pop(self, key: str) -> None:
        self.cache_engine.pop(key)

    """
    1. translate node relation expr > list[nid]
        main cache tactics is to translate tree relation queries into a list of ids. 
        nodes get topologically ordered at query. 

    2. temp store particallar node conditionnal queries.




    topology based node retrival
    1. topology lookup
        retrieve list of branch containing node name.
    2. realise operation on branch path to retrieve nodes of interest.
    3. use path for reference retrieval, look up and collect nids
    4. return nids
    """

    def siblings_ids(self, name: str, include_self: bool = False, *args, **kwargs) -> list[int]:
        """siblings possess the same parent. can be done either via path or by traversal ???"""
        parent = self.get_parent(name)
        if parent is None:
            raise ValueError("Parent not found")
        
        candidates = self.topological_search(parent)
        references = self.cache_engine.get(f"{self.tree_name}_references")

        nodes, visited = [], []
        for path in candidates:
            node = path.get_node_after(parent, 1)
            if node is None:
                continue
            if node in visited:
                continue

            child = references.get(node)
            if child is None:
                raise KeyError(f"Unknown node name: {child}")
            nodes.append(child)
            
        return [n.id for n in nodes]


    def parent_id(self, name: str, *args, **kwargs) -> int:
        candidates = self.topological_search(name)
        if len(candidates) == 0:
            raise ValueError()

        references = self.cache_engine.get(f"{self.tree_name}_references")
        parent = candidates[0].get_node_before(name, 1)
        if parent is None:
            raise ValueError("no parent") # delegate to the tree engine to return None... improvable then

        parent = references.get(parent)
        return parent.id

    def children_ids(self, name: str, *args, **kwargs) -> list[int]:
        candidates = self.topological_search(name)
        references = self.cache_engine.get(f"{self.tree_name}_references")

        nodes, visited = [], []
        for path in candidates:
            node = path.get_node_after(name, 1)
            if node is None:
                continue
            if node in visited:
                continue

            child = references.get(node)
            if child is None:
                raise KeyError(f"Unknown node name: {child}")
            nodes.append(child)
        return [n.id for n in nodes]

    def ancestor_ids(self, name: str, *args, **kwargs) -> list[int]:
        candidates = self.topological_search(name)
        references = self.cache_engine.get(f"{self.tree_name}_references")

        visited, ancestors = [], []
        for path in candidates:
            for node in path.iter_until(name):
                if node in visited:
                    continue
                ancestor = references.get(node)
                if ancestor is None:
                    raise KeyError(f"Unknown node name: {node}")
                visited.append(node)
                ancestors.append(ancestor)
        return [n.id for n in ancestors]

    def descendant_ids(self, name: str, *args, **kwargs) -> list[int]:
        candidates = self.topological_search(name)
        references = self.cache_engine.get(f"{self.tree_name}_references")

        visited, descendants = [], []
        for path in candidates:
            for node in path.iter_after(name):
                if node in visited:
                    continue
                descendant = references.get(node)
                if descendant is None:
                    raise KeyError(f"Unknown node name: {node}")
                visited.append(node)
                descendants.append(descendant)
        return [n.id for n in descendants]

    def branch_ids(self, name: str, *args, **kwargs) -> list[int]:
        candidates = self.topological_search(name)
        references = self.cache_engine.get(f"{self.tree_name}_references")

        visited, nodes = [], []
        for path in candidates:
            for node in path.iter():
                if node in visited:
                    continue
                data = references.get(node)
                if data is None:
                    raise KeyError(f"Unknown node name: {node}")
                visited.append(node)
                nodes.append(data)
        return [n.id for n in nodes]

    def distance(self) -> int:
        ...

    def closest_common_ancestor(self) -> int:
        ...

    def topological_search(self, name: str) -> list[NodePath]:
        paths, prefix = [], None

        topology = self.cache_engine.get(f"{self.tree_name}_topology")
        for path in topology:
            p = NodePath(path)
            if p.contains_node(name):
                paths.append(p)
                if prefix is None and p.len > 1:
                    prefix = p.rstrip_from_node(name, include_seperator=True)
                if p.len == 1:
                    break
            if prefix is not None and p.contains_subpath(prefix) is False:
                break
        return paths

    def get_parent(self, name) -> str | None:
        topology = self.cache_engine.get(f"{self.tree_name}_topology")
        for branch in topology:
            p = NodePath(branch)
            if p.contains_node(name):
                parent = p.get_node_before(name, 1)

                if parent is None:
                    raise ValueError(f"parent not found.")
                return parent
        return None