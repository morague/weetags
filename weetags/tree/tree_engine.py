from __future__ import annotations

from http.client import ImproperConnectionState
import json
from pathlib import Path
from functools import wraps
from collections import deque

from pytest import Session
from sqlalchemy import Result, Table, create_engine, select, func, ColumnElement
from sqlalchemy.engine import Engine as BaseEngine

from typing import Any, Generator

from weetags.common.types import Relation, TraversalOrder, OnCollision, BaseRelations
from weetags.common import EngineURI, Engine, BoundEngine, QueryBuilder
from weetags.common.path_utils import NodePath
from weetags.tree.tree_cache import TreeCache
from weetags.tree.traversal import (
    PreOrderTreeTraversal, 
    InOrderTreeTraversal, 
    PostOrderTreeTraversal
)
from weetags.tree.importer import Importer
from weetags.tree.test import TreeResult, TreeSession




def tree_topology_cache(relation: Relation):
    def inner(f):
        @wraps(f)
        def wrapper(instance: TreeEngine, *args: Any, **kwargs: Any):
            if instance.cache is not None:
                try:
                    match relation:
                        case "parent":
                            nids = instance.cache.parent_id(*args, **kwargs)
                        case "children":
                            nids = instance.cache.children_ids(*args, **kwargs)
                        case "descendant":
                            nids = instance.cache.descendant_ids(*args, **kwargs)
                        case "ancestor":
                            nids = instance.cache.ancestor_ids(*args, **kwargs)
                        case "branch":
                            nids = instance.cache.branch_ids(*args, **kwargs)
                        case _:
                            raise NotImplementedError()
                except Exception as e:
                    return f(instance, *args, **kwargs)    

                if isinstance(nids, list):
                    return instance._nodes_from_nid(*nids)
                elif isinstance(nids, int):
                    return instance._node_from_nid(nids)
                else:
                    return f(instance, *args, **kwargs)    
            else:
                return f(instance, *args, **kwargs)
        return wrapper
    return inner

class TreeEngine(BoundEngine):
    uri: EngineURI
    cache: TreeCache | None

    name: str
    tree: Table
    _topology: Table
    _metadata: Table

    def __init__(self, name: str, base_engine: BaseEngine, uri: EngineURI, cache: TreeCache | None = None):
        super().__init__(name, base_engine, uri)

        self.cache = cache
        if self.cache is not None:
            self.cache.bind(name, self)

    @classmethod
    def build(cls, tree_name: str, uri: EngineURI, cache: TreeCache | None = None) -> TreeEngine:
        base = cls.from_uri(uri)
        return cls.from_engine(tree_name, base, cache)

    @classmethod
    def from_configs(cls, configs: dict[str, Any]) -> TreeEngine:
        name = configs.get("name", None)
        if name is None:
            raise KeyError(f"Missing key: name.")

        uri = configs.get("uri", {})
        url = EngineURI(**uri)

        cache_configs = configs.get("cache", None)
        cache = None
        if cache_configs is not None:
            cache = TreeCache(**cache_configs)
        return cls.build(name, uri, cache)

    @classmethod
    def from_engine(cls, name: str, engine: Engine, cache: TreeCache | None = None) -> TreeEngine:
        return cls(name, engine.engine, engine.uri, cache)

    def add_cache(self, cache: TreeCache) -> TreeEngine:
        self.cache = cache
        self.cache.bind(self.name, self)
        return self

    # TREE PROPERTIES
    def tree_size(self) -> int:
        stmt = select(func.count(self.tree.c.id))
        with self.sessionmaker() as session:
            data = session.execute(stmt).scalar_one()
        return data

    def tree_degree(self) -> int:
        stmt = select(func.max(func.json_array_length(self.tree.c.children)))
        with self.sessionmaker() as session:
            tree_degree = session.execute(stmt).scalar_one()
        return tree_degree

    def leaves(self) -> list[dict[str, Any]]:
        stmt = select(self.tree).where(func.json_array_length(self.tree.c.children) == 0)
        with self.sessionmaker() as session:
            leaves = session.execute(stmt).fetchall()
        return [d._asdict() for d in leaves]

    def breadth(self) -> int:
        stmt = select(func.count(self.tree.c.id)).where(func.json_array_length(self.tree.c.children) == 0)
        with self.sessionmaker() as session:
            breadth = session.execute(stmt).scalar_one()
        return breadth

    def tree_depth(self) -> int:
        stmt = select(func.max(self.tree.c.level))
        with self.sessionmaker() as session:
            depth = session.execute(stmt).scalar_one()
        return depth

    def width(self, level: int) -> int:
        if level > self.tree_depth():
            raise ValueError("Tree depth is lower than level.")
        stmt = select(func.count(self.tree.c.id)).where(self.tree.c.level == level)
        with self.sessionmaker() as session:
            width = session.execute(stmt).scalar_one()
        return width
        
    def root(self) -> dict[str, Any]:
        stmt = select(self.tree).where(self.tree.c.parent == None)
        with self.sessionmaker() as session:
            data = session.execute(stmt).fetchall()
        if len(data) == 0:
            raise ValueError(f"Tree {self.tree.name} has no root.")
        elif len(data) > 1:
            raise ValueError(f"Tree {self.tree.name} has too many roots")
        return data[0]._asdict()

    def _n_roots(self) -> int:
        return len(self.roots(self.name))

    # TREE PARTS ENUMERATION
    def node(self, name: str) -> dict[str, Any] | None:
        return self._node(name)

    def nodes_where(self, conditions: list) -> list[dict[str, Any]]:
        stmt = QueryBuilder(self.tree, self.metadata).select().where_from_str(conditions)
        with self.sessionmaker() as session:
            nodes = session.execute(stmt()).fetchall()
        return [n._asdict() for n in nodes]

    @tree_topology_cache("parent")
    def parent_node(self, name: str) -> dict[str, Any] | None:
        node = self._node_or_raise(name)        
        parent_name = node.get("parent", None)
        if parent_name is None:
            return None
        return self._node(parent_name)

    @tree_topology_cache("children")
    def children_nodes(self, name: str) -> list[dict[str, Any]]:
        node = self._node_or_raise(name)
        children_names = node.get("children",  [])
        return self._nodes(*children_names)

    def sibling_nodes(self, name: str, include_self: bool = False) -> list[dict[str, Any]]:
        parent = self.parent_node(name)
        if parent is None:
            return []

        parent_name = parent["name"]
        parent_node = self._node_or_raise(parent_name)
        siblings = parent_node.get("children", [])
        if include_self is False:
            siblings = [n for n in siblings if n != name]
        return self._nodes(*siblings)

    @tree_topology_cache("descendant")
    def descendant_nodes(self, name: str, order: TraversalOrder = "level") -> list[dict[str, Any]]:
        """TODO: match order type & use asked traversal method"""
        descendants = []

        # LEVEL ORDER 
        queue = deque([name])
        while len(queue) > 0:
            node_name = queue.popleft()
            node = self._node_or_raise(node_name)
            node_children = node.get("children", [])
            queue.extend(node_children)
            if node.get("name") != name:
                descendants.append(node)
        return descendants

    @tree_topology_cache("ancestor")
    def ancestor_nodes(self, name: str) -> list[dict[str, Any]]:
        ancestors = []

        node_name = name
        while parent := self.parent_node(node_name):
            node_name = parent["name"]
            ancestors.append(parent)
        return ancestors

    @tree_topology_cache("branch")
    def branch_nodes(self, name: str, order: TraversalOrder = "level") -> list[dict[str, Any]]:
        node = self._node_or_raise(name)

        ancestors = self.ancestor_nodes(name)
        descendants = self.descendant_nodes(name)
        return ancestors[::-1] + [node] + descendants

    def lowest_common_ancestor(self, name: str, other_name: str) -> dict[str, Any]:
        node = self._node_or_raise(name)
        other = self._node_or_raise(other_name)

        node_path = NodePath(node["path"])
        other_path = NodePath(other["path"])
        for ancestor_name in node_path.nodes[::-1]:
            if ancestor_name in other_path.nodes:
                ancestor = self._node_or_raise(ancestor_name)
                return ancestor
        raise KeyError("Unable to find a common ancestor")


    def distance(self, name: str, other_name: str) -> int:
        node = self._node_or_raise(name)
        other = self._node_or_raise(other_name)

        node_path = NodePath(node["path"])
        other_path = NodePath(other["path"])
        ancestor, ancestor_name = None, None
        for ancestor_name in node_path.nodes[::-1]:
            if ancestor_name in other_path.nodes:
                ancestor = self._node(ancestor_name)
                break
        if ancestor is None:
            raise KeyError("Unable to find a common ancestor")
        assert ancestor_name is not None
        
        return node_path.distance(name, ancestor_name) + other_path.distance(other_name, ancestor_name)


    def traversal(self, sub_tree: str | None = None, order: TraversalOrder = "pre") -> Generator[dict[str, Any]]:
        base = sub_tree or self.root()["name"]
        match order:
            case "pre":
                yield from PreOrderTreeTraversal(self.name, self.engine, self.uri).walk(base)
            case "in":
                yield from InOrderTreeTraversal(self.name, self.engine, self.uri).walk(base)
            case "post":
                yield from PostOrderTreeTraversal(self.name, self.engine, self.uri).walk(base)
            case "level":
                yield from self.level_order_traversal(base)
            case _:
                raise ValueError("traveral order must be either: [`pre`, `in`, `post`, `level`]")


    def level_order_traversal(self, *level_node_names: str) -> Generator[dict[str, Any]]:
        next_level = []
        for node in self._nodes(*level_node_names):
            next_level.extend(node.get("children", []))
            yield node
        if next_level:
            yield from self.level_order_traversal(*next_level)
    

    def add_node(self, name: str, parent: str | None, metadata: dict[str, Any] | None = None) -> None:
        if parent is None and self._n_roots() > 0:
            raise KeyError("Tree already has a root node.")

        if parent is not None:
            parent_node = self._node_or_raise(parent)
            parent_name = parent_node["name"]
            parent_path = [parent_node["path"]]
        else:
            parent_name = None
            parent_path = []

        _ = self._node_or_raise(name)
        path = ".".join(parent_path + [name])
        level = len(path.split(".")) - 1
        nid = self.write_topology({"name": name, "path": path, "parent": parent_name, "children": [], "level": level})

        if metadata is None:
            metadata = {}
        metadata.update({"id": nid})
        self.write_metadata(metadata)

        if parent is not None:
            self._add_child(parent, name)


    def remove_node(self, name: str, force: bool = False) -> None:
        node = self._node_or_raise(name)
        if len(node.get("children", [])) > 0 and force is False:
            raise ValueError(f"Node {name} has children. Use `prune` method instead or add `force` argument to this method.")
        elif len(node.get("children", [])) > 0 and force:
            return self.prune_subtree(name)

        parent = node.get("parent", None)
        if parent is not None:
            self._remove_child(parent, name)
        self.delete_topology(node["id"])

    def remove_nodes_where(self, conditions: list, force: bool = False) -> None:
        stmt = QueryBuilder(self.tree, self.metadata).select().where_from_str(conditions)
        with self.sessionmaker() as session:
            nodes = session.execute(stmt()).fetchall()
        names = [n._asdict()["name"] for n in nodes]
        [self.remove_node(name, force) for name in names]

    def prune_subtree(self, name: str) -> None:
        """prune a node all of it's subtree."""
        self.prune(name)

    def update_node(self, name: str, values: dict[str, Any]) -> None:
        node = self._node_or_raise(name)
        self.update_metadata([node["id"]], values)

    def update_nodes_where(self, conditions: list, values: dict[str, Any]) -> None:
        stmt = QueryBuilder(self.tree, self.metadata).select().where_from_str(conditions)
        with self.sessionmaker() as session:
            nodes = session.execute(stmt()).fetchall()
        nids = [n._asdict()["id"] for n in nodes]
        self.update_metadata(nids, values)

    def update_node_name(self, name: str, new_name: str) -> None:
        node = self._node_or_raise(name)
        _ = self._node_or_raise(new_name)

        parent_name = node["parent"]
        children_names = node.get("children", []) 
        if parent_name is not None:
            self._replace_child(parent_name, name, new_name)
        self.update_topology([node["id"]], {"name": new_name})

        for child in children_names:
            self._replace_parent(child, new_name)

    def move_subtree(self, name: str, parent_new_name: str | None) -> None:
        if parent_new_name is None and self._n_roots() > 0:
            raise ValueError("Tree already has a root node.")

        # HANDLE OLD AND NEW PARENT CHILDREN
        node = self._node_or_raise(name)
        node_name = node["name"]
        parent_name = node["parent"]
        if parent_name is not None:
            self._remove_child(parent_name, name)

        if parent_new_name is not None:
            new_parent = self._node_or_raise(parent_new_name)
            self._add_child(parent_new_name, name)
        else:
            new_parent = None

        # HANDLE MOVED NODE AND IT'S SUBTREE
        if new_parent is None:
            path = f"{node_name}"
        else:
            parent_path = new_parent["path"]
            if parent_path is None:
                path = f"{node_name}"
            else:
                path = f"{parent_path}.{node_name}"
        level = len(path.split(".")) - 1
        self.update_topology([node["id"]], {"parent": parent_new_name, "path": path, "level": level})

        children = node.get("children", [])
        for child in children:
            self._update_subtree_path(child, path)

    def _update_subtree_path(self, name: str, parent_path: str | None = None) -> None:
        node = self._node_or_raise(name)
        node_name = node["name"]
        if parent_path is None:
            path = f"{node_name}"
        else:
            path = f"{parent_path}.{node_name}"
        level = len(path.split(".")) - 1

        self.update_topology([node["id"]], {"path": path, "level": level})
        children = node.get("children", [])
        for child in children:
            self._update_subtree_path(child, path)

    def append_list(self, name: str, key: str, value: Any) -> None:
        node = self._node_or_raise(name)
        v: list[Any] = node.get(key, [])
        v.append(value)
        self.update_metadata([node["id"]], {key:v})

    def extend_list(self, name: str, key: str, value: list[Any]) -> None:
        node = self._node_or_raise(name)
        v: list[Any] = node.get(key, [])
        v.extend(value)
        self.update_metadata([node["id"]], {key:v})

    def pop_list(self, name: str, key: str, value: Any = -1) -> Any:
        node = self._node_or_raise(name)
        v: list[Any] = node.get(key, [])
        try: 
            index = v.index(value)
        except IndexError:
            return None

        poped = v.pop(index)
        self.update_metadata([node["id"]], {key:v})
        return poped

    def add_object_key(self, name: str, key: str, path: str, value: str) -> None:
        node = self._node_or_raise(name)
        obj = node.get(key, {})
        for k in path.split("."):
            ...

    def pop_object_key(self, name: str, key: str, path: str) -> None:
        node = self._node_or_raise(name)
        obj = node.get(key, {})
        for k in path.split("."):
            ...

    def export_to_file(self, outfile: str | Path, subtree: str | None = None, order: TraversalOrder = "pre") -> None:
        with open(outfile, "w+") as f:
            for node in self.traversal(subtree, order):
                payload = json.dumps(node)
                f.write(payload + "\n")

    def nodes(self, *conditions: ColumnElement) -> list[dict[str, Any]]:
        stmt = select(self.tree).where(*conditions)
        return [n._asdict() for n in self.execute(stmt).fetchall()]

    def nodes_relations(self, relation: BaseRelations, *conditions: ColumnElement) -> list[dict[str, Any]]:
        ...