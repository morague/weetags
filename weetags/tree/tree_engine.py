from __future__ import annotations

import json
from pathlib import Path
from functools import wraps
import re



from sqlalchemy import Subquery, Table, select, func, ColumnElement
from sqlalchemy.sql import Subquery
from sqlalchemy.engine import Engine as BaseEngine

from typing import Any, Generator, Literal, Sequence, Type

from weetags.common.conditions import Condition, ConditionType, SqlFunction
from weetags.common.types import Relation, TraversalOrder, OnCollision, BaseRelations
from weetags.common import EngineURI, Engine, BoundEngine, QueryBuilder
from weetags.common.path_utils import NodePath, NodePathCollection
import weetags.common.utils as cutils
import weetags.common.serializer as serializers
from weetags.tree.fields import Relation as Rel
import weetags.tree.update as upt
from weetags.tree.tree_cache import TreeCache
from weetags.tree.traversal import (
    PreOrderTreeTraversal, 
    InOrderTreeTraversal, 
    PostOrderTreeTraversal
)
from weetags.tree.importer import Importer

def query_cache(f):
    @wraps(f)
    def wrapper(instance: TreeEngine, *args: Any, **kwargs: Any):
        if instance.cache is not None:
            signature = cutils.Signature(f, args, kwargs)
            check = instance.cache.check_cached_result_key(signature.sha1_digest)
            if check:
                return instance.cache.get_cached_result(signature.sha1_digest)

            result = f(instance, *args, **kwargs)
            instance.cache.set_query_result(signature.sha1_digest, result)

            name = signature.get_parameter("name", "all")
            instance.cache.append_node_tracker(name, signature.sha1_digest)
            return result
        else:
            return f(instance, *args, **kwargs)
    return wrapper

def clear_query_cache(scope: Literal["partial", "all"]):
    def inner(f):

        @wraps(f)
        def wrapper(instance: TreeEngine, *args: Any, **kwargs: Any):
            result = f(instance, *args, **kwargs)
            if instance.cache is not None:
                signature = cutils.Signature(f, args, kwargs)
                match scope:
                    case "partial":
                        name = signature.get_parameter("name", "all")
                        instance.cache.clear_tracker(name)
                    case "all":
                        instance.cache.clear_all_tracker()
            return result
        return wrapper
    return inner


def topology_cache(relation: Relation):
    def inner(f):
        @wraps(f)
        def wrapper(instance: TreeEngine, *args: Any, **kwargs: Any):
            if instance.cache is not None:
                signature = cutils.Signature(f, args, kwargs)
                try:
                    match relation:
                        case "parent":
                            nodes = instance.cache.parent(*args, **kwargs)                            
                        case "children":
                            nodes = instance.cache.children(*args, **kwargs)
                        case "siblings":
                            nodes = instance.cache.siblings(*args, **kwargs)
                        case "descendants":
                            nodes = instance.cache.descendants(*args, **kwargs)
                        case "ancestors":
                            nodes = instance.cache.ancestors(*args, **kwargs)
                        case "branch":
                            nodes = instance.cache.branch(*args, **kwargs)
                        case _:
                            raise NotImplementedError()
                    fields = signature.get_parameter("fields")
                except Exception as e:
                    return f(instance, *args, **kwargs)    

                if isinstance(nodes, list):
                    return instance._nodes(*nodes, fields=fields)
                elif isinstance(nodes, str):
                    return instance._node(nodes)
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

    def __init__(
        self, 
        name: str, 
        base_engine: BaseEngine, 
        uri: EngineURI, 
        cache: TreeCache | None = None, 
        serializer: serializers.BaseSerializer | None = None
    ) -> None:
        super().__init__(name, base_engine, uri, serializer)

        self.cache = cache
        if self.cache is not None:
            self.cache.bind(name, self)

    @classmethod
    def build(cls, tree_name: str, uri: EngineURI, cache: TreeCache | None = None) -> TreeEngine:
        base = Engine.from_uri(uri)
        return cls.from_engine(tree_name, base, cache)

    @classmethod
    def from_configs(cls, configs: dict[str, Any]) -> TreeEngine:
        name = configs.get("name", None)
        if name is None:
            raise KeyError(f"Missing key: name.")

        uri = configs.get("uri", {})
        if isinstance(uri, dict):
            uri = EngineURI(**uri)

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
    @query_cache
    def tree_size(self) -> int:
        stmt = select(func.count(self.tree.c.id))
        return self._serialize_value(stmt)

    @query_cache
    def tree_degree(self) -> int:
        stmt = select(func.max(func.json_array_length(self.tree.c.children)))
        return self._serialize_value(stmt)

    @query_cache
    def leaves(self) -> list[dict[str, Any]]:
        stmt = select(self.tree).where(func.json_array_length(self.tree.c.children) == 0)
        return self._serialize_records(stmt)

    @query_cache
    def breadth(self) -> int:
        stmt = select(func.count(self.tree.c.id)).where(func.json_array_length(self.tree.c.children) == 0)
        return self._serialize_value(stmt)

    @query_cache
    def tree_depth(self) -> int:
        stmt = select(func.max(self.tree.c.level))
        return self._serialize_value(stmt)

    @query_cache
    def width(self, level: int) -> int:
        if level > self.tree_depth():
            raise ValueError("Tree depth is lower than level.")

        stmt = select(func.count(self.tree.c.id)).where(self.tree.c.level == level)
        return self._serialize_value(stmt)

    @query_cache
    def root(self, fields: list[str] | None = None) -> dict[str, Any]:
        stmt = select(*self._get_selected(fields)).where(self.tree.c.parent == None)
        data = self._serialize_records(stmt)
        if len(data) == 0:
            raise ValueError(f"Tree {self.tree.name} has no root.")
        elif len(data) > 1:
            raise ValueError(f"Tree {self.tree.name} has too many roots")
        return data[0]

    def _n_roots(self) -> int:
        return len(self.roots(self.name))

    # TREE PARTS ENUMERATION
    @query_cache
    def node(self, name: str, fields: list[str] | None = None) -> dict[str, Any] | None:
        return self._node(name)

    def nodes(self, *conditions: ColumnElement | Rel) -> list[dict[str, Any]]:
        tables, c, relations = [self.tree], [], []
        for condition in conditions:
            if isinstance(condition, Rel):
                relations.append(condition.as_condition())
            else:
                t = getattr(condition, "t", None)
                if t is not None and t not in tables:
                    tables.append(t)
                c.append(condition)

        r = self._parse_rel_conditions(relations)
        stmt = select(self.tree).select_from(*tables).where(*c, *r)
        return self._serialize_records(stmt)

    def remove_nodes(self, *conditions: ColumnElement | Rel, force: bool = False) -> None:
        tables, c, relations = [self.tree], [], []
        for condition in conditions:
            if isinstance(condition, Rel):
                relations.append(condition.as_condition())
            else:
                t = getattr(condition, "t", None)
                if t is not None and t not in tables:
                    tables.append(t)
                c.append(condition)

        r = self._parse_rel_conditions(relations)
        stmt = select(self.tree.c.name).select_from(*tables).where(*c, *r)
        names =  self._serialize_list(stmt)
        [self.remove_node(name, force) for name in names]

    def update_nodes(self, *conditions: ColumnElement | Rel, values: dict[str, Any]) -> None:
        tables, c, relations = [self.tree], [], []
        for condition in conditions:
            if isinstance(condition, Rel):
                relations.append(condition.as_condition())
            else:
                t = getattr(condition, "t", None)
                if t is not None and t not in tables:
                    tables.append(t)
                c.append(condition)

        r = self._parse_rel_conditions(relations)
        stmt = select(self.tree.c.id).select_from(*tables).where(*c, *r)
        nids = self._serialize_list(stmt)
        self.update_metadata(nids, values)


    @query_cache
    def nodes_where(
        self, 
        relations: Sequence[Condition] | None = None,
        conditions: Sequence[Condition] | None = None,
        fields: list[str] | None = None, 
        page: int = 0, 
        page_size: int = 10
    ) -> list[dict[str, Any]]:
        fields = fields or []
        r = self._parse_rel_conditions(relations)
        t, q = self._parse_cmp_conditions(conditions)
        stmt = (
            QueryBuilder(self.tree, self.metadata)
            .select()
            .select_from(*t)
            .fields_from_str(*fields)
            .where(*q, *r)
            .offset(page * page_size)
            .limit(page_size)
            .order_by(self.tree.c.path)
        )
        return self._serialize_records(stmt())

    def closest(
        self,
        name: str,
        relations: Sequence[Condition] | None = None,
        conditions: Sequence[Condition] | None = None,
        fields: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        fields = fields or []
        r = self._parse_rel_conditions(relations)
        t, q = self._parse_cmp_conditions(conditions)
        stmt = (
            QueryBuilder(self.tree, self.metadata)
            .select()
            .select_from(*t)
            .fields_from_str(*fields)
            .where(*q, *r)
        )

        centroid = self._node_or_raise(name)
        centroid_path = NodePath(centroid["path"])

        nodes = self._serialize_records(stmt())
        dists = self._distance(centroid_path, *[n["path"] for n in nodes])
        results = []
        if len(dists) > 0:
            closest_dist = min(dists)
            indexes = [i for i in range(len(dists)) if dists[i] == closest_dist]
            results = [{"distance": closest_dist, "node": nodes[i]} for i in indexes]
        return results

    def closest_nodes(self, name: str, *conditions: ColumnElement | Rel) -> list[dict[str, Any]]:
        tables, c, relations = [self.tree], [], []
        for condition in conditions:
            if isinstance(condition, Rel):
                relations.append(condition.as_condition())
            else:
                t = getattr(condition, "t", None)
                if t is not None and t not in tables:
                    tables.append(t)
                c.append(condition)

        r = self._parse_rel_conditions(relations)
        stmt = select(self.tree).select_from(*tables).where(*c, *r)
        nodes = self._serialize_records(stmt)

        centroid = self._node_or_raise(name)
        centroid_path = NodePath(centroid["path"])

        dists = self._distance(centroid_path, *[n["path"] for n in nodes])
        results = []
        if len(dists) > 0:
            closest_dist = min(dists)
            indexes = [i for i in range(len(dists)) if dists[i] == closest_dist]
            results = [{"distance": closest_dist, "node": nodes[i]} for i in indexes]
        return results

    @query_cache
    @topology_cache("parent")
    def parent_node(self, name: str, fields: list[str] | None = None) -> dict[str, Any] | None:
        node = self._node_or_raise(name, fields)        
        parent_name = node.get("parent", None)
        if parent_name is None:
            return None
        return self._node(parent_name)

    @query_cache
    @topology_cache("children")
    def children_nodes(self, name: str, fields: list[str] | None = None) -> list[dict[str, Any]]:
        node = self._node_or_raise(name)
        children_names = node.get("children",  [])
        return self._nodes(*children_names, fields=fields)

    @query_cache
    @topology_cache("siblings")
    def sibling_nodes(self, name: str, fields: list[str] | None = None, include_self: bool = False) -> list[dict[str, Any]]:
        parent = self.parent_node(name)
        if parent is None:
            return []

        parent_name = parent["name"]
        parent_node = self._node_or_raise(parent_name)
        siblings = parent_node.get("children", [])
        if include_self is False:
            siblings = [n for n in siblings if n != name]
        return self._nodes(*siblings, fields= fields)

    @query_cache
    @topology_cache("descendants")
    def descendant_nodes(self, name: str, fields: list[str] | None = None, include_self: bool = False) -> list[dict[str, Any]]:
        paths = self.subtree_topology(name)
        descendants = NodePathCollection(*paths).descendants_of(name)
        if include_self:
            descendants.insert(0, name)
        return self._nodes(*descendants, fields= fields)

    @query_cache
    @topology_cache("ancestors")
    def ancestor_nodes(self, name: str, fields: list[str] | None = None, include_self: bool = False) -> list[dict[str, Any]]:
        paths = self.subtree_topology(name)
        ancestors = NodePathCollection(*paths).ancestors_of(name)
        if include_self:
            ancestors.append(name)
        return self._nodes(*ancestors, fields= fields)

    @query_cache
    @topology_cache("branch")
    def branch_nodes(self, name: str, fields: list[str] | None = None) -> list[dict[str, Any]]:
        paths = self.subtree_topology(name)
        branch = NodePathCollection(*paths).branch_of(name)
        return self._nodes(*branch, fields= fields)

    @query_cache
    def lowest_common_ancestor(self, name: str, other_name: str, fields: list[str] | None = None) -> dict[str, Any]:
        node = self._node_or_raise(name)
        other = self._node_or_raise(other_name)

        node_path = NodePath(node["path"])
        other_path = NodePath(other["path"])
        for ancestor_name in node_path.nodes[::-1]:
            if ancestor_name in other_path.nodes:
                ancestor = self._node_or_raise(ancestor_name, fields= fields)
                return ancestor
        raise KeyError("Unable to find a common ancestor")


    @query_cache
    def distance(self, name: str, other_name: str) -> int:
        node = self._node_or_raise(name)
        other = self._node_or_raise(other_name)
        dist = self._distance(node["path"], other["path"])
        return dist[0]
    
    def _distance(self, path: str | NodePath, *other_paths: str | NodePath) -> list[int]:
        def _dist(p: NodePath, commom_ancestor: str):
            dist = 0
            for n in p.nodes[::-1]:
                if n != commom_ancestor:
                    dist += 1
                else:
                    break
            return dist

        dists, centroid_path = [], NodePath(path)
        for i, other in enumerate(other_paths):
            opath = NodePath(other)

            j, commom_ancestor = opath.len - 1, None
            while j >= 0:
                inner_node = opath.nodes[j]
                if inner_node in centroid_path.nodes:
                    commom_ancestor = inner_node
                    break
                j -= 1
            if commom_ancestor is None:
                raise ValueError("Malformed tree")
            
            dist = _dist(centroid_path, commom_ancestor)
            dist += _dist(opath, commom_ancestor)
            dists.append(dist)
        return dists

    def traversal(self, sub_tree: str | None = None, order: TraversalOrder = "pre") -> Generator[dict[str, Any]]:
        base = sub_tree or self.root()["name"]
        match order:
            case "pre":
                yield from PreOrderTreeTraversal(self.name, self.engine, self.uri, self._serializer).walk(base)
            case "in":
                yield from InOrderTreeTraversal(self.name, self.engine, self.uri, self._serializer).walk(base)
            case "post":
                yield from PostOrderTreeTraversal(self.name, self.engine, self.uri, self._serializer).walk(base)
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
    




    @clear_query_cache("all")
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

    def graft_nodes(
        self, 
        nodes: list[dict[str, Any]], 
        keymap: dict[str, Any] | None = None, 
        on_collision: OnCollision = "raise"
    ) -> None:
        Importer(self.name, self, batch_size=100).loads(nodes, keymap, on_collision)

    @clear_query_cache("all")
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

    @clear_query_cache("all")
    def remove_nodes_where(
        self, 
        relations: Sequence[Condition] | None = None, 
        conditions: Sequence[Condition] | None = None, 
        force: bool = False
    ) -> None:
        r = self._parse_rel_conditions(relations)
        t, q = self._parse_cmp_conditions(conditions)

        stmt = (
            QueryBuilder(self.tree, self.metadata)
            .select()
            .fields(self.tree.c.name)
            .select_from(*t)
            .where(*q, *r)
        )
        names = self._serialize_list(stmt())
        [self.remove_node(name, force) for name in names]

    @clear_query_cache("all")
    def prune_subtree(self, name: str) -> None:
        """prune a node all of it's subtree."""
        self.prune(name)

    @clear_query_cache("partial")
    def update_node(self, name: str, values: dict[str, Any]) -> None:
        node = self._node_or_raise(name)
        self.update_metadata([node["id"]], values)

    @clear_query_cache("all")
    def update_nodes_where(
        self,
        values: dict[str, Any],
        relations: Sequence[Condition] | None = None, 
        conditions: Sequence[Condition] | None = None, 
    ) -> None:
        r = self._parse_rel_conditions(relations)
        t, q = self._parse_cmp_conditions(conditions)

        stmt = (
            QueryBuilder(self.tree, self.metadata)
            .select()
            .fields(self.tree.c.id)
            .select_from(*t)
            .where(*q, *r)
        )
        nids = self._serialize_list(stmt())
        self.update_metadata(nids, values)

    @clear_query_cache("partial")
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

    @clear_query_cache("all")
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

    @clear_query_cache("partial")
    def append_list(self, name: str, key: str, value: Any) -> None:
        node = self._node_or_raise(name)
        v: list[Any] = node.get(key, [])
        v.append(value)
        self.update_metadata([node["id"]], {key:v})

    @clear_query_cache("partial")
    def extend_list(self, name: str, key: str, value: list[Any]) -> None:
        node = self._node_or_raise(name)
        v: list[Any] = node.get(key, [])
        v.extend(value)
        self.update_metadata([node["id"]], {key:v})

    @clear_query_cache("partial")
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

    @clear_query_cache(scope="partial")
    def set_object_key(self, name: str, path: str, value: Any) -> None:
        upt.JsonObjectUpdateKey(self).apply(name, path, value)

    @clear_query_cache(scope="partial")
    def pop_object_key(self, name: str, path: str,) -> None:
        upt.JsonObjectRemoveKey(self).apply(name, path)

    @clear_query_cache(scope="partial")
    def object_append_list(self, name: str, path: str, value: Any) -> None:
        upt.JsonObjectAppendList(self).apply(name, path, value)

    @clear_query_cache(scope="partial")
    def object_extend_list(self, name: str, path: str, value: list[Any]) -> None:
        upt.JsonObjectExtendList(self).apply(name, path, value)

    @clear_query_cache(scope="partial")
    def object_pop_list(self, name: str, path: str, index: int) -> None:
        upt.JsonObjectPopList(self).apply(name, path, index)

    def import_from_file(self, path: str | Path, keymap: dict[str, Any] | None = None, on_collision: OnCollision = "raise") -> None:
        Importer(self.name, self, batch_size=100).load(path, keymap, on_collision)

    def export_to_file(self, outfile: str | Path, subtree: str | None = None, order: TraversalOrder = "pre") -> None:
        with open(outfile, "w+") as f:
            for node in self.traversal(subtree, order):
                payload = json.dumps(node)
                f.write(payload + "\n")

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

    def _relation_as_subquery(self, relation: BaseRelations, value: str) -> Subquery:
        match relation:
            case "ancestors":
                paths = self.subtree_topology(value)
                names = NodePathCollection(*paths).ancestors_of(value)
                q = select(self.tree.c.id).where(self.tree.c.name.in_(names)).subquery()
            case "descendants":
                paths = self.subtree_topology(value)
                names = NodePathCollection(*paths).descendants_of(value)
                q = select(self.tree.c.id).where(self.tree.c.name.in_(names)).subquery()
            case "branch":
                paths = self.subtree_topology(value)
                names = NodePathCollection(*paths).branch_of(value)
                q = select(self.tree.c.id).where(self.tree.c.name.in_(names)).subquery()
            case "children":
                node = self._node_or_raise(value)
                names = node.get("children",  [])
                q = select(self.tree.c.id).where(self.tree.c.name.in_(names)).subquery()
            case "parent":
                node = self._node_or_raise(value)        
                names = node.get("parent", None)
                if names is None:
                    raise ValueError(f"No parent for node: {value}")
                q = select(self.tree.c.id).where(self.tree.c.name.in_([names])).subquery()
            case "siblings":
                parent = self.parent_node(value)
                if parent is None:
                    raise ValueError(f"No parent for node: {value}") 
                parent_name = parent["name"]
                parent_node = self._node_or_raise(parent_name)
                names = [name for name in parent_node.get("children", []) != value]
                q = select(self.tree.c.id).where(self.tree.c.name.in_([names])).subquery()
            case _:
                raise ValueError("???")
        return q

    def _parse_rel_conditions(self, relations: Sequence[Condition] | None) -> Sequence[ColumnElement]:
        if relations is None:
            return []
        rel = []
        for r in relations:
            subquery = self._relation_as_subquery(**r.as_dict())
            rel.append(self.tree.c.id.in_(subquery)) # pyright: ignore
        return rel

    def _apply_operator(self, fname: str, operator: str, value: Any) -> ColumnElement:
        c = self._field_or_raise(self.tree, fname)
        e = getattr(c, operator)
        return e(value)

    def _apply_f_operator(self, func: SqlFunction, func_args: Sequence[Any], operator: str, value: Any) -> tuple[Table, ColumnElement]:
        f = func.to_callable()
        fname, *args = func_args
        args = [arg for arg in args if arg is not None] # remove None for for_each, however might not work long term
        c = self._field_or_raise(self.tree, fname)
        t = f(c, *args).table_valued('value', joins_implicitly=True)
        e = getattr(t.c.value, operator)
        return (t, e(value))

    def _parse_cmp_conditions(self, conditions: Sequence[Condition] | None) -> tuple[Sequence[Table], Sequence[ColumnElement]]:
        t, e = [], []
        if conditions is None:
            return (t, e)
        for condition in conditions:
            match condition.type:
                case ConditionType.OPERATION:
                    payload = condition.as_dict()
                    e.append(self._apply_operator(**payload))
                case ConditionType.FSQL_OPERATION:
                    payload = condition.as_dict()
                    subt, expr = self._apply_f_operator(**payload)
                    t.append(subt)
                    e.append(expr)
                case _:
                    raise ValueError()
        return (t, e)
