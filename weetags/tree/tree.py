from __future__ import annotations

from pathlib import Path
from typing import Any, Generator

from weetags.common import Engine, EngineURI
from weetags.common.configs import FieldType
from weetags.common.types import TraversalOrder, OnCollision
from weetags.tree.tree_engine import TreeEngine
from weetags.common.alteration import Alteration
from weetags.tree.tree_cache import TreeCache
from weetags.tree.importer import Importer
from weetags.tree.node import Node

class TreeTopology:
    name: str
    _engine: TreeEngine

    def __init__(self, name: str, engine: TreeEngine) -> None:
        self.name = name
        self._engine = engine

    def add_node(self, name: str, parent: str, metadata: dict[str, Any] | None = None) -> None:
        self._engine.add_node(name, parent, metadata)

    def graft_nodes(self, nodes: list[dict[str, Any]], keymap: dict[str, Any] | None = None, on_collision: OnCollision = "raise") -> None:
        Importer(self.name, self._engine, batch_size=100).loads(nodes, keymap, on_collision)

    def import_from_file(self, path: str | Path, keymap: dict[str, Any] | None = None, on_collision: OnCollision = "raise") -> None:
        Importer(self.name, self._engine, batch_size=100).load(path, keymap, on_collision)

    def export_to_file(self, outfile: str | Path, subtree: str | None = None, order: TraversalOrder = "pre") -> None:
        self._engine.export_to_file(outfile, subtree, order)

    def move_subtree(self, name: str, parent_new_name: str) -> None:
        self._engine.move_subtree(name, parent_new_name)

    def remove_node(self, name: str, force: bool = False) -> None:
        self._engine.remove_node(name, force)

    def remove_nodes_where(self, conditions: list, force: bool = False) -> None:
        self._engine.remove_nodes_where(conditions, force)

    def prune_subtree(self, name: str) -> None:
        self._engine.prune_subtree(name)

class TreeMetadata:
    name: str
    _engine: TreeEngine

    def __init__(self, name: str, engine: TreeEngine) -> None:
        self.name = name
        self._engine = engine

    def update_node(self, name: str, values: dict[str, Any]) -> None:
        self._engine.update_node(name, values)

    def update_nodes_where(self, conditions: list, values: dict[str, Any]) -> None:
        self._engine.update_nodes_where(conditions, values)

    def append_list(self, name: str, key: str, value: Any) -> None:
        self._engine.append_list(name, key, value)

    def append_object(self) -> None:
        raise NotImplementedError()

    def extend_list(self, name: str, key: str, value: list[Any]) -> None:
        self._engine.extend_list(name, key, value)

    def extend_object(self) -> None:
        raise NotImplementedError()

    def pop_list(self, name: str, key: str) -> None:
        self._engine.pop_list(name, key)

    def pop_object(self) -> None:
        raise NotImplementedError()

class Tree:
    name: str
    _engine: TreeEngine
    
    def __init__(
        self, 
        name: str, 
        engine: TreeEngine, 
    ) -> None:
        
        self.name = name
        self._engine = engine
        self._engine.reflect()
    
    def __repr__(self) -> str:
        return f"<Tree: {self.name}>"

    @property
    def Alteration(self) -> Alteration:
        return Alteration(self.name, self._engine)

    @property
    def Topology(self) -> TreeTopology:
        return TreeTopology(self.name, self._engine)

    @property
    def Metadata(self) -> TreeMetadata:
        return TreeMetadata(self.name, self._engine)

    @property
    def size(self) -> int:
        return self._engine.tree_size()

    @property
    def depth(self) -> int:
        """maximum number of levels in the tree"""
        return self._engine.tree_depth()

    @property
    def degree(self) -> int:
        """maximum number of children for one node in the tree"""
        return self._engine.tree_degree()

    @property
    def breadth(self) -> int:
        """number of leaves in the tree"""
        return self._engine.breadth()

    @property
    def empty(self) -> bool:
        return self.size == 0

    @property
    def root(self) -> Node | None:
        return self._into_node(self._engine.root())

    @property
    def leaves(self) -> list[Node]:
        return self._into_nodes(self._engine.leaves())

    @property
    def topology_fields(self) -> list[str]:
        return self._engine._topology.columns.keys()

    @property
    def metadata_fields(self) -> list[str]:
        return self._engine._metadata.columns.keys()

    @property
    def topology_fields_dtype(self) -> dict[str, str]:
        return {c.name:FieldType.from_sqlalchemy(c.type).value for c in self._engine._topology.columns.values()}

    @property
    def metadata_fields_dtype(self) -> dict[str, str]:
        return {c.name:FieldType.from_sqlalchemy(c.type).value for c in self._engine._metadata.columns.values()}

    @property
    def info(self) -> dict[str, Any]:
        cache = self._engine.cache
        if cache is not None:
            cache = cache.cache_engine.name
        return {
            "name": self.name,
            "size": self.size,
            "depth": self.depth,
            "breadth": self.breadth,
            "topology": self.topology_fields_dtype,
            "metadata": self.metadata_fields_dtype,
            "engine": {
                "database": {
                    "dialect": self._engine.uri.dialect,
                    "database": self._engine.uri.database,
                },
                "cache": cache
            }
        }

    @classmethod
    def initialize(cls, name: str, engine_uri: EngineURI, cache: TreeCache | None = None) -> Tree:
        engine = TreeEngine.from_uri(name, engine_uri, cache)
        return cls(name, engine)

    @classmethod
    def from_configs(cls, name: str, engine_uri: dict[str, Any], cache: dict[str, Any] | None = None) -> Tree:
        tree_cache = None
        if cache is not None:
            tree_cache = TreeCache(**cache)

        uri = EngineURI(**engine_uri)
        return cls.initialize(name, uri, tree_cache)

    @classmethod
    def from_engine(cls, name: str, engine: Engine, cache: TreeCache | None = None) -> Tree:
        tree_engine = TreeEngine.from_engine(name, engine, cache)
        return cls(name, tree_engine)

    def sync(self) -> None:
        self._engine.reflect()

    def width(self, level: int) -> int:
        return self._engine.width(level)

    def node(self, name: str) -> Node | None:
        return self._into_node(self._engine.node(name))

    def nodes_where(self, conditions: list) -> list[Node]:
        return self._into_nodes(self._engine.nodes_where(conditions))

    def parent_node(self, name: str) -> Node | None:
        """return parent node of a given node name"""
        return self._into_node(self._engine.parent_node(name))

    def children_nodes(self, name: str) -> list[Node]:
        """return list of children nodes of a given node name"""
        return self._into_nodes(self._engine.children_nodes(name))

    def sibling_nodes(self, name: str, include_self: bool = False) -> list[Node]:
        """return list of siblings nodes of a given node name"""
        return self._into_nodes(self._engine.sibling_nodes(name, include_self))

    def ancestor_nodes(self, name) -> list[Node]:
        """return list of ancestors nodes of a given node name"""
        return self._into_nodes(self._engine.ancestor_nodes(name))

    def descendant_nodes(self, name: str, order: TraversalOrder = "level") -> list[Node]:
        """return list of descendants nodes of a given node name"""
        return self._into_nodes(self._engine.descendant_nodes(name, order))

    def branch_nodes(self, name: str, order: TraversalOrder = "pre") -> list[Node]:
        return self._into_nodes(self._engine.branch_nodes(name, order))

    def distance(self, name: str, other_name: str) -> int:
        return self._engine.distance(name, other_name)

    def lowest_common_ancestor(self, name: str, other_name: str) -> Node | None:
        return self._into_node(self._engine.lowest_common_ancestor(name, other_name))

    def traversal(self, sub_tree: str | None = None, order: TraversalOrder = "pre") -> Generator[Node]:
        for d in self._engine.traversal(sub_tree, order):
            node = self._into_node(d)
            assert node is not None
            yield node

    def draw(self, subtree: str | None = None, style: None = None) -> None:
        print(self._draw(subtree, style))

    def _draw(self, subtree: str | None = None, style: None = None) -> str:
        raise NotImplementedError()

    def _into_node(self, data: dict[str, Any] | None) -> Node | None:
        if data is None:
            return None
        return Node(**data).connect(self._engine, self.name)

    def _into_nodes(self, data: list[dict[str, Any]]) -> list[Node]:
        return [Node(**d).connect(self._engine, self.name) for d in data]