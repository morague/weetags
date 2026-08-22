from __future__ import annotations
from abc import ABC, abstractmethod

from sqlalchemy.engine import Engine as BaseEngine

from typing import Any, Generator

from weetags.common import EngineURI, BoundEngine


class TreeTraversal(ABC, BoundEngine):
    name: str
    topology: list[str]
    visited: list[str]

    def __init__(self, name: str, base_engine: BaseEngine, uri: EngineURI):
        super().__init__(name, base_engine, uri)
        self.name = name

    @abstractmethod
    def walk(self, sub_tree: str) -> Generator[dict[str, Any]]:
        raise NotImplementedError()



class PreOrderTreeTraversal(TreeTraversal):
    topology: list[str]
    visited: list[str]

    def __init__(self, name, base_engine, uri):
        super().__init__(name, base_engine, uri)

    def walk(self, sub_tree: str):
        node = self._node(sub_tree)
        if node is None:
            raise ValueError(f"Unknown node name: {sub_tree}")
    
        self.topology = self.subtree_topology(sub_tree)
        self.visited = []
        yield from self._walk_sub_tree(sub_tree)

    def _walk_sub_tree(self, sub_tree: str) -> Generator[dict[str, Any]]:
        node = self._node(sub_tree)
        if node is None:
            raise ValueError(f"Unknown node name: {sub_tree}")

        node_path: str = node["path"]
        self.visited.append(node_path)
        yield node

        topology = self.subtree_topology(sub_tree)
        for path in topology:
            if path not in self.visited:
                child_name = path.split(".")[len(node_path.split("."))]
                yield from self._walk_sub_tree(child_name)

class InOrderTreeTraversal(TreeTraversal):
    sub_tree_root: str
    topology: list[str]
    visited: list[str]

    def __init__(self, name, base_engine, uri):
        super().__init__(name, base_engine, uri)

    def walk(self, sub_tree):
        node = self._node(sub_tree)
        if node is None:
            raise ValueError(f"Unknown node name: {sub_tree}")

        self.sub_tree_root = sub_tree
        self.topology = self.subtree_topology(sub_tree)
        self.visited = []
        for path in self.topology:
            if path not in self.visited:
                node_name = path.split(".")[-1]
                yield from self._tree_ascend(node_name)

    def _tree_ascend(self, name: str) -> Generator[dict[str, Any]]:
        node = self._node(name)
        if node is None:
            raise ValueError(f"Unknown node name: {name}")
        node_name = node["name"]
        node_path: str = node["path"]
        node_parent = node.get("parent")

        previous_leaf = self._previous_topological_leaf()
        next_leaf = self._next_topological_leaf()

        if (
            node_path not in self.visited
            and (
                node_name != self.sub_tree_root
            ) or (
                node_name == self.sub_tree_root
                and previous_leaf is not None
                and next_leaf is not None
                and self._path_similirity(previous_leaf, next_leaf) == 1
            )
        ):
            self.visited.append(node_path)
            yield node

        if node_parent is not None:
            yield from self._tree_ascend(node_parent)

    def _next_topological_leaf(self) -> str | None:
        for path in self.topology:
            if path not in self.visited:
                return path
        return None

    def _previous_topological_leaf(self) -> str | None:
        previous = None
        for path in self.topology:
            if path not in self.visited:
                return previous
            else:
                previous = path
        return None

    def _path_similirity(self, p1: str, p2: str) -> int:
        sim = 0
        for p in p1.split("."):
            if p in p2.split("."):
                sim += 1
        return sim

class PostOrderTreeTraversal(TreeTraversal):
    topology: list[str]
    visited: list[str]

    def __init__(self, name, base_engine, uri):
        super().__init__(name, base_engine, uri)

    def walk(self, sub_tree):
        node = self._node(sub_tree)
        if node is None:
            raise ValueError(f"Unknown node name: {sub_tree}")

        self.sub_tree_root = sub_tree
        self.topology = self.subtree_topology(sub_tree)
        self.visited = []

        for path in self.topology:
            if path not in self.visited:
                yield from self._ascend_tree(path)

    def _ascend_tree(self, path: str) -> Generator[dict[str,Any]]:
        if path not in self.visited:
            node_name = path.split(".")[-1]
            node = self._node(node_name)
            assert node is not None

            if path not in self.visited:
                self.visited.append(path)
                yield node

            parent_name = node["parent"]
            if parent_name is not None:
                parent_tree_topology = self.subtree_topology(parent_name)
                for p in parent_tree_topology:
                    if p not in self.visited:
                        yield from self._ascend_tree(p)

                parent_path = ".".join(path.split(".")[:-1])
                yield from self._ascend_tree(parent_path)
