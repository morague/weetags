from __future__ import annotations

from collections import defaultdict
from typing import Generator, Literal


def contains_seperator(f):
    def wrapper(instance: NodePath, *args, **kwargs):
        seperator = args[0]
        if seperator not in instance.nodes:
            raise ValueError(f"Path does not contain the separator: {seperator}")
        return f(instance, *args, **kwargs)
    return wrapper

def validate_fix(f):
    def wrapper(instance: NodePath, *args, **kwargs):
        size = args[0]
        if size == 0:
            raise ValueError("`size` must be > 0.")
        if size > len(instance.nodes):
            raise ValueError("`size` must be < than the path size")
        return f(instance, *args, **kwargs)
    return wrapper


class NodePath:
    """
    A Node Path is the list of nodes seperating the root from the leaf of the Path.
    A subPath is a path of size <= of the NodePath that is contained in the NodePath.
        Subpath, like suffixes, are not necessary starting from the root.
    
    main NodePath operations:
    contains:
        node (is a node name part of the path ?)
        subpath (is a subpath part of the path ?)
    
    prefix & suffix:
        prefix: get the n first nodes from the path as a subpath
        prefix: get the n last nodes from the path as a subpath
    
    """

    def __init__(self, path: str | NodePath) -> None:
        if isinstance(path, NodePath):
            self.path = path.path
            self.nodes = path.nodes
            self.len = path.len
        else:
            self.path = path
            self.nodes = path.split(".")
            self.len = len(self.nodes)

    def __repr__(self) -> str:
        return f"<NodePath: {self.path}>"

    def contains_node(self, node: str) -> bool:
        return node in self.nodes

    def contains_subpath(self, path: str) -> bool:
        return path in self.path

    @validate_fix
    def suffix(self, size: int, /) -> str:  
        index = len(self.nodes) - size
        return ".".join(self.nodes[index:])

    @validate_fix
    def prefix(self, size: int, /) -> str:
        return ".".join(self.nodes[:size])

    def subpath(self, node: str, node_is: Literal["leaf", "root"] = "root") -> str:
        if self.contains_node(node) is False:
            raise ValueError(f"{node} is not part of path: {self.path}.")
        index = self.nodes.index(node)

        if node_is == "leaf":
            subpath = ".".join(self.nodes[:index + 1])
        else:
            subpath = ".".join(self.nodes[index:])
        return subpath

    @contains_seperator
    def lstrip_until(self, node: str, /, include_node: bool = False) -> str:
        index = self.nodes.index(node)
        if include_node:
            index += 1
        if index >= len(self.nodes):
            raise ValueError("last node from a path has to be excluded")
        return ".".join(self.nodes[index:])

    @contains_seperator
    def rstrip_from(self,  node: str, /, include_node: bool = False) -> str:
        index = self.nodes.index(node)
        if include_node is False:
            index += 1
        if index == 0:
            raise ValueError("first node from a path has to be included")
        return ".".join(self.nodes[:index])

    def distance(self, node0: str, node1: str) -> int:
        if not all([self.contains_node(node0), self.contains_node(node1)]):
            raise ValueError(f"{node0} or {node1} is not part of path: {self.path}.")
        name_pos = self.nodes.index(node0)
        other_pos = self.nodes.index(node1)
        return abs(name_pos - other_pos)

    def iter(self) -> Generator[str]:
        for node in self.nodes:
            yield node

    @contains_seperator
    def iter_until(self, name: str) -> Generator[str]:
        for node in self.nodes:
            yield node
            if node == name:
                break

    @contains_seperator
    def iter_from(self, name: str) -> Generator[str]:
        path = NodePath(self.subpath(name, node_is="root"))
        for node in path.nodes:
            yield node


class NodePathCollection:
    paths: list[NodePath]

    def __init__(self, *paths: str | NodePath) -> None:
        self.paths = [NodePath(path) for path in paths]

    def __repr__(self) -> str:
        return f"<NodePathCollection: size {len(self.paths)}>"

    @classmethod
    def from_generator(cls, g: Generator) -> NodePathCollection:
        return cls(*[node["path"] for node in g])

    def ancestors_of(self, node: str) -> list[str]:
        ancestors = None
        for path in self._iter_path_with_node(node):
            if node == path.nodes[0]:
                return []
            subpath = path.rstrip_from(node, include_node=True)
            ancestors = NodePath(subpath).nodes
            break
        if ancestors is None:
            raise ValueError(f"No paths contains node name: {node}")
        return ancestors

    def descendants_of(self, node: str) -> list[str]:
        descendants = []
        for path in self._iter_path_with_node(node):
            if node == path.nodes[-1]:
                return []
            subpath = path.lstrip_until(node, include_node=True)
            [descendants.append(n) for n in NodePath(subpath).nodes if n not in descendants]
        return descendants

    def branch_of(self, node: str) -> list[str]:
        branch = []
        for path in self._iter_path_with_node(node):
            [branch.append(n) for n in path.nodes if n not in branch]
        return branch

    def _iter_path_with_node(self, node: str) -> Generator[NodePath]:
        for path in self.paths:
            if node not in path.nodes:
                continue
            yield path

    def unique_nodes(self) -> Generator[str]:
        uniq = set()
        for path in self.paths:
            for node in path.nodes:
                if node not in uniq:
                    yield node
                    uniq.add(node)

    def node_references(self) -> dict[str, list[str]]:
        references = defaultdict(list)
        for path in self.paths:
            for node in path.nodes:
                references[node].append(path.path)
        return references