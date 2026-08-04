from __future__ import annotations

from typing import Generator

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
    def __init__(self, path: str) -> None:
        self.path = path
        self.nodes = path.split(".")
        self.len = len(self.nodes)

    def __repr__(self) -> str:
        return f"<NodePath: {self.path}>"

    def contains_node(self, node: str) -> bool:
        return node in self.nodes

    def contains_subpath(self, path: str) -> bool:
        return path in self.path

    def distance(self, name: str, other: str) -> int:
        print(self.nodes)
        name_pos = self.nodes.index(name)
        other_pos = self.nodes.index(other)
        return abs(name_pos - other_pos)

    @validate_fix
    def suffix(self, size: int, /) -> str:  
        index = len(self.nodes) - size
        return ".".join(self.nodes[index:])

    @validate_fix
    def prefix(self, size: int, /) -> str:
        return ".".join(self.nodes[:size])

    @contains_seperator
    def lstrip_from_node(self, seperator: str, /, include_seperator: bool = False) -> str:
        index = self.nodes.index(seperator)
        if include_seperator is False:
            index += 1
        if index == len(self.nodes):
            raise ValueError("last node from a path has to be included")
        return ".".join(self.nodes[index:])

    @contains_seperator
    def rstrip_from_node(self, seperator: str, /, include_seperator: bool = False) -> str:
        index = self.nodes.index(seperator)
        if include_seperator:
            index += 1
        if index == 0:
            raise ValueError("first node from a path has to be included")
        return ".".join(self.nodes[:index])

    def prefix_from_node(self, name: str, size: int, include_seperator: bool = True) -> str:
        striped_path = NodePath(self.rstrip_from_node(name, include_seperator))
        return striped_path.prefix(size)

    def suffix_from_node(self, name: str, size: int, include_seperator: bool = True) -> str:
        striped_path = NodePath(self.lstrip_from_node(name, include_seperator))
        print(striped_path)
        return striped_path.suffix(size)

    def get_node_before(self, name: str, dist: int) -> str | None:
        index = self.nodes.index(name)
        index = index - dist
        if index < 0:
            return None
        return self.nodes[index]

    def get_node_after(self, name: str, dist: int) -> str | None:
        index = self.nodes.index(name)
        index = index + dist
        if index >= self.len:
            return None
        return self.nodes[index]

    def iter(self) -> Generator[str]:
        for node in self.nodes:
            yield node

    @contains_seperator
    def iter_until(self, name: str) -> Generator[str]:
        for node in self.nodes:
            if node == name:
                break
            yield node

    @contains_seperator
    def iter_after(self, name: str) -> Generator[str]:
        path = NodePath(self.lstrip_from_node(name))
        for node in path.nodes:
            yield node