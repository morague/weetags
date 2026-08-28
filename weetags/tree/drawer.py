from __future__ import annotations 
from typing import Generator

from weetags.common.path_utils import NodePath
from weetags.common import BoundEngine
from weetags.common.types import DrawStyle

class Unode:
    name: str
    children: list[Unode]
    layer: int
    is_leaf: bool

    def __init__(self, name: str, layer: int, children: list[Unode]):
        self.name = name
        self.layer = layer
        self.children = children
        self.is_leaf = False

    def __repr__(self) -> str:
        return f"<Unode(name: {self.name}, layer: {self.layer}, leaf: {self.is_leaf})>"

    def iter(self) -> Generator[Unode]:
        for index, node in enumerate(self.children):
            if index == len(self.children) - 1:
                node.is_leaf = True
            yield node
            yield from node.iter()


class LinkedTree:
    tree: list[Unode]
    nodes: dict[str, Unode]

    def __init__(self) -> None:
        self.tree = []
        self.nodes = {}

    @property
    def base(self) -> Unode:
        node = self.tree[0]
        node.is_leaf = True
        return node

    @classmethod
    def from_topology(cls, topology: list[str], root: str) -> LinkedTree:
        tree = cls()
        for branch in topology:
            base, path = None,  NodePath(branch)
            subtree_path = NodePath(path.lstrip_until(root))
            for index, node in enumerate(subtree_path.nodes):
                unode = tree.nodes.get(node, None)
                if tree.nodes.get(node, None) is None:
                    unode = Unode(node, index, [])
                    tree.nodes.update({node:unode})

                if base is not None and unode not in base.children:
                    assert unode is not None
                    base.children.append(unode)
                elif base is None and unode not in tree.tree:
                    assert unode is not None
                    tree.tree.append(unode)
                base = unode
        return tree

    def iter(self) -> Generator[Unode]:
        for index, node in enumerate(self.tree):
            if index == len(self.tree) - 1:
                node.is_leaf = True
            yield node
            yield from node.iter()

class TreeDrawer:
    _engine: BoundEngine

    base: str
    style: DrawStyle = "ascii"
    extra_space: bool = False

    INITIAL_SPACE: int = 1
    INTERNAL_SPACE: int = 3
    STYLES = {
        "ascii": ("|", "|-- ", "+-- "),
        "ascii-ex": ("\u2502", "\u251c\u2500\u2500 ", "\u2514\u2500\u2500 "),
        "ascii-exr": ("\u2502", "\u251c\u2500\u2500 ", "\u2570\u2500\u2500 "),
        "ascii-em": ("\u2551", "\u2560\u2550\u2550 ", "\u255a\u2550\u2550 "),
        "ascii-emv": ("\u2551", "\u255f\u2500\u2500 ", "\u2559\u2500\u2500 "),
        "ascii-emh": ("\u2502", "\u255e\u2550\u2550 ", "\u2558\u2550\u2550 "),
    }

    def __init__(self, engine: BoundEngine) -> None:
        self._engine = engine

    @property
    def trunc(self) -> str:
        return TreeDrawer.STYLES[self.style][0]

    @property
    def branch(self) -> str:
        return TreeDrawer.STYLES[self.style][1]

    @property
    def leaf(self) -> str:
        return TreeDrawer.STYLES[self.style][2]

    def draw(self, subtree: str | None = None, style: DrawStyle = "ascii-ex", extra_spacing: bool = False) -> Generator[str]:
        self.style = style
        self.extra_space = extra_spacing
        base = self._get_subtree_base(subtree)
        topology = self._engine.subtree_topology(base)

        linkedtree = LinkedTree.from_topology(topology, base)

        root, layers = linkedtree.base, {}
        yield f"{root.name}"
        for node in root.iter():
            yield self.draw_line(node, layers)
            layers.update({node.layer:node.is_leaf})

    def draw_line(self, node: Unode, layers: dict[int, bool]) -> str:
        line = self.INITIAL_SPACE * " "
        for layer in range(0, node.layer):
            layer += 1
            if layer == node.layer and node.is_leaf:
                line += f"{self.leaf}{node.name}" 
            elif layer == node.layer and node.is_leaf is False:
                line += f"{self.branch}{node.name}" 
            elif layers[layer] is False:
                spacing = self.INTERNAL_SPACE * " "
                line += f"{self.trunc}{spacing}"
            elif layers[layer]:
                spacing = self.INTERNAL_SPACE * " "
                line += f" {spacing}"
        return line

    def _get_subtree_base(self, subtree: str | None = None) -> str:
        if subtree is None:
            roots = self._engine.roots(self._engine.name)
            if len(roots) == 0:
                raise ValueError(f"Tree {self._engine.tree.name} has no root.")
            elif len(roots) > 1:
                raise ValueError(f"Tree {self._engine.tree.name} has too many roots")
            base = roots[0]["name"]
        else:
            self._engine._node_or_raise(subtree)
            base = subtree
        self.base = base
        return base

