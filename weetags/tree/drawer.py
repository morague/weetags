from __future__ import annotations 
from sqlalchemy import Table

from weetags.common import Engine
from weetags.common.types import DrawStyle

class UNode(list):
    name: str

    def __init__(self, iterable: list[UNode], name: str):
        super().__init__(iterable)
        self.name = name


class TreeDrawer:
    _engine: Engine
    name: str

    tree = Table
    _topology = Table
    _metadata = Table

    base: str
    style: DrawStyle = "ascii"

    STYLES = {
        "ascii": ("|", "|-- ", "+-- "),
        "ascii-ex": ("\u2502", "\u251c\u2500\u2500 ", "\u2514\u2500\u2500 "),
        "ascii-exr": ("\u2502", "\u251c\u2500\u2500 ", "\u2570\u2500\u2500 "),
        "ascii-em": ("\u2551", "\u2560\u2550\u2550 ", "\u255a\u2550\u2550 "),
        "ascii-emv": ("\u2551", "\u255f\u2500\u2500 ", "\u2559\u2500\u2500 "),
        "ascii-emh": ("\u2502", "\u255e\u2550\u2550 ", "\u2558\u2550\u2550 "),
    }

    IS = " "
    TS = " " * 3

    def __init__(self, name: str, engine: Engine) -> None:
        self.name = name
        self._engine = engine

        self._engine.get_tree(name)

    @property
    def trunc(self) -> str:
        return TreeDrawer.STYLES[self.style][0]

    @property
    def branch(self) -> str:
        return TreeDrawer.STYLES[self.style][1]

    @property
    def leaf(self) -> str:
        return TreeDrawer.STYLES[self.style][2]

    def draw(self, subtree: str | None = None, style: DrawStyle = "ascii-ex") -> str:
        self.style = style
        base = self._get_subtree_base(subtree)
        topology = self._engine.sub_tree_topology_from_name(base)

        d, visited = f"{self.base}\n", [self.base]
        
        for index, branch in enumerate(topology):
            branch = self._branch_lstrip(branch)
            for i, node in enumerate(branch.split(".")):
                if node in visited:
                    continue

                
                
                """
                skip trunc layer when next branch (same layer is)
                peeking in the future 1 time isn't sufficient to avoid unused truncs
                peeking n times in the future until:
                    same layer node is !=
                """

                
                d += self.draw_line(i, 0, node)
                visited.append(node)
        print(d)


    def draw_line(self, l: int, sl: int, name: str, is_leaf: bool = False) -> str:
        tree = self.branch
        if is_leaf:
            tree = self.leaf

        line = "{trunc}{edge}{name}\n"

        trunc = self.IS + (self.trunc + self.TS) * l
        edge = tree
        return line.format(
            trunc=trunc,
            edge=edge,
            name=name
        )

    def _branch_lstrip(self, branch: str) -> str:
        meet_base, b = False, []
        for node in branch.split("."):
            if meet_base:
                b.append(node)
            if node == self.base:
                meet_base = True
        return ".".join(b)


    def _get_subtree_base(self, subtree: str | None = None) -> str:
        if subtree is None:
            roots = self._engine._roots(self.name)
            if len(roots) == 0:
                raise ValueError(f"Tree {self.tree.name} has no root.")
            elif len(roots) > 1:
                raise ValueError(f"Tree {self.tree.name} has too many roots")
            base = roots[0]["name"]
        else:
            node = self._engine._node_from_name(subtree)
            if node is None:
                raise KeyError(f"Unknown node name: {subtree}")
            base = subtree
        self.base = base
        return base






# d = TreeDrawer("languages", Engine.from_uri(EngineURI(database="weetags.sqlite")))
# d.draw()