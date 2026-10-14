from __future__ import annotations

from itertools import zip_longest
from os import copy_file_range
from pathlib import Path
from random import choice
from string import hexdigits
from sanic import Request, Sanic

import weetags.common.exceptions as excp
from weetags.common.configs import TreeConfig
from weetags.tree.builder import TreeBuilder
from weetags.tree.tree_engine import TreeEngine
from weetags.server.arguments import ArgumentParser


def statefull_building(configs: TreeConfig, states: dict[str, int]) -> None:
    TreeBuilder.from_configs(configs)
    states[f"ready_{configs.name}"] = 1

def get_engine(request: Request, arguments: ArgumentParser) -> TreeEngine:
    engine: TreeEngine = request.app.ctx.entities.get(arguments.tree_name)
    if engine is None:
        raise excp.TreeEngineError(f"Unknown tree name: {tree_name}")
    return engine

def generate_notification_payload(message: str | None = None, level: str | None = None) -> list[dict]:
    payload = []
    if message is not None and level and level is not None:
        payload.append({"level": level, "message": message})
    return payload

def uid(size: int=8) -> str:
    return "".join([choice(hexdigits) for _ in range(size)])

class Motd:
    size: int = 120
    margin: int = 2

    def __init__(self) -> None:
        self.banner = self._banner()
        self.banner_line_max_len = max([len(l) for l in self.banner.split("\n")])


    @property
    def s(self) -> int:
        return self.banner_line_max_len + self.margin

    @property
    def outer(self) -> str:
        outer = f"+{'-' * (self.s + 1)}"

        diff = self.size - len(outer)
        outer += f"+{'-' * diff}+"
        return outer

    @property
    def empty_banner_space(self) -> str:
        return f"| {' ' * (self.banner_line_max_len + self.margin)}"

    def diff_margin(self, value: str) -> int:
        return self.banner_line_max_len - len(value) + self.margin

    def spaced_banner_line(self, value: str) -> str:
        return f"| {value}{self.diff_margin(value) * ' '}"

    def build(self, app: Sanic) -> str:
        motd = f"{self.outer}\n"

        deployed = self._formated_trees(app)
        banner_lines = ["empty"] + self.banner.split("\n") + ["empty"]
        lines = zip_longest(deployed, banner_lines, fillvalue=None)

        for tree, banner in lines:
            b = self.banner_side(banner)
            t = self.tree_side(tree)
            motd += b + t

        motd += f"{self.outer}\n"
        return motd

    def _banner(self) -> str:
        banner = Path("weetags/server/BANNER").read_text()
        version = Path("weetags/VERSION").read_text()
        return f"{banner}v{str(version)}"

    def _formated_trees(self, app: Sanic) -> list[str]:
        repr = []

        
        for name, configs in app.config.TREES_CONFIGS.items():
            c = [
                f"\t* {name}",
                f"\t\t* type: {configs.type}",
                f"\t\t* engine: {configs.uri}",
                f"\t\t* cache: {configs.cache.cache}",
                f"\t\t* serving at: `/v1/trees/{name}`",
                f"+{'-' * (self.size - self.banner_line_max_len - 4)}+"
            ]
            repr.extend(c)

        return repr[:-1]

    def banner_side(self, banner: str | None) -> str:
        match banner:
            case "empty" | None:
                b = self.empty_banner_space
            case _:
                b = self.spaced_banner_line(banner)
        return b

    def tree_side(self, tree: str | None) -> str:
        if tree is None:
            diff = self.size - self.banner_line_max_len - 5
            t = f"| {' ' * diff}|\n"
        elif tree.startswith("+"):
            t = tree + "\n"
        else:
            n_tabs = sum([1 for c in tree if c == "\t"])
            if n_tabs == 1:
                diff = (self.size - self.banner_line_max_len) - len(tree) - (n_tabs * 6) + 1
            else:
                diff = (self.size - self.banner_line_max_len) - len(tree) - (n_tabs * 6)
            t = f"| {tree}{diff * ' '}|\n"
        return t
