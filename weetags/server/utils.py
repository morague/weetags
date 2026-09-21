from __future__ import annotations

from os import read
from typing import Any
from attrs import define, field
from sanic import Request

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
        raise KeyError(f"Unknown tree name: {tree_name}")
    return engine

def generate_notification_payload(message: str | None = None, level: str | None = None) -> list[dict]:
    payload = []
    if message is not None and level and level is not None:
        payload.append({"level": level, "message": message})
    return payload