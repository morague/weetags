from __future__ import annotations

import os
import time
from multiprocessing import Manager
from sanic import Sanic

from weetags.common.serializer import HttpSerializer
import weetags.server.utils as sutils
from weetags.tree.tree_engine import TreeEngine
from weetags.tree.tree import Tree

async def states(app: Sanic):
    app.shared_ctx.states = Manager().dict()

async def build(app: Sanic) -> None:
    trees_configs = app.config.TREES_CONFIGS
    for name, configs in trees_configs.items():
        if app.shared_ctx.states.get(f"builder_lock_{name}", 0) == 0:
            print(os.getpid(), name)
            app.shared_ctx.states[f"builder_lock_{name}"] = 1
            app.shared_ctx.states[f"building_{name}"] = 1
            app.shared_ctx.states[f"ready_{name}"] = 0
            app.m.manage(
                f"treebuilder_{name}", 
                sutils.statefull_building, 
                {"configs": configs, "states": app.shared_ctx.states}
            )

async def load_trees(app: Sanic) -> None:
    trees_configs = app.config.TREES_CONFIGS

    app.ctx.entities = {}
    loaded = all([name in app.ctx.entities.keys() for name in trees_configs.keys()])
    while loaded is not True:
        for name, configs in trees_configs.items():
            ready = app.shared_ctx.states.get(f"ready_{name}", 0)
            if ready == 0:
                time.sleep(1)
            else:
                engine = TreeEngine.from_configs(configs.tree_inline)
                engine.set_serializer(HttpSerializer())
                app.ctx.entities[name] = engine 
        loaded = all([name in app.ctx.entities.keys() for name in trees_configs.keys()])