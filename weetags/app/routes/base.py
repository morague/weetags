from __future__ import annotations

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json, text, empty

from typing import Any, Literal, get_args

from weetags.app.authentication.authentication import protected

from weetags.app.routes.middlewares import extract_params
from weetags.trees.tree import Tree
from weetags.exceptions import TreeDoesNotExist, UnknownRelation, OutputError



base = Blueprint("base")

# base.on_request(extract_params, priority=100)

@base.get("favicon.ico")
async def favicon(request: Request):
    return empty()

@base.route("/weetags/info", methods=["GET", "POST"])
async def info(request: Request):
    trees = request.app.ctx.trees
    return json({name:tree.info() for name,tree in trees.items()})

