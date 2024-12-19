from __future__ import annotations

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json, text, empty

from typing import Any, Literal, get_args

from weetags.app.authentication.authentication import protected

from weetags.app.routes.middlewares import extract_params
from weetags.tree import Tree
from weetags.exceptions import TreeDoesNotExist, UnknownRelation, OutputError


base = Blueprint("base")

@base.get("favicon.ico")
async def favicon(request: Request):
    return empty()

@base.route("/weetags/info", methods=["GET", "POST"])
async def info(request: Request):
    trees = request.app.ctx.trees
    return json({"status": 200, "reasons": "OK", "data": {name:tree.info for name,tree in trees.items()}})
