from __future__ import annotations

from collections import ChainMap
from sanic import Blueprint
from sanic.request import Request
from sanic.response import json, text, empty, JSONResponse
from sanic_ext import openapi


from typing import Any, Literal, get_args

from weetags.trees.tree import Tree
from weetags.exceptions import TreeDoesNotExist, UnknownRelation, OutputError
from weetags.app.routes.middlewares import extract_params

Node = dict[str, Any]
Relations = Literal["parent", "children", "siblings", "ancestors", "descendants"]

writer = Blueprint("writer", "/records")
writer.on_request(extract_params, priority=100)

@writer.route("add/node/<tree_name:str>/<nid:str>", methods=["POST"])
async def add_node(request: Request, tree_name:str, nid: str) -> JSONResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    params = request.ctx.params.get_kwargs(tree.add_node)
    if params.get("node",None) is None:
        raise ValueError("missing node payload")

    params.get("node").update({"id": nid})
    params.get("node").pop("nid", None)
    tree.add_node(**params)
    return json({"status": 200, "reasons": "OK", "data": {"added": nid}},status=200)

@writer.route("update/node/<tree_name:str>/<nid:str>", methods=["POST"])
async def update_node(request: Request, tree_name:str, nid: str):
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    params = request.ctx.params.get_kwargs(tree.update_node)

    if params.get("set_values",None) is None:
        raise ValueError("missing set_values payload")

    tree.update_node(nid, params.get("set_values"))
    return json({"status": 200, "reasons": "OK", "data": {"updated": nid}},status=200)

@writer.route("delete/node/<tree_name:str>/<nid:str>", methods=["GET", "POST"])
async def delete_node(request: Request, tree_name:str, nid: str):
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    params = request.ctx.params.get_kwargs(tree.delete_node)
    tree.delete_node(**params)
    return json({"status": 200, "reasons": "OK", "data": {"deleted": nid}},status=200)
