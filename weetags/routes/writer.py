from __future__ import annotations

from collections import ChainMap
from sanic import Blueprint
from sanic.request import Request
from sanic.response import json, text, empty
from sanic_ext import openapi


from typing import Any, Literal, get_args

from weetags.params_handler import ParamsHandler
from weetags.trees.permanent_tree import PermanentTree
from weetags.exceptions import TreeDoesNotExist, UnknownRelation, OutputError

Node = dict[str, Any]
Relations = Literal["parent", "children", "siblings", "ancestors", "descendants"]

writer = Blueprint("writer")


@writer.route("add/node/<tree_name:str>/<nid:str>", methods=["POST"])
async def add_node(request: Request, tree_name:str, nid: str):
    tree: PermanentTree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    payload = request.load_json()
    payload.update({"id": nid})
    tree.add_node(payload)
    return empty()

@writer.route("update/node/<tree_name:str>/<nid:str>", methods=["POST"])
async def update_node(request: Request, tree_name:str, nid: str):
    tree: PermanentTree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    payload = request.load_json()
    tree.update_node(nid, payload)
    return empty()

@writer.route("delete/node/<tree_name:str>/<nid:str>", methods=["DELETE"])
async def delete_node(request: Request, tree_name:str, nid: str):
    tree: PermanentTree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))
    tree.delete_node(nid)
    return empty()


@writer.route("add/user/<user:str>", methods=["POST"])
async def add_node(request: Request, user: str):
    pass

@writer.route("update/user/<user:str>", methods=["POST"])
async def update_node(request: Request, user: str):
    pass

@writer.route("delete/user/<user:str>", methods=["DELETE"])
async def delete_node(request: Request, user: str):
    pass