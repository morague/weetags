from __future__ import annotations

from sanic import Blueprint
from sanic.request import Request
from sanic.response import text, HTTPResponse

from typing import Any, Literal

from weetags.app.authentication.authentication import protected

from weetags.app.routes.middlewares import extract_params
from weetags.trees.tree import Tree
from weetags.exceptions import TreeDoesNotExist


Node = dict[str, Any]
Relations = Literal["parent", "children", "siblings", "ancestors", "descendants"]


shower = Blueprint("show", "/show")

shower.on_request(extract_params, priority=100)

@shower.route("/<tree_name:str>", methods=["GET", "POST"])
@protected
async def show(request: Request, tree_name: str) -> HTTPResponse:
    tree: Tree = request.app.ctx.trees.get(tree_name, None)
    if tree is None:
        raise TreeDoesNotExist(tree_name, list(request.app.ctx.trees.keys()))

    params = request.ctx.params.get_kwargs(tree.draw_tree)
    return text(tree.draw_tree(**params))
