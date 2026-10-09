from __future__ import annotations

import json as serde
from sanic_ext import render
from sanic import Blueprint, HTTPResponse, Request, json, redirect, empty
from sanic.response import JSONResponse, HTTPResponse

from typing import get_args

from weetags.common.types import BaseRelations
from weetags.common.utils import OP, SQLF
from weetags.tree.tree_engine import TreeEngine
from weetags.tree.tree import Tree
import weetags.server.arguments as arg
from weetags.server.utils import get_engine, generate_notification_payload, uid
from weetags.server.authentication import Authenticator

basebp = Blueprint("base", "/")
weetagsbp = Blueprint("weetags", "/", version="v1")
auth = Blueprint("auth", "/", version="v1")
nodesbp = Blueprint("nodes", "/", version="v1")
utilsbp = Blueprint("utils", "/", version="v1")
explorerbp = Blueprint("explorer", "/", version="v1")

topologybp = Blueprint("topology", "/", version="v1")
metadatabp = Blueprint("metadata", "/", version="v1")
alterationbp = Blueprint("alteration", "/", version="v1")

@basebp.route("/favicon.ico", methods=["GET"])
async def favicon(request: Request) -> HTTPResponse:
    return empty()

@auth.route("/auth", methods=["POST"])
@arg.parser(arg.AuthArguments)
async def authenticate(request: Request, arguments: arg.AuthArguments) -> HTTPResponse:
    auth: Authenticator | None = getattr(request.app.ctx, "auth", None)
    if auth is None:
        raise ValueError("Authentication is not set.")

    try:
        token = auth.authenticate(arguments.username, arguments.password)
    except Exception as e:
        if arguments.redirect:
            return redirect(f"/v1/login?message={e}&level=error")
        else:
            raise e

    data = {"token": token, "max_age": auth.MAX_AGE, "cookie": arguments.set_cookie}
    response =  json({"status_code": 200, "reasons": "OK", "data": data})

    if arguments.set_cookie:
        response.add_cookie(
            "Authorization",
            f"Bearer {token}",
            httponly=True,
            samesite="Strict",
            max_age=auth.MAX_AGE,
        )
    return response

@auth.route("login", methods=["GET"])
@arg.parser(arg.LoginArguments)
async def login(request: Request, arguments: arg.LoginArguments) -> HTTPResponse:
    notif = generate_notification_payload(arguments.message, arguments.level)
    return await render("login.html", context={"notifications": notif})

@nodesbp.route("trees/<tree_name:str>/root", methods=["GET"])
@arg.parser(arg.RootArguments)
async def root(request: Request, arguments: arg.RootArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    root = engine.root(arguments.fields)
    return json({"status_code": 200, "reasons": "OK", "data": root})

@nodesbp.route("trees/<tree_name:str>/nodes", methods=["GET", "POST"])
@arg.parser(arg.NodesArguments)
async def nodes(request: Request, arguments: arg.NodesArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    if any([arguments.q, arguments.r]):
        data = engine.nodes_where(arguments.r, arguments.q, arguments.fields, arguments.page, arguments.page_size)
    else:
        data = engine._non_ordered_walk(arguments.fields, arguments.page, arguments.page_size)
    return json({"status_code": 200, "reasons": "OK", "data": data})

@nodesbp.route("trees/<tree_name:str>/nodes/<node_name:str>", methods=["GET"])
@arg.parser(arg.NodeArguments)
async def get_node(request: Request, arguments: arg.NodeArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    node = engine._node(arguments.node_name, arguments.fields)
    return json({"status_code": 200, "reasons": "OK", "data": node})

@nodesbp.route("trees/<tree_name:str>/nodes/<node_name:str>/closest", methods=["GET"])
@arg.parser(arg.ClosestNodesArguments)
async def closest(request: Request, arguments: arg.ClosestNodesArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    nodes = engine.closest(arguments.node_name, arguments.r, arguments.q, arguments.fields)
    return json({"status_code": 200, "reasons": "OK", "data": nodes})

@nodesbp.route("trees/<tree_name:str>/nodes/<node_name:str>/<relation:(parent|children|branch)>", methods=["GET"])
@arg.parser(arg.RelationArguments)
async def get_node_relation1(request: Request, arguments: arg.RelationArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    match arguments.relation:
        case "parent":
            data = engine.parent_node(arguments.node_name, arguments.fields)
        case "children":
            data = engine.children_nodes(arguments.node_name, arguments.fields)
        case "branch":
            data = engine.branch_nodes(arguments.node_name, arguments.fields)
        case _:
            raise ValueError(f"Unknown relation: {arguments.relation}")
    return json({"status_code": 200, "reasons": "OK", "data": data})

@nodesbp.route("trees/<tree_name:str>/nodes/<node_name:str>/<relation:(sibling|ancestors|descendants)>", methods=["GET"])
@arg.parser(arg.Relation1Arguments)
async def get_node_relation(request: Request, arguments: arg.Relation1Arguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    match arguments.relation:
        case "siblings":
            data = engine.sibling_nodes(arguments.node_name, arguments.fields, arguments.include_self)
        case "ancestors":
            data = engine.ancestor_nodes(arguments.node_name, arguments.fields, arguments.include_self)
        case "descendants":
            data = engine.descendant_nodes(arguments.node_name, arguments.fields, arguments.include_self)
        case _:
            raise ValueError(f"Unknown relation: {arguments.relation}")
    return json({"status_code": 200, "reasons": "OK", "data": data})

@utilsbp.route("/trees", methods=["GET"])
async def infos(request: Request) -> JSONResponse:
    engines: dict[str, TreeEngine] = request.app.ctx.entities
    data = {k:Tree(k, engine).infos for k,engine in engines.items()}
    return json({"status_code": 200, "reasons": "OK", "data": data})

@utilsbp.route("/trees/<tree_name:str>", methods=["GET"])
@arg.parser(arg.BaseTreeArguments)
async def get_infos(request: Request, arguments: arg.BaseTreeArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    tree = Tree(arguments.tree_name, engine)
    return json({"status_code": 200, "reasons": "OK", "data": tree.infos})

@utilsbp.route("trees/<tree_name:str>/distance", methods=["GET"])
@arg.parser(arg.CmpUtilsArguments)
async def node_distance(request: Request, arguments: arg.CmpUtilsArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    distance = engine.distance(arguments.node1, arguments.node2)
    return json({"status_code": 200, "reasons": "OK", "data": distance})

@utilsbp.route("trees/<tree_name:str>/closest-common-ancestor", methods=["GET"])
@arg.parser(arg.CmpUtilsArguments)
async def closest_ancestor(request: Request, arguments: arg.CmpUtilsArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    common = engine.lowest_common_ancestor(arguments.node1, arguments.node2, arguments.fields)
    return json({"status_code": 200, "reasons": "OK", "data": common})

@utilsbp.route("trees/<tree_name:str>/draw", methods=["GET"])
@arg.parser(arg.DrawArguments)
async def draw(request: Request, arguments: arg.DrawArguments):
    engine: TreeEngine = get_engine(request, arguments)
    tree = Tree(arguments.tree_name, engine)
    kwargs = arguments.get_kwargs(tree._draw)

    response = await request.respond()
    assert response is not None
    for line in tree._draw(**kwargs):
        await response.send(line + "\n")

@utilsbp.route("trees/<tree_name:str>/fields", methods=["GET"])
@arg.parser(arg.Fieldrguments)
async def fields(request: Request, arguments: arg.Fieldrguments):
    distinct = ""
    if arguments.distinct:
        distinct = "?distinct"
    return redirect(f"fields/{arguments.field_name}{distinct}")

@utilsbp.route("trees/<tree_name:str>/fields/<field_name:str>", methods=["GET"])
@arg.parser(arg.Fieldrguments)
async def get_field(request: Request, arguments: arg.Fieldrguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    data = engine._field(arguments.field_name, arguments.distinct)
    return json({"status_code": 200, "reasons": "OK", "data": data})

@utilsbp.get("trees/<tree_name:str>/export")
@arg.parser(arg.ExportArguments)
async def export(request: Request, arguments: arg.ExportArguments):
    engine: TreeEngine = get_engine(request, arguments)
    
    response = await request.respond(
        headers={
            "Content-Disposition": f'Attachment; filename="{arguments.tree_name}_{arguments.subtree or "root"}_{uid(8)}.jl"',
            "Content-Type": "application/jsonlines",
        },
        content_type="application/jsonlines"
    )
    assert response is not None

    for node in engine.traversal(arguments.subtree, "pre"):
        data = f"{serde.dumps(node)}\n"
        await response.send(data)
        
@explorerbp.route("trees/<tree_name:str>/explorer", methods=["GET"])
@arg.parser(arg.ExplorerArguments)
async def explorer(request: Request, arguments: arg.ExplorerArguments) -> HTTPResponse:
    engine: TreeEngine = get_engine(request, arguments)

    context = {
        "tree": arguments.tree_name,
        "page": arguments.page,
        "page_size": arguments.page_size,
        "fields": engine._topology.columns.keys() + [f for f in engine._metadata.columns.keys() if f != "id"],
        "operators": list(OP.keys()),
        "sql_functions": list(SQLF.keys()),
        "relations": get_args(BaseRelations.__value__)
    }
    return await render("explorer.html", context= context)


@topologybp.route("trees/<tree_name:str>/nodes/<node_name:str>", methods=["PUT"])
@arg.parser(arg.AddNodeArguments)
async def add(request: Request, arguments: arg.AddNodeArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.add_node(arguments.node_name, arguments.parent, arguments.metadata)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@topologybp.route("trees/<tree_name:str>/graft", methods=["PUT"])
@arg.parser(arg.AddNodesArguments)
async def graft(request: Request, arguments: arg.AddNodesArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.graft_nodes(arguments.nodes, arguments.keymap, arguments.on_collision)
    return json({"status_code": 200, "reasons": "OK", "data": []})

# @topologybp.route("trees/<tree_name:str>/import", methods=["POST"])
# @arg.parser(arg.AddNodesArguments)
# async def import_from_file(request: Request, arguments: arg.AddNodesArguments) -> JSONResponse:
#     engine: TreeEngine = get_engine(request, arguments)
#     engine.import_from_file(arguments.keymap, arguments.on_collision)
#     return json({"status_code": 200, "reasons": "OK", "data": []})

@topologybp.route("trees/<tree_name:str>/nodes", methods=["DELETE"])
@arg.parser(arg.RemoveNodesArguments)
async def remove_where(request: Request, arguments: arg.RemoveNodesArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.remove_nodes_where(arguments.r, arguments.q, arguments.force)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@topologybp.route("trees/<tree_name:str>/nodes/<node_name:str>", methods=["DELETE"])
@arg.parser(arg.RemoveNodeArguments)
async def remove(request: Request, arguments: arg.RemoveNodeArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.remove_node(arguments.node_name, arguments.force)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@topologybp.route("trees/<tree_name:str>/prune/<subtree:str>", methods=["DELETE"])
@arg.parser(arg.PruneArguments)
async def prune(request: Request, arguments: arg.PruneArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.prune_subtree(arguments.subtree)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@metadatabp.route("trees/<tree_name:str>/nodes", methods=["PATCH"])
@arg.parser(arg.UpdateNodesArguments)
async def update(request: Request, arguments: arg.UpdateNodesArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.update_nodes_where(arguments.values, arguments.r, arguments.q)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@metadatabp.route("trees/<tree_name:str>/nodes/<node_name:str>", methods=["PATCH"])
@arg.parser(arg.UpdateNodeArguments)
async def update_where(request: Request, arguments: arg.UpdateNodeArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.update_node(arguments.node_name, arguments.values)
    return json({"status_code": 200, "reasons": "OK", "data": []})


@metadatabp.route("trees/<tree_name:str>/nodes/<node_name:str>/appendList", methods=["PATCH"])
@arg.parser(arg.AppendListArguments)
async def append_list(request: Request, arguments: arg.AppendListArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.append_list(arguments.node_name, arguments.key, arguments.value)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@metadatabp.route("trees/<tree_name:str>/nodes/<node_name:str>/extendList", methods=["PATCH"])
@arg.parser(arg.ExtendListArguments)
async def extend_list(request: Request, arguments: arg.ExtendListArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.extend_list(arguments.node_name, arguments.key, arguments.value)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@metadatabp.route("trees/<tree_name:str>/nodes/<node_name:str>/popList", methods=["PATCH"])
@arg.parser(arg.PopListArguments)
async def pop_list(request: Request, arguments: arg.PopListArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.pop_list(arguments.node_name, arguments.key, arguments.index)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@metadatabp.route("trees/<tree_name:str>/nodes/<node_name:str>/setObjectKey", methods=["PATCH"])
@arg.parser(arg.SetObjectKeyArguments)
async def set_object_key(request: Request, arguments: arg.SetObjectKeyArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.set_object_key(arguments.node_name, arguments.path, arguments.value)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@metadatabp.route("trees/<tree_name:str>/nodes/<node_name:str>/popObjectKey", methods=["PATCH"])
@arg.parser(arg.PopObjectKeyArguments)
async def pop_object_key(request: Request, arguments: arg.PopObjectKeyArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.pop_object_key(arguments.node_name, arguments.path)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@metadatabp.route("trees/<tree_name:str>/nodes/<node_name:str>/objectAppendList", methods=["PATCH"])
@arg.parser(arg.SetObjectKeyArguments)
async def append_object_list(request: Request, arguments: arg.SetObjectKeyArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.object_append_list(arguments.node_name, arguments.path, arguments.value)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@metadatabp.route("trees/<tree_name:str>/nodes/<node_name:str>/objectExtendList", methods=["PATCH"])
@arg.parser(arg.ExtendObjectKeyArguments)
async def extend_object_list(request: Request, arguments: arg.ExtendObjectKeyArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.object_extend_list(arguments.node_name, arguments.path, arguments.value)
    return json({"status_code": 200, "reasons": "OK", "data": []})

@metadatabp.route("trees/<tree_name:str>/nodes/<node_name:str>/objectPopList", methods=["PATCH"])
@arg.parser(arg.PopListObjectArguments)
async def pop_object_list(request: Request, arguments: arg.PopListObjectArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    engine.object_pop_list(arguments.node_name, arguments.path, arguments.index)
    return json({"status_code": 200, "reasons": "OK", "data": []})