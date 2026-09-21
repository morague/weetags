from __future__ import annotations


from pydantic import Json
from sanic import Blueprint, HTTPResponse, Request, json, redirect, html
from sanic.response import JSONResponse, ResponseStream, HTTPResponse
from sanic_ext import render

from weetags.common.utils import OP
from weetags.tree.tree_engine import TreeEngine
from weetags.tree.tree import Tree
import weetags.server.arguments as arg
from weetags.server.utils import get_engine, generate_notification_payload
from weetags.server.authentication import Authenticator


"""
/auth
/login

/weetags
    /info
    /info/<tree_name:str>

    
/tree
    /info
    /nodes ( GET PATCH POST DELETE )
        /<node_name:str>  ( GET )
        /relation
        
        /move
        /prune
        /



        /traversal
        /distance
        /common-ancestor
        /search

    /permissions



args:
    tree_name (str)
    name (str)
    fields= tree.fields
    relations: (str) Relation
    conditions=
    limit (int)
    offset (int)
"""

weetagsbp = Blueprint("weetags", "/", version="v1")
auth = Blueprint("auth", "/", version="v1")
nodesbp = Blueprint("nodes", "/", version="v1")
utilsbp = Blueprint("utils", "/", version="v1")
explorerbp = Blueprint("explorer", "/", version="v1")

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

@nodesbp.route("trees/<tree_name:str>/search", methods=["GET"])
@arg.parser(arg.NodesArguments)
async def search(request: Request, arguments: arg.NodesArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    return json({"status_code": 200, "reasons": "OK", "data": []})

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
    if arguments.q is not None:
        data = engine.nodes_where(arguments.q, arguments.fields, arguments.page, arguments.page_size)
    else:
        data = engine._non_ordered_walk(arguments.fields, arguments.page, arguments.page_size)
    return json({"status_code": 200, "reasons": "OK", "data": data})

@nodesbp.route("trees/<tree_name:str>/nodes/<node_name:str>", methods=["GET"])
@arg.parser(arg.NodeArguments)
async def get_node(request: Request, arguments: arg.NodeArguments) -> JSONResponse:
    engine: TreeEngine = get_engine(request, arguments)
    node = engine._node(arguments.node_name, arguments.fields)
    return json({"status_code": 200, "reasons": "OK", "data": node})

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












@utilsbp.route("/trees/<tree_name:str>/infos", methods=["GET"])
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

@explorerbp.route("trees/<tree_name:str>/explorer", methods=["GET"])
@arg.parser(arg.ExplorerArguments)
async def explorer(request: Request, arguments: arg.ExplorerArguments) -> HTTPResponse:
    engine: TreeEngine = get_engine(request, arguments)

    context = {
        "tree": arguments.tree_name,
        "page": arguments.page,
        "page_size": arguments.page_size,
        "fields": engine._topology.columns.keys() + [f for f in engine._metadata.columns.keys() if f != "id"],
        "operators": list(OP.keys())
    }
    return await render("explorer.html", context= context)









@nodesbp.route("trees/<tree_name:str>/nodes/<node_name:str>", methods=["DELETE"])
async def delete_node(request: Request) -> JSONResponse:
    return json({"status_code": 200, "reasons": "OK", "data": ["hello"]})

@nodesbp.route("trees/<tree_name:str>/nodes/<node_name:str>", methods=["POST"])
async def add_node(request: Request) -> JSONResponse:
    return json({"status_code": 200, "reasons": "OK", "data": ["hello"]})

@nodesbp.route("trees/<tree_name:str>/nodes/<node_name:str>", methods=["PATCH"])
async def update_node(request: Request) -> JSONResponse:
    return json({"status_code": 200, "reasons": "OK", "data": ["hello"]})
