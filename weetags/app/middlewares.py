import logging
import traceback
from time import perf_counter
from collections import ChainMap
from urllib.parse import unquote
from sanic.request import Request
from sanic.response import HTTPResponse, json

from weetags.exceptions import WeetagsException
from weetags.app.params_handler import ParamParser


logger = logging.getLogger("endpointAccess")

async def log_entry(request: Request) -> None:
    request.ctx.t = perf_counter()

async def log_exit(request: Request, response: HTTPResponse) -> None:
    perf = round(perf_counter() - request.ctx.t, 5)
    if response.status == 200:
        logger.info(f"[{request.host}] > {request.method} {request.url} [{str(response.status)}][{str(len(response.body))}b][{perf}s]")

async def extract_params(request: Request) -> None:
    nid = {k:unquote(v) for k,v in request.match_info.items() if k in ["nid", "nid0", "nid1"]} or {}
    query_args = {k:(v[0] if len(v) == 1 else v) for k,v in request.args.items()}
    payload = request.load_json() or {}
    params = dict(ChainMap(nid, payload, query_args))
    request.ctx.params = ParamParser(**params)
    print(request.ctx.params)

async def cookie_token(request: Request) -> None:
    cookie = request.cookies.get("Authorization", None)
    if cookie is not None:
        request.headers.add("Authorization", cookie)

async def error_handler(request: Request, exception: Exception):
    perf = round(perf_counter() - request.ctx.t, 5)
    status = getattr(exception, "status", 500)
    logger.error(f"[{request.host}] > {request.method} {request.url} : {str(exception)} [{str(status)}][{str(len(str(exception)))}b][{perf}s]")
    if not isinstance(exception.__class__.__base__, WeetagsException):
        # log traceback of non handled errors
        logger.error(traceback.format_exc())
    return json({"status": status, "reasons": str(exception)}, status=status)
