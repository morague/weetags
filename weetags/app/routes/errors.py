import logging
from time import perf_counter
from sanic import json
from sanic.request import Request
from sanic.log import logger

import traceback
from weetags.exceptions import WeetagsException

logger = logging.getLogger("endpointAccess")

async def error_handler(request: Request, exception: Exception):
    perf = round(perf_counter() - request.ctx.t, 5)
    status = getattr(exception, "status", 500)
    logger.error(f"[{request.host}] > {request.method} {request.url} : {str(exception)} [{str(status)}][{str(len(str(exception)))}b][{perf}s]")
    if not isinstance(exception.__class__.__base__, WeetagsException):
        # log traceback of non handled errors
        logger.error(traceback.format_exc())
    return json({"status": status, "reasons": str(exception)}, status=status)
