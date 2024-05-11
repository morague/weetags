import logging
from time import perf_counter
from sanic import text
from sanic.request import Request
from sanic.log import logger



logger = logging.getLogger("endpointAccess")

async def error_handler(request: Request, exception: Exception):
    perf = round(perf_counter() - request.ctx.t, 5)
    status = getattr(exception, "status", 500)
    logger.error(f"[{request.host}] > {request.method} {request.url} : {str(exception)} [{str(status)}][{str(len(str(exception)))}b][{perf}s]")
    return text(str(exception), status=exception.status)
