from __future__ import annotations

import jwt
import time
from pathlib import Path
from hashlib import sha256
from sanic.request import Request

from typing import Any, Optional

from weetags.engine.engine import TreeEngine
from weetags.engine.schema import (
    User,
    Restriction,
    UsersTable, 
    RestrictionsTable
)

from weetags.exceptions import (
    OutatedAuthorizationToken,
    AuthorizationTokenRequired,
    InvalidToken,
    MissingLogin,
    InvalidLogin,
    AccessDenied
)

StrOrPath = str | Path
Users = Restricions = list[dict[str, Any]] | None

from functools import wraps

def protected(f):
    @wraps(f)
    async def wrapped(request: Request, *args, **kwargs):
        authenticator: Authenticator = request.app.ctx.authenticator
        if authenticator is None:
            response = await f(request, *args, **kwargs)
            return response

        elif authenticator.authorize(request):
            response = await f(request, *args, **kwargs)
            return response
        else:
            raise AccessDenied()
    return wrapped

class Authenticator(TreeEngine):
    def __init__(self, database: StrOrPath = ":memory:") -> None:
        super().__init__("", database)

    @classmethod
    def initialize(
        cls,
        users: Optional[Users] = None,
        restrictions: Optional[Users] = None,
        database: Optional[StrOrPath] = ":memory:",
        replace: bool = False
    ) -> Authenticator:
        authenticator = cls(database)

        tables = authenticator._get_tables("weetags")
        if ((len(tables) == 0 or replace) and users is None):
            raise ValueError("you must define users and restrictions in your configurations")

        if len(tables) > 0 and replace:
            authenticator._drop("weetags__restrictions")
            authenticator._drop("weetags__users")

        if len(tables) == 0 or replace:
            authenticator._create_tables(UsersTable(), RestrictionsTable())

        if users is None and replace:
            raise ValueError("Set some users or remove the authenticator from the configs")

        if users is not None:
            parsed_users = [User(**settings) for settings in users]
            authenticator._add_users(*parsed_users)
        if restrictions is not None:
            parsed_restrictions = [Restriction(**settings) for settings in restrictions]
            authenticator._add_restrictions(*parsed_restrictions)

        return authenticator


    def authenticate(self, request: Request, username: str, password: str) -> str:
        user = self._get_user(username)
        if user is None:
            raise InvalidLogin()

        max_age = user.get("max_age")
        auth_level = user.get("auth_level")
        salted_password= user["salt"] + password
        password_sha256 = sha256(salted_password.encode()).hexdigest()

        if password_sha256 != user["password_sha256"]:
            raise InvalidLogin()

        return jwt.encode({"auth_level": auth_level, "max_age": self._max_time_age(max_age)}, request.app.config.SECRET)

    def authorize(self, request: Request) -> bool:
        token = request.token
        route = request.route
        tree = request.match_info.get("tree_name")

        if token is None:
            raise AuthorizationTokenRequired()

        if tree is None:
            raise ValueError("tree name not available")

        if route is None:
            raise ValueError("Route name not available")

        try:
            payload = jwt.decode(
                token, request.app.config.SECRET, algorithms=["HS256"]
            )
        except jwt.exceptions.InvalidTokenError:
            raise InvalidToken()

        _, blueprint, _ = route.name.split('.')
        restriction = self._get_restriction(tree, blueprint)

        if restriction is not None and not any([level in payload["auth_level"] for level in restriction["auth_level"]]):
            raise AccessDenied()

        if int(time.time()) > payload["max_age"]:
            raise OutatedAuthorizationToken()
        return True
            
    def _max_time_age(self, max_age: int) -> int:
        return int(time.time()) + max_age


