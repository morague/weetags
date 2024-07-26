import jwt
import time
from hashlib import sha256
from functools import wraps

from sanic import text
from sanic.request import Request
from sanic_routing import Route

from typing import Optional, Any

from weetags.app.authentication.schema import UsersTable, RestrictionsTable
from weetags.app.utils import generate_uuid
from weetags.database.db import _Db
from weetags.exceptions import (
    OutatedAuthorizationToken, 
    AuthorizationTokenRequired, 
    InvalidToken, 
    MissingLogin, 
    InvalidLogin, 
    AccessDenied
)

def protected(f):
    @wraps(f)
    async def wrapped(request: Request, *args, **kwargs):
        authenticator: Authenticator = request.app.ctx.authenticator
        if authenticator is None:
            response = await f(request, *args, **kwargs)
            return response
        
        tree_name = request.match_info.get("tree_name")
        restriction = authenticator.has_restriction(request.route, tree_name)
        
        if restriction is None:
            response = await f(request, *args, **kwargs)
            return response
        
        if authenticator.authorize(request, restriction["auth_level"]):
            response = await f(request, *args, **kwargs)
            return response
        else:
            raise AccessDenied()
    return wrapped


class Authenticator(_Db):
    def __init__(
        self,
        *,
        path: str = "db.db",
        users: Optional[list[dict[str, str]]] = None,
        restrictions: Optional[list[dict[str, str]]] = None,
        max_age: Optional[int] | None = None,
        replace: bool = False
        ) -> None:
        super().__init__(path)
        self.max_age = max_age

        tables = self.get_tables("weetags")
        if ((len(tables) == 0 or replace) and 
            (users is None or restrictions is None)):
            raise ValueError("you define users and restrictions in your configurations")
        
        if len(tables) > 0 and replace:
            self.drop("weetags__restrictions")
            self.drop("weetags__users")
        
        if len(tables) == 0 or replace:
            self.create_table(
                UsersTable.initialize(),
                RestrictionsTable.initialize()
            )
            self._set_users(users)
            self._set_restrictions(restrictions)
        
    def authenticate(self, request: Request) -> dict[str, str] | None:
        if not request.token:
            return None

        try:
            payload = jwt.decode(
                request.token, request.app.config.SECRET, algorithms=["HS256"]
            )
        except jwt.exceptions.InvalidTokenError:
            raise InvalidToken()
        else:
            return payload
        
    def authorize(self, request: Request, restriction: str) -> bool:
        auth = [False, False]
        payload = self.authenticate(request)
        if payload is None:
            raise AuthorizationTokenRequired()
        
        if restriction in payload["auth_level"]:
            auth[0] = True
            
        if payload["max_age"] is None:
            auth[1] = True
        elif int(time.time()) <= payload["max_age"]:
            auth[1] = True
        else:
            raise OutatedAuthorizationToken()
        return all(auth)

    def login(self, request: Request) -> bool:
        payload = request.ctx.params
        username = payload.get("username", None)
        password = payload.get("password", None)
        
        if username is None or password is None:
            raise MissingLogin()
             
        res = self.con.execute(
            "SELECT password_sha256, salt, auth_level FROM weetags__users WHERE username=?",
            [username]
        ).fetchone()
        
        if res is None:
            raise InvalidLogin()
        
        password_sha256 = sha256((res["salt"] + password).encode("utf-8")).hexdigest()   
        if password_sha256 != res["password_sha256"]:
            raise InvalidLogin()

        token = jwt.encode({"auth_level": res["auth_level"].split(','), "max_age": self._set_token_max_age()}, request.app.config.SECRET)
        return text(token)
        
        
    def has_restriction(self, route: Route, tree_name: str) -> str | None:
        _, blueprint, _ = route.name.split('.')
        auth_level = self.con.execute(f"SELECT auth_level FROM weetags__restrictions WHERE tree=? AND blueprint=?",[tree_name, blueprint]).fetchone()
        return auth_level
    
    
    def _set_restrictions(self, restrictions: dict[str, str]) -> None:
        for r in restrictions:
            auth_level = ",".join(r["auth_level"])
            self.write(
                "weetags__restrictions", 
                ["tree", "blueprint", "auth_level"], 
                [r["tree"], r["blueprint"], auth_level]
            )
            
    def _set_users(self, users: list[str, str]) -> None:
        for user in users:
            password = user.get("password", None)
            auth_levels = user.get("auth_level", None)

            if auth_levels is None:
                raise KeyError("must set list of authorizations for each users")
            
            if password is None:
                raise KeyError("Users must have password set")

            auth_level = ",".join(auth_level)
            salt = generate_uuid()
            password_sha256 = sha256((salt + password).encode("utf-8")).hexdigest()

            self.write(
                "weetags__users", 
                ["username", "password_sha256", "auth_level", "salt"], 
                [user["username"], password_sha256, auth_level, salt]
            )
        
    def _set_token_max_age(self) -> int | None:
        max_age = self.max_age
        if self.max_age:
            max_age = int(time.time()) + self.max_age
        return max_age

    
    
    # def _users_table(self) -> None:
    #     self._drop("weetags__users")
    #     self._table(
    #         "weetags__users", 
    #         {"username": "TEXT", "password_sha256": "TEXT", "auth_level": "TEXT"}, 
    #         ["username"]
    #     )

        

    # def _restriction_table(self) -> None:
    #     self._drop("weetags__restrictions")
    #     self._table(
    #         "weetags__restrictions", 
    #         {"tree": "TEXT", "blueprint": "TEXT", "auth_level": "TEXT"}, 
    #         ["tree","blueprint"]
    #     )
