import jwt
import time
from hashlib import sha256
from functools import wraps
from sanic import text
from sanic.request import Request
from sanic_routing import Route

from typing import Optional, Any

from weetags.trees.db import Db
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


class Authenticator(Db):
    def __init__(
        self,
        *,
        path: str = "db.db",
        users: Optional[list[dict[str, str]]] = None,
        restrictions: Optional[list[dict[str, str]]] = None,
        salt: Optional[str] | None = None,
        max_age: Optional[int] | None = None
        ) -> None:
        super().__init__(path)
        self.salt = salt
        self.max_age = max_age

        if users is None:
            self._users_table()
        else:
            self._users_table()
            self._fill_users(users)
        if restrictions is not None and users is not None:
            self._restriction_table()
            self._fill_restrictions(restrictions)
        elif restrictions is None:
            self._restriction_table()
        
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
        
        if username is None:
            raise MissingLogin()
        
        if password is None: 
            raise MissingLogin()
        
        password_sha256 = sha256(password.encode("utf-8")).hexdigest()        
        res = self.con.execute(
            "SELECT auth_level FROM weetags__users WHERE username=? AND password_sha256=?",
            [username, password_sha256]
        ).fetchone()
        
        if res is None:
            raise InvalidLogin()

        token = jwt.encode({"auth_level": res["auth_level"].split(','), "max_age": self._set_token_max_age()}, request.app.config.SECRET)
        return text(token)
        
        
    def has_restriction(self, route: Route, tree_name: str) -> str | None:
        _, blueprint, _ = route.name.split('.')
        auth_level = self.con.execute(f"SELECT auth_level FROM weetags__restrictions WHERE tree=? AND blueprint=?",[tree_name, blueprint]).fetchone()
        return auth_level
    
    
    
    def _users_table(self) -> None:
        self._drop("weetags__users")
        self._table(
            "weetags__users", 
            {"username": "TEXT", "password_sha256": "TEXT", "auth_level": "TEXT"}, 
            ["username"]
        )

        

    def _restriction_table(self) -> None:
        self._drop("weetags__restrictions")
        self._table(
            "weetags__restrictions", 
            {"tree": "TEXT", "blueprint": "TEXT", "auth_level": "TEXT"}, 
            ["tree","blueprint"]
        )


    def _fill_restrictions(self, restrictions: dict[str, str]) -> None:
        for r in restrictions:
            auth_level = ",".join(r["auth_level"])
            self._write(
                "weetags__restrictions", 
                ["tree", "blueprint", "auth_level"], 
                [r["tree"], r["blueprint"], auth_level]
            )
            
    def _fill_users(self, users: dict[str, str]) -> None:
        for user in users:
            password = users.get("password", None)
            password_sha256 = users.get("password_sha256", None)
            if password is None and password_sha256 is None:
                raise KeyError("Users must have password or password_sh256 set")
            
            if password:
                password_sha256 = sha256(password.encode("utf-8")).hexdigest()
    
            auth_level = ",".join(user["auth_level"])
            self._write(
                "weetags__users", 
                ["username", "password_sha256", "auth_level"], 
                [user["username"], password_sha256, auth_level]
            )

    def _apply_salt(self, password: str) -> str:
        if self.salt:
            password = self.salt + password
        return password
        
    def _set_token_max_age(self) -> int | None:
        max_age = self.max_age
        if self.max_age:
            max_age = int(time.time()) + self.max_age
        return max_age

    
    
    