import random
import pytest
import jwt
import time
from sanic import Sanic, response, Request
from sanic.blueprints import Blueprint

from weetags.app.authentication import Authenticator, protected
from weetags.engine.schema import Restriction, User
from weetags.exceptions import InvalidLogin, OutatedAuthorizationToken, InvalidToken, AccessDenied, AuthorizationTokenRequired

random.seed(10)


@pytest.fixture
def app():
    app = Sanic("TestSanic")
    app.config["SECRET"] = "xxx"

    reader = Blueprint("reader")

    @reader.get("/<tree_name>")
    def foo(request, tree_name):
        return response.text("foo")

    @reader.get("/reader/<tree_name>")
    @protected
    def bar(request, tree_name):
        return response.text("bar")

    app.blueprint(reader)
    return app

@pytest.mark.auth
def test_users():
    user1 = {
        "username": "admin",
        "password": "admin",
        "max_age": 600
    }

    user3 = {
        "username": "admin",
        "password": "admin",
        "auth_level": ["super user"],
        "max_age": 600.9
    }

    user4 = {
        "username": "admin",
        "password": "admin",
        "auth_level": ["super user"],
        "max_age": 600
    }

    with pytest.raises(TypeError):
        u1 = User(**user1)
        u3 = User(**user3)

    u4 = User(**user4)
    assert u4.values == [
        'admin',
        '4a1cebdf32cdb6e13538e665c0586808086d1478a493fc3b7bd37aed49092fe2',
        ['super user'],
        'KcBEKanDFrPZkcHF',
        600
    ]

@pytest.mark.auth
def test_db():

    user1 = {
        "username": "admin",
        "password": "admin",
        "auth_level": ["super user"],
        "max_age": 600
    }

    user2 = {
        "username": "admin2",
        "password": "admin2",
        "auth_level": ["user"],
        "max_age": 600
    }
    restriction1 = {"tree": "topics", "blueprint": "reader", "auth_level": ["admin", "super admin"]}

    auth = Authenticator.initialize(users=[user1, user2], restrictions=[restriction1], database=":memory:")

    user = auth.con.execute("SELECT * FROM weetags__users").fetchone()
    restriction = auth.con.execute("SELECT * FROM weetags__restrictions").fetchone()
    auth_level =auth._get_restriction("topics", "reader")
    assert user == {
        'username': 'admin',
        'password_sha256': 'a2aaf7d5cc93bfd1d1f885bba96dbf2d20d0db3fa00263ea11a50365f50da80e',
        'auth_level': ['super user'],
        'salt': 'uepVxcAiMwyAsRqD',
        'max_age': 600
    }
    assert restriction == restriction1
    assert auth_level == {"auth_level": ["admin", "super admin"]}
    assert auth._get_user("admin3") == None

    user4 = {
            "username": "bbb",
            "password": "aaa",
            "auth_level": ["user"],
            "max_age": 1000
        }
    auth._add_users(User(**user4))
    user = auth._get_user("bbb")

    assert user == {
        "username": "bbb",
        'password_sha256': 'f05e44f36fc5a9e48926d9871402fe5d8f9132cba1c6966d7b0a4267c1a1f90e',
        'auth_level': ['user'],
        'salt': 'imtIxXpuQJCBEePL',
        'max_age': 1000
    }

@pytest.mark.auth
def test_auth(app):
    user1 = {
        "username": "admin",
        "password": "admin",
        "auth_level": ["super user"],
        "max_age": 600
    }

    user2 = {
        "username": "admin2",
        "password": "admin2",
        "auth_level": ["admin"],
        "max_age": 0
    }

    restriction1 = {"tree": "topics", "blueprint": "reader", "auth_level": ["admin", "super admin"]}
    auth = Authenticator.initialize(users=[user1, user2], restrictions=[restriction1], database=":memory:")

    request, response = app.test_client.get("/topics")
    with pytest.raises(InvalidLogin):
        token = auth.authenticate(request, "admin", "admn")
        token = auth.authenticate(request, "admn", "admin")

    token = auth.authenticate(request, "admin2", "admin2")
    request.headers.add("Authorization", f"Bearer {token}")
    time.sleep(1)
    with pytest.raises(OutatedAuthorizationToken):
        auth.authorize(request)

    request, response = app.test_client.get("/topics")
    token = auth.authenticate(request, "admin", "admin")
    request.headers.add("Authorization", f"Bearer {token}")
    with pytest.raises(AccessDenied):
        auth.authorize(request)

    request, response = app.test_client.get("/topics")
    token = auth.authenticate(request, "admin2", "admin2") + "a"
    request.headers.add("Authorization", f"Bearer {token}")
    with pytest.raises(InvalidToken):
        auth.authorize(request)

    request, response = app.test_client.get("/topics")
    with pytest.raises(AuthorizationTokenRequired):
        auth.authorize(request)

    request, response = app.test_client.get("/topics")
    token = auth.authenticate(request, "admin2", "admin2")
    request.headers.add("Authorization", f"Bearer {token}")
    assert auth.authorize(request) == True

    app.ctx.authenticator = auth
    request, response = app.test_client.get("/topics")
    token = auth.authenticate(request, "admin2", "admin2")
    request.headers.add("Authorization", f"Bearer {token}")
    request, response = app.test_client.get("/reader/topics")
