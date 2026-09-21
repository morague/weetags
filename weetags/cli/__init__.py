from __future__ import annotations
from email.policy import default

import click
from pathlib import Path
from getpass import getpass
from typing import Literal

from weetags.common.loaders import YamlLoader
from weetags.server.authentication import Authenticator, EngineURI
from weetags.tree.builder import TreeBuilder

@click.group("wee")
def wee(): ...

@click.group("trees")
def trees(): ...

@click.group("users")
def users(): ...

@click.group("rules")
def rules(): ...

@click.group("update")
def user_update(): ...

@users.command("add")
@click.argument("username", type=str)
@click.option("--roles", "-r", multiple=True, type=str)
@click.option("--configs", "-c", default=Path("server.yml"), type=Path)
def add_user(username: str, roles: list[str], configs: Path):
    password = getpass("Password: ")
    confirm = password = getpass("Confirm Password: ")
    if password != confirm:
        raise ValueError("Confirmation different from password")

    c = YamlLoader().load(configs)
    auth = Authenticator.from_configs(c["auth"])
    auth.users.set_user(username, password, roles)

@users.command("rm")
@click.argument("username", type=str)
@click.option("--configs", "-c", default=Path("server.yml"), type=Path)
def rm_user(username: str, configs: Path) -> None:
    c = YamlLoader().load(configs)
    auth = Authenticator.from_configs(c["auth"])
    auth.users.remove_user(username)

@user_update.command("password")
@click.argument("username", type=str)
@click.option("--configs", "-c", default=Path("server.yml"), type=Path)
def update_password(username: str, configs: Path) -> None:
    c = YamlLoader().load(configs)
    auth = Authenticator.from_configs(c["auth"])

    old_password = getpass("Current Password: ")
    password = getpass("New Password: ")
    confirm = password = getpass("Confirm New Password: ")
    if password != confirm:
        raise ValueError("Confirmation different from password")
    
    auth.users.update_user_password(username, old_password, password, confirm)

@user_update.command("roles")
@click.argument("username", type=str)
@click.option("--roles", "-r", multiple=True, type=str)
@click.option("--configs", "-c", default=Path("server.yml"), type=Path)
def update_roles(username: str, roles: list[str], configs: Path) -> None:

    c = YamlLoader().load(configs)
    auth = Authenticator.from_configs(c["auth"])
    auth.users.update_user_roles(username, roles)

@rules.command("add")
@click.argument("rule", type=str)
@click.option("--methods", "-m", multiple=True, type=str)
@click.option("--priority", "-p", type=int)
@click.option("--role", "-r", type=str)
@click.option("--type", "-t", type=str)
@click.option("--configs", "-c", default=Path("server.yml"), type=Path)
def add_rule(rule: str, methods: list[str], priority: int, role: str, type: Literal["path", "blueprint"], configs: Path) -> None:
    if type not in ["path", "blueprint"]:
        raise ValueError("type is either: `path`, `blueprint`.")
    for m in methods:
        if m not in ["GET", "POST", "PATCH", "PUT", "DELETE", "*"]:
            raise ValueError("methods must be HTTP methods")
    if priority >= 1000:
        raise ValueError()

    c = YamlLoader().load(configs)
    auth = Authenticator.from_configs(c["auth"])
    auth.rules.set_rule(type, methods, rule, priority, role)


@rules.command("build")
@click.option("--configs", "-c", default=Path("server.yml"), type=Path)
def build_rules(configs: Path) -> None:
    c = YamlLoader().load(configs)
    auth = Authenticator.from_configs(c["auth"])

@trees.command("build")
@click.argument("path", type=Path)
def build_trees(path: Path) -> None:
    TreeBuilder.build_from_tree_file(path)

users.add_command(user_update)
wee.add_command(users)
wee.add_command(rules)
wee.add_command(trees)

if __name__ == "__main__":
    wee()