from __future__ import annotations

import sys
from os import environ
from pathlib import Path
from sanic import Sanic
from sanic.log import LOGGING_CONFIG_DEFAULTS

from typing import Any, Optional

from weetags.tree import Tree
from weetags.app.parsers import get_config
from weetags.tree_builder import TreeBuilder
from weetags.app.authentication import Authenticator
from weetags.app.routes import base, shower, records, utils, writer, login
from weetags.app.middlewares import log_entry, log_exit, cookie_token, error_handler

Settings = dict[str, Any]

class Weetags(object):
    def __init__(
        self,
        *,
        env: str,
        trees: dict[str, Settings],
        sanic: Optional[Settings] | None = None,
        logging: Optional[Settings] | None = None,
        authentication: Optional[Settings] | None = None
        ) -> None:

        self.env = env
        self.print_banner()

        if not trees:
            raise ValueError("no trees settings")
        if sanic is None:
            sanic = {}

        self.app = Sanic("Weetags", log_config=self.configurate_logging(logging))
        self.app.config.update({k.upper():v for k,v in sanic.get("app", {}).items()})
        self.register_bluprints(sanic.get("blueprints", None))

        self.app.on_request(log_entry, priority=500)
        self.app.on_response(log_exit, priority=500)
        self.app.on_request(cookie_token, priority=99)
        self.app.error_handler.add(Exception, error_handler)

        self.app.ctx.trees = self.register_trees(trees)

        self.app.ctx.authenticator = None
        if authentication:
            self.app.ctx.authenticator = Authenticator.initialize(**authentication)


    @classmethod
    def create_app(cls) -> Sanic:
        cfg = get_config(environ.get("WEETAGS_CONFIG_FILEPATH", "./configs/configs.yaml"))
        weetags = cls(**cfg)
        return weetags.app

    def print_banner(self):
        print((Path(__file__).parent / "banner").read_text())
        print(f"Booting {self.env} ENV")

    def register_trees(self, trees_settings: dict[str, Settings]) -> dict[str, Tree]:
        return {name:TreeBuilder.build_tree(**settings) for name, settings in trees_settings.items()}

    def register_bluprints(self, blueprints: list[str] | None) -> None:
        self.app.blueprint(base)
        if blueprints is None:
            raise ValueError("you must register blueprints")
        [self.app.blueprint(getattr(sys.modules[__name__], b)) for b in blueprints]

    def configurate_logging(self, logging: dict[str, Any] | None) -> dict[str, Any]:
        if logging is None:
            return LOGGING_CONFIG_DEFAULTS
        logging["loggers"].update(LOGGING_CONFIG_DEFAULTS["loggers"])
        logging["handlers"].update(LOGGING_CONFIG_DEFAULTS["handlers"])
        logging["formatters"].update(LOGGING_CONFIG_DEFAULTS["formatters"])
        return logging
