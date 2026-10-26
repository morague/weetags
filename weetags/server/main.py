from __future__ import annotations

import os
import glob
import sanic_ext
from sanic_ext import openapi
from sanic import Sanic
from pathlib import Path
from sanic.log import LOGGING_CONFIG_DEFAULTS

from typing import Any, Type


from weetags.common.loaders import ConfigLoader, YamlLoader, Loader, select_loader
from weetags.common.configs import TreeConfig
import weetags.server.listeners as lstn
import weetags.server.middlewares as mdlw
from weetags.server.routes import weetagsbp, nodesbp, utilsbp, auth, explorerbp, basebp, topologybp, metadatabp
from weetags.server.authentication import Authenticator



class Weetags:
    app: Sanic

    def __init__(
        self, 
        trees: str | Path, 
        authenticator: Authenticator | None = None,
        configs: dict[str, Any] | None = None,
        api: dict[str, Any] | None = None,
        logging_configs: dict[str, Any] | None = None
    ) -> None:
        self.app = Sanic("weetags", log_config=self.logging(logging_configs))
        self.app.config.TREES_CONFIGS = self.tree_configs(trees)
        self.app.config.TEMPLATING_ENABLE_ASYNC = True
        self.app.config.TEMPLATING_PATH_TO_TEMPLATES = "weetags/server/templates/"

        if authenticator is not None:
            self.app.ctx.auth = authenticator
        if api is None:
            api = {}

        self.update_configs(configs)
        
        self.app.static("/static", file_or_directory="./weetags/server/static", directory_view=True)
        self.load_api(**api)
        self.app.error_handler.add(Exception, mdlw.error_handler)

        self.app.main_process_start(lstn.states)
        self.app.main_process_start(lstn.header)
        self.app.after_server_start(lstn.build, priority=10)
        self.app.after_server_start(lstn.load_trees, priority=9)

        self.app.config.OAS_AUTODOC = False
        oas = YamlLoader().load("weetags/server/oas.yml")
        self.app.ext.openapi.raw(oas)


    def __call__(self, *args: Any, **kwds: Any) -> Sanic:
        return self.app

    @classmethod
    def create_app(cls, path: str | Path | None = None) -> Weetags:
        env_path = os.environ.get("SERVER_CONFIGS_PATH", None)
        if path is None and env_path is None:
            raise ValueError("Configs file path not found.")
        if path is None:
            path = env_path
        assert path is not None
        configs = ConfigLoader().load(path, select_loader(path))
        return cls.from_configs(configs)   

    @classmethod
    def from_configs(cls, configs: dict[str, Any]) -> Weetags:
        trees = configs.get("trees", None)
        if trees is None:
            raise ValueError("Define trees.")
        
        auth_configs = configs.get("auth")
        auth = None
        if auth_configs is not None:
            auth = Authenticator.from_configs(auth_configs)

        logging = configs.get("logging", None)
        app_configs = configs.get("configs", None)
        return cls(trees, auth, app_configs, logging)

    def tree_configs(self, file_or_folder: str | Path) -> dict[str, Any]:
        path = Path(file_or_folder)

        if path.is_dir():
            paths = glob.glob(f"{str(path)}/*")
            trees_configs = [TreeConfig.parse_file(p, select_loader(p)) for p in paths]
        else:
            trees_configs = [TreeConfig.parse_file(path, select_loader(path))]
        return {t.name:t for t in trees_configs}

    def update_configs(self, configs: dict[str, Any] | None = None) -> None:
        self.app.update_config({
            "TEMPLATING_ENABLE_ASYNC": True,
            "TEMPLATING_PATH_TO_TEMPLATES": "weetags/server/templates",
            "CORS_ORIGIN": "*"
        })
        if configs is not None:
            self.app.config.update({k.upper(): v for k, v in configs.items()})

    def logging(self, logging_configs: dict[str, Any] | None = None) -> dict[str, Any]:
        if logging_configs is None:
            default = {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "simple": {
                        "class": "logging.Formatter",
                        "format": "[%(asctime)s][%(process)d][%(name)7s][%(levelname)8s] | %(message)s",
                        "datefmt": "%d-%m-%Y %H:%M:%S",
                    }
                },
                "handlers": {
                    "stream": {
                        "class": "logging.StreamHandler",
                        "level": "INFO",
                        "formatter": "simple",
                        "stream": "ext://sys.stdout",
                    }
                },
                "loggers": {
                    "access": {
                        "level": "INFO",
                        "handlers": ["stream"],
                        "propagate": False
                    },
                    "build": {
                        "level": "INFO",
                        "handlers": ["stream"],
                        "propagate": False
                    }
                }
            }
        else:
            default = logging_configs
        default["loggers"].update(LOGGING_CONFIG_DEFAULTS["loggers"])
        default["handlers"].update(LOGGING_CONFIG_DEFAULTS["handlers"])
        default["formatters"].update(LOGGING_CONFIG_DEFAULTS["formatters"])
        return default

    def set_api_v1(self) -> None:
        self.app.blueprint(basebp)
        self.app.blueprint(nodesbp)
        self.app.blueprint(utilsbp)
        self.app.blueprint(auth)
        self.app.blueprint(explorerbp)
        self.app.blueprint(topologybp)
        self.app.blueprint(metadatabp)
        self.app.on_request(mdlw.go_fast, priority=10)
        self.app.on_request(mdlw.authorize, priority=9)
        self.app.on_response(mdlw.log_exit, priority=10)

        openapi.exclude(bp=basebp)
        openapi.exclude(bp=nodesbp)
        openapi.exclude(bp=utilsbp)
        openapi.exclude(bp=auth)
        openapi.exclude(bp=explorerbp)
        openapi.exclude(bp=topologybp)
        openapi.exclude(bp=metadatabp)

    def load_api(self, **v: bool) -> None:
        if v.get("v1", True):
            self.set_api_v1()