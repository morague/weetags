from __future__ import annotations

import sys
from os import environ
from sanic import Sanic
from sanic.log import LOGGING_CONFIG_DEFAULTS

from typing import Any, Optional

from weetags.exceptions import TreeDoesNotExist

from weetags.routes.reader import reader
from weetags.routes.writer import writer
from weetags.routes.login import login
from weetags.routes.errors import error_handler
from weetags.routes.middlewares import log_entry, log_exit


from weetags.trees.treeBuilder import PermanentTreeBuilder
from weetags.trees.permanent_tree import PermanentTree

from weetags.tools.parsers import get_config
from weetags.tools.authentication import Authenticator

TreeSettings = dict[str, Any]


banner = """
      ___           ___           ___           ___           ___           ___           ___       
     /\__\         /\  \         /\  \         /\  \         /\  \         /\  \         /\  \    
    /:/ _/_       /::\  \       /::\  \        \:\  \       /::\  \       /::\  \       /::\  \   
   /:/ /\__\     /:/\:\  \     /:/\:\  \        \:\  \     /:/\:\  \     /:/\:\  \     /:/\ \  \  
  /:/ /:/ _/_   /::\~\:\  \   /::\~\:\  \       /::\  \   /::\~\:\  \   /:/  \:\  \   _\:\~\ \  \ 
 /:/_/:/ /\__\ /:/\:\ \:\__\ /:/\:\ \:\__\     /:/\:\__\ /:/\:\ \:\__\ /:/__/_\:\__\ /\ \:\ \ \__\ 
 \:\/:/ /:/  / \:\~\:\ \/__/ \:\~\:\ \/__/    /:/  \/__/ \/__\:\/:/  / \:\  /\ \/__/ \:\ \:\ \/__/
  \::/_/:/  /   \:\ \:\__\    \:\ \:\__\     /:/  /           \::/  /   \:\ \:\__\    \:\ \:\__\  
   \:\/:/  /     \:\ \/__/     \:\ \/__/     \/__/            /:/  /     \:\/:/  /     \:\/:/  /  
    \::/  /       \:\__\        \:\__\                       /:/  /       \::/  /       \::/  /   
     \/__/         \/__/         \/__/                       \/__/         \/__/         \/__/                                                                                                                                                                                                                             
"""



class Weetags(object):
    def __init__(
        self,
        *,
        env: str,
        trees: dict[str, TreeSettings],
        sanic: dict[str, Any] | None,
        logging: Optional[dict[str, Any]] | None = None,
        authentication: Optional[dict[str, Any]] | None = None
        ) -> None:
        
        self.env = env
        self.print_banner()
        
        if not trees:
            raise ValueError("no trees settings")
        
        
        logging["loggers"].update(LOGGING_CONFIG_DEFAULTS["loggers"])
        logging["handlers"].update(LOGGING_CONFIG_DEFAULTS["handlers"])
        logging["formatters"].update(LOGGING_CONFIG_DEFAULTS["formatters"])
        
        trees = self.register_trees(trees)
        self.app = Sanic("Weetags", log_config=logging)
        self.app.config.update({k.upper():v for k,v in sanic.get("app", {}).items()})
        self.register_bluprints(sanic.get("blueprints", None))
        self.app.on_request(log_entry, priority=500)
        self.app.on_response(log_exit, priority=500)
        self.app.error_handler.add(Exception, error_handler)
        self.app.ctx.trees = trees
        self.app.ctx.authenticator = None
        if authentication:
            self.app.ctx.authenticator = self.build_auth_interface(authentication)
            
    @classmethod
    def create_app(cls):
        cfg = get_config(environ.get("CONFIG_FILEPATH", "./configs/configs.yaml"))
        return cls(**cfg)

    def register_trees(self, trees: dict[str, TreeSettings]) -> dict[str, PermanentTree]:
        return {name:PermanentTreeBuilder.build_permanent_tree(**settings) for name, settings in trees.items()}

    def print_banner(self):
        print(banner)
        print(f"Booting {self.env} ENV")
    
    def register_bluprints(self, blueprints: list[str] | None) -> None:
        if blueprints is None:
            raise ValueError("you must register blueprints")
        [self.app.blueprint(getattr(sys.modules[__name__], blueprints[0])) for b in blueprints]
    
    def build_auth_interface(self, authentication: dict[str, Any]) -> Authenticator:
        return Authenticator(**authentication)


