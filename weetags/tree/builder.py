from __future__ import annotations

from pathlib import Path
from sqlalchemy import Table

from typing import Any, Literal

from weetags.common import EngineURI, Engine
from weetags.common.types import OnChange, OnCollision
from weetags.common.base import TreeTopologyDefinition, TreeMetadataDefinition, TreeViewDefinition
from weetags.common.configs import DataDefinition, FieldDefinition, TreeConfig
from weetags.common.loaders import Loader
from weetags.tree.importer import Importer
from weetags.tree.tree import Tree


"""
args, kwargs:

    init oriented args:
        order of usage: not_exist > skip_init > recreate > on_change > only_init

        not_exist: bool > do not create nor populate if tree structure already exist  ???? or recreate ???
        recreate: bool > force recreating the structure
        on_change: str or enum value
            raise > raise error if any modifications
            ignore > ignore any modifications made to the data source if the actual structure in place is different
            alter > alter exisiting structure when applicable, else raise
            recreate > drop and recreate whole structure.
        only_init: bool > do not populate


    importation oriented args:
        on_colision: str or Enum value. 
            raise > whenever a colision occur, raise an error
            ignore > ignore payload
            update > update metadata only
            replace > replace inplace by payload
"""


class TreeBuilder:
    """
    TODO: add builder arguments
    TODO: check database before hand
    TODO: build indexes
    """
    engine: Engine

    def __init__(self, engine: Engine):
        self.engine = engine

    @classmethod
    def from_uri(cls, uri: EngineURI) -> TreeBuilder:
        engine = Engine.from_uri(uri)
        return cls(engine)

    def build(
        self, 
        name: str, 
        fields: list[FieldDefinition], 
        data: list[dict[str, Any]] | None = None,
        keymap: dict[str, Any] | None = None,
        indexes: list[list[str]] | None = None,

        not_exist: bool = False,
        recreate: bool = False,
        on_change: OnChange = "raise",
        on_collision: OnCollision = "raise",
        skip_init: bool = False,
        skip_import: bool = False,
    ) -> Tree:

        do = self._handle_existing_structure(name, fields, not_exist, recreate, on_change, skip_init)
        if do is False:
            return Tree.from_engine(name, self.engine)

        if skip_init is False:
            tree = self.initialize_tree(name, fields)
        if data is not None and skip_import is False:
            data_definition = DataDefinition(data=data, keymap=keymap)
            tree = self.populate_tree(name, data_definition, on_collision)
        else:
            tree = Tree.from_engine(name, self.engine)
        return tree

    def build_from_file(
        self, 
        path: str | Path, 
        loader: Loader | None = None,


        not_exist: bool = False,
        recreate: bool = False,
        on_change: OnChange = "raise",
        on_collision: OnCollision = "raise",
        skip_init: bool = False,
        skip_import: bool = False,
    ) -> Tree:
  
        configs = TreeConfig.parse_file(path, loader)
        do = self._handle_existing_structure(configs.name, configs.fields, not_exist, recreate, on_change, skip_init)
        if do is False:
            return Tree.from_engine(configs.name, self.engine)

        # BUILD PROCESS
        if skip_init is False:
            self.initialize_tree(configs.name, configs.fields, on_change)

        if skip_import is False and configs.data is not None:
            self.populate_tree(configs.name, configs.data, on_collision)
        return Tree.from_engine(configs.name, self.engine)

    def initialize_tree(self, name: str, fields: list[FieldDefinition], on_change: OnChange = "raise") -> Tree:
        self.engine._create_schema()
        tree_topology = self._intialize_tree_topology(name)
        tree_metadata = self._intialize_tree_metadata(name, fields, tree_topology)
        self._initialize_tree_view(name, tree_topology, tree_metadata)
        return Tree.from_engine(name, self.engine)

    def populate_tree(self, name: str, data: DataDefinition, on_collision: OnCollision = "raise") -> Tree:
        if data.path is None and data.data is None:
            return Tree.from_engine(name, self.engine)
        elif data.path is not None:
            Importer(name, self.engine, batch_size=100).load(data.path, keymap=data.keymap, on_collision=on_collision)
        elif data.data is not None:
            Importer(name, self.engine, batch_size=100).loads(data.data, keymap=data.keymap, on_collision=on_collision)
        else:
            raise ValueError("Datadefinition missconfigurated.")
        return Tree.from_engine(name, self.engine)

    def _intialize_tree_topology(self, name: str) -> Table:        
        table = self.engine._create_table(f"_{name}_topology", TreeTopologyDefinition.columns, exist_ok=True)
        return table

    def _intialize_tree_metadata(self, name: str, fields: list[FieldDefinition], topology: Table) -> Table:
        for f in fields:
            if f.name in TreeTopologyDefinition().namespace:
                raise ValueError(f"fields name already in use: {TreeTopologyDefinition().namespace}.")
        
        columns = TreeMetadataDefinition().generate_columns(fields, topology)
        table = self.engine._create_table(f"_{name}_metadata", columns, exist_ok=True)
        return table
    
    def _initialize_tree_view(self, name: str, topology: Table, metadata: Table) -> None:
        stmt = TreeViewDefinition(self.engine.uri.dialect).render_view_query(name, topology, metadata)
        self.engine.execute_statement(stmt)

    def _handle_existing_structure(
        self, 
        name: str, 
        fields: list[FieldDefinition],
        not_exist: bool = False, 
        recreate: bool = False, 
        on_change: OnChange = "raise", 
        skip_init: bool = False
    ) -> bool:
        if self.engine.exist(name) and not_exist:
            # tree structure already exist, 
            # building is only done when the structure does NOT exist
            return False
        
        elif self.engine.exist(name) is False and skip_init:
            raise AssertionError("Tree must be initialized. deactivate builder `skip_init` argument.")

        elif self.engine.exist(name) and recreate:
            # tree structure exist, but force recreate
            self.engine._drop_tree(name)
        elif self.engine.exist(name):
            # TEST STRUCUTURE AGAINST THE NEW ONE. APPLY DEFINED STRATEGY WHEN CHANGES
            raise NotImplementedError()
        
        return True
