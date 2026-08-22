from __future__ import annotations

from logging import config
from os import uname
from pathlib import Path
from sqlalchemy import Table, Index, UniqueConstraint

from typing import Any, Literal, Type

from weetags.common import EngineURI, Engine
from weetags.common.types import OnChange, OnCollision
from weetags.common.base import TreeTopologyDefinition, TreeMetadataDefinition, TreeViewDefinition
import weetags.common.configs as conf
from weetags.common.loaders import Loader, YamlLoader
from weetags.tree.importer import Importer
from weetags.tree.tree import Tree
from weetags.tree.tree_cache import TreeCache


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



    @classmethod
    def build_from_tree_file(cls, path: str | Path, loader: Type[Loader] = YamlLoader) -> Tree:
        configs = conf.TreeConfig.parse_file(path, loader)

        kwargs = configs.tree_inline
        cache, c = None, kwargs.pop("cache", None)
        if c is not None:
            cache = TreeCache(**c)

        builder = cls.from_uri(configs.uri)
        return builder._build(configs, cache)


    def build_from_file(
        self,
        name: str, 
        fields: list[conf.FieldDefinition], 
        data_path: Path | str | None = None,
        keymap: dict[str, Any] | None = None,
        indexes: list[list[str]] | None = None,
        unique_constraints: list[list[str]] | None = None,

        not_exist: bool = False,
        recreate: bool = False,
        on_change: OnChange = "raise",
        on_collision: OnCollision = "raise",
        skip_init: bool = False,
        skip_import: bool = False,

        cache: TreeCache | None = None
    ) -> Tree:
        s = conf.TreeStructureDefinition(fields, indexes, unique_constraints)
        b = conf.BuilderDefinition(not_exist, recreate, on_change, on_collision, skip_init,skip_import)
        d = conf.DataDefinition(path=data_path, keymap=keymap)
        configs = conf.TreeConfig(name, self.engine.uri, "tree", cache=None, structure=s, data=d, builder=b)
        return self._build(configs, cache)

    def build(
        self,
        name: str, 
        fields: list[conf.FieldDefinition], 
        data: list[dict[str, Any]] | None = None,
        keymap: dict[str, Any] | None = None,
        indexes: list[list[str]] | None = None,
        unique_constraints: list[list[str]] | None = None,

        not_exist: bool = False,
        recreate: bool = False,
        on_change: OnChange = "raise",
        on_collision: OnCollision = "raise",
        skip_init: bool = False,
        skip_import: bool = False,

        cache: TreeCache | None = None
    ) -> Tree:
        s = conf.TreeStructureDefinition(fields, indexes, unique_constraints)
        b = conf.BuilderDefinition(not_exist, recreate, on_change, on_collision, skip_init,skip_import)
        d = conf.DataDefinition(data=data, keymap=keymap)
        configs = conf.TreeConfig(name, self.engine.uri, "tree", cache=None, structure=s, data=d, builder=b)
        return self._build(configs, cache)

    def _build(self, configs: conf.TreeConfig, cache: TreeCache | None = None) -> Tree:
        b = configs.builder
        if b is None:
            raise ValueError("Builder needs Builder configuration.")

        do = self._check_existing(configs.name, b.not_exist, b.skip_init)
        if do is False:
            return Tree.from_engine(configs.name, self.engine, cache)

        s = configs.structure
        if s is None:
            raise ValueError("Builder needs Structure configuration.")
        do = self._check_existing_structure(configs.name, s.fields, b.recreate, b.on_change, b.skip_import)
        if do is False:
            return Tree.from_engine(configs.name, self.engine, cache)

        if b.skip_init is False:
            tree = self.initialize_tree(configs.name, s.fields, s.indexes, s.unique_constraints, b.on_change)

        d = configs.data
        if d and b.skip_import is False:
            tree = self.populate_tree(configs.name, d, b.on_collision)
        else:
            tree = Tree.from_engine(configs.name, self.engine)

        tree.set_cache(cache)
        return tree

    def initialize_tree(
        self, 
        name: str, 
        fields: list[conf.FieldDefinition], 
        indexes: list[list[str]] | None = None,
        unique_constraints: list[list[str]] | None = None,
        on_change: OnChange | str= "raise"
    ) -> Tree:
        self.engine.create_schema()
        tree_topology = self._intialize_tree_topology(name)
        tree_metadata = self._intialize_tree_metadata(name, fields, tree_topology, indexes, unique_constraints)
        self._initialize_tree_view(name, tree_topology, tree_metadata)
        return Tree.from_engine(name, self.engine)

    def populate_tree(self, name: str, data: conf.DataDefinition, on_collision: OnCollision | str = "raise") -> Tree:
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
        table = self.engine.create_table(f"_{name}_topology", TreeTopologyDefinition.columns, exist_ok=True)
        return table

    def _intialize_tree_metadata(
        self, 
        name: str, 
        fields: list[conf.FieldDefinition], 
        topology: Table,
        indexes: list[list[str]] | None = None,
        unique_constraints: list[list[str]] | None = None
    ) -> Table:
        for f in fields:
            if f.name in TreeTopologyDefinition().namespace:
                raise ValueError(f"fields name already in use: {TreeTopologyDefinition().namespace}.")
        
        columns = TreeMetadataDefinition().generate_columns(fields, topology)


        builded_indexes = self._indexes(name, fields, indexes)
        builded_uconstraint = self._unique_constraints(name, fields, unique_constraints)
        table = self.engine.create_table(
            f"_{name}_metadata", 
            columns, 
            indexes=builded_indexes, 
            unique_constraints=builded_uconstraint,
            exist_ok=True
        )
        return table
    
    def _initialize_tree_view(self, name: str, topology: Table, metadata: Table) -> None:
        stmt = TreeViewDefinition(self.engine.uri.dialect).render_view_query(name, topology, metadata)
        self.engine.execute_statement(stmt)

    def _check_existing(self, name: str, not_exist: bool = False, skip_init: bool = False) -> bool:
        if self.engine.exist(name) and not_exist:
            # tree structure already exist, 
            # building is only done when the structure does NOT exist
            return False
        elif self.engine.exist(name) is False and skip_init:
            raise AssertionError("Tree must be initialized. deactivate builder `skip_init` argument.")
        return True

    def _check_existing_structure(
        self, 
        name: str, 
        fields: list[conf.FieldDefinition], 
        recreate: bool = False, 
        on_change: OnChange | str = "raise", 
        skip_import: bool = False
    ) -> bool:
        if self.engine.exist(name) and recreate:
            self.engine._drop(name)
        elif self.engine.exist(name):
            # TEST STRUCUTURE AGAINST THE NEW ONE. APPLY DEFINED STRATEGY WHEN CHANGES
            # diff = self compare structure
            # if diff and on_change == "alter":
            #     # diff are found we can try to alter inplace structure.
            #     ...
            # elif diff and on_change == "recreate":
            #     # tthre is a diff, we want to drop the tree
            #     self.engine._drop_tree(name)
            # elif diff and on_change == "raise":
            #     # there is a diff and we want to raise
            #     raise ValueError()
            # elif diff and on_change == "ignore" and skip_import is False:
            #      # there is a change, we ignore it, but we want to do the importation step. there will be a conflict...
            #     raise ValueError()
            # else:
            #     raise ValueError()
            raise NotImplementedError()
        return True

    def _indexes(
        self, 
        name: str, 
        fields: list[conf.FieldDefinition], 
        indexes: list[list[str]] | None = None
    ) -> list[Index]:
        if indexes is None:
            return []

        i = []
        field_names = [f.name for f in fields]
        for index in indexes:
            valid = all([fname in field_names for fname in index])
            if valid:
                index_name = f"_{name}_metadata_" + "_".join(index) + "_index"
                ii = Index(index_name, *index)
                i.append(ii)
        return i

    def _unique_constraints(
        self, 
        name: str, 
        fields: list[conf.FieldDefinition], 
        unique_constraints: list[list[str]] | None = None
    ) -> list[UniqueConstraint]:
        if unique_constraints is None:
            return []

        constraints = []
        field_names = [f.name for f in fields]
        for constraint in unique_constraints:
            valid = all([fname in field_names for fname in constraint])
            if valid:
                constraint_name = f"_{name}_metadata_" + "_".join(constraint) + "_unique"
                uc = UniqueConstraint(*constraint, name=constraint_name)
                constraints.append(uc)
        return constraints