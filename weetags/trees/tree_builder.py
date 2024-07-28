from collections import defaultdict, deque
from typing import Any, TypeVar, Optional, Literal

from weetags.tools.loaders import Loader, JlLoader, JsonLoader
from weetags.database.db import _Db
from weetags.database.table import Table, Field
from weetags.database.namespace import NameSpace
from weetags.trees.tree_schema import NodesTable, MetadataTable, IndexTable
from weetags.trees.tree import Tree
from weetags.app.utils import infer_loader

Payload = dict[str, Any]
DataPath = TreeName = TableName = FieldName = str
_Nid = TypeVar("_Nid", str, int)
_SqliteTypes = TypeVar("_SqliteTypes", str, int, dict, list, bytes)
Loaders = Literal["default", "lazy"]


class TreeBuilder(_Db):
    BATCH_SIZE = 500
    root_id = None

    def __init__(
        self,
        name: str,
        db: str = ":memory:",
        data: list[Payload] | str | None = None,
        model: Optional[dict[FieldName, _SqliteTypes]] = None,
        strategy: Loaders = "lazy",
        ) -> None:
        """init Treebuilder.
        :params:
            :name: (str) tree name
            :path: (str, optionnal) path to the sqlite database. default: "db.db"
            :permanent: (bool) setup sqlite on `:memory:` when false. default: True
            :data: (list[Payload] | Path | None, optional) File path or list of records to populate inside the tree.
                    data can be none. In this case you must either provide the data model of the tree or having the tree already created in the database.
            :model: (dict[str, str], optionnal) dictionnary with fieldsnames as key and sqlite types as value.
            :strategy: (str, optionnal) data loading strategy. for large files, it is recommended to use the lazy loader. default: "lazy"

        Build TreeBuilder Object with following attributes:
        :attributes:
            :name: tree_name
            :data: instance able to load the data according to the selected strategy.
            :tables: tables instances with tables schema and queries generators.

        :raise:
            :valueError: When the TreeBuilder is unable to generate the table instances.
                        this can happen when no data, model nor existing tables are found.
        """
        super().__init__(db)
        self.name = name
        self._get_loader(data, strategy)
        self._get_model(model)
        self._get_tables()

    @classmethod
    def build_permanent_tree(
        cls,
        *,
        name: str,
        data: list[Payload] | str | None = None,
        db: str = ":memory:",
        indexes: Optional[list[FieldName]] = None,
        model: Optional[dict[FieldName, _SqliteTypes]] = None,
        strategy: Loaders = "lazy",
        read_only: bool = False,
        replace: bool = False
    ) -> Tree:
        builder = cls(name, db, data, model, strategy)
        if replace:
            builder.drop_tree()
        if not builder.get_tables(name) or replace:
            builder.build_tree_structure()
            builder.populate_tree()
            if indexes:
                builder.build_indexes(indexes)
        return Tree(name=name, db=db, read_only=read_only)


    def drop_tree(self) -> None:
        tables = self.get_tables(self.name)
        for table in tables:
            self.drop(table[0])

    def build_tree_structure(self) -> None:
        tables = [self.tables["nodes"], self.tables["metadata"]]
        self.create_table(*tables)

    def populate_tree(self) -> None:
        batch, parent2children = deque(), defaultdict(list)

        for node in self.data.loader():
            if node.get("children", None) is None:
                node.update({"children":[]})

            # add directly the root node ... with meta data.
            if node["parent"] is None and self.root_id is None:
                self._build_root(node)
                continue

            # build batch
            batch.append(node)
            if node.get("parent", None):
                parent2children[node["parent"]].append(node["id"])

            # write db when batch size is attained
            if len(batch) == self.BATCH_SIZE:
                (batch, parent2children) = self._build_nodes(batch, parent2children)
                parent2children = self._add_remaining_children(parent2children)
                self.con.commit()
        # do the remaining nodes
        if len(batch) > 0:
            (batch, parent2children) = self._build_nodes(batch, parent2children)
            parent2children = self._add_remaining_children(parent2children)
            self.con.commit()

        # still need to setup metadata
        self._build_metadata()
        self.con.commit()

    def build_indexes(self, indexes: list[FieldName]) -> None:
        nodes_table = self.tables["nodes"]
        for fname in indexes:
            field = getattr(self.tables["nodes"], fname.split(".")[0], None)
            if field is None:
                raise ValueError("trying to build index on a non referenced field.")

            if field.dtype == "JSON" and len(fname.split(".")) > 1:
                base, path = field.split(".")
                self.add_column(nodes_table, base, path)
                self.create_triggers(nodes_table, fname.replace(".","_"), path)
                self.create_index(nodes_table, fname.replace(".","_"))

            elif field.dtype == "JSONlist" and len(fname.split(".")) == 1:
                index_field = {
                    "value":Field(fname, field.dtype, pk=True),
                    "nid": Field("nid", "TEXT", pk=True, fk=f"{nodes_table.table_name}.id")
                }
                index_table = IndexTable.initialize(self.name, fname, "TEXT")
                self.tables[fname] = index_field
                self.create_table(index_table)
                self.create_index(index_table, fname)
                self.create_triggers(index_table, fname)

            else:
                self.create_index(nodes_table, fname)



    def _build_root(self, node: Payload) -> None:
        nodes_table = self.tables["nodes"].table_name
        metadata_table = self.tables["metadata"].table_name
        self.root_id = node["id"]
        self.write(nodes_table, list(node.keys()), list(node.values()))
        self.write(metadata_table, ["nid", "depth", "is_root", "is_leaf"], [node["id"], 0, True, False])

    def _build_nodes(
        self,
        batch: deque,
        parent2children: dict[str, list[_Nid]]
        ) -> tuple[deque, dict[str, list[_Nid]]]:

        nodes_table = self.tables["nodes"].table_name
        k, v = list(batch[0].keys()), []
        while len(batch) > 0:
            node = batch.popleft()
            children = parent2children.pop(node["id"], [])
            node.update({"children": children})
            v.append(tuple(node.values()))
        self.write_many(nodes_table, k, v, commit=True)
        return (batch, parent2children)

    def _add_remaining_children(self, parent2children: dict[str, list[_Nid]]) -> dict[str, list[_Nid]]:
        nodes_table = self.tables["nodes"].table_name
        remains = self.get_children_from_ids(nodes_table, list(parent2children.keys()))
        for node in remains:
            new_children = parent2children.pop(node["id"])
            children = node["children"] + new_children
            self._update(nodes_table, [("children", children)], [("id","=",node["id"])], commit=False)
        return parent2children

    def _build_metadata(self) -> None:
        nodes_table = self.tables["nodes"].table_name
        metadata_table = self.tables["metadata"].table_name
        root = self.get_children_from_id(nodes_table, self.root_id)
        current_layer, layers_size, queue, values = 1, defaultdict(int), deque(root["children"]), []
        layers_size[current_layer] += len(root["children"])
        while len(queue) > 0:
            nid = queue.popleft()
            children = self.get_children_from_id(nodes_table, nid)
            values.append(tuple((nid, current_layer, False, not any(children["children"]))))
            queue.extend(children["children"])

            layers_size[current_layer + 1] += len(children["children"])
            layers_size[current_layer] -= 1
            if layers_size[current_layer] == 0:
                current_layer += 1

            if len(values) == self.BATCH_SIZE:
                self.write_many(metadata_table, ["nid", "depth", "is_root", "is_leaf"], values)
                values = []
        if len(values) > 0:
            self.write_many(metadata_table, ["nid", "depth", "is_root", "is_leaf"], values)

    def _get_tables(self) -> None:
        self.tables = {}
        tables = self.get_tables(self.name)
        if len(tables) > 0:
            for table in tables:
                table_name = table[0]
                info = self.table_info(table_name)
                fk_info = self.table_fk_info(table_name)
                tkey = table_name.split("__")[1]
                self.tables[tkey] = Table.from_pragma(table_name, info, fk_info)
        elif self._model is None:
            raise ValueError("input either data or a data model")
        nodes_fields = {k:Field(k,v) for k,v in self._model.items() if k not in ["nid", "id", "parent", "children"]}
        self.tables["nodes"] = NodesTable.initialize(self.name, **nodes_fields)
        self.tables["metadata"] = MetadataTable.initialize(self.name)

    def _get_loader(self, data: list[Payload] | str | None, strategy: Loaders) -> Loader | None:
        if isinstance(data, str):
            loader = infer_loader(data)
            data = loader(data, strategy)
        elif isinstance(data, list):
            data = Loader(data)
        self.data = data
        return self.data

    def _get_model(self, model: Optional[dict[FieldName, _SqliteTypes]] | None):
        if self.data is None and model is None:
            self._model = None
        elif model is None:
            self._infer_model()
        else:
            self._model = model

    def _infer_model(self) -> dict[str,str]:
        def _infer_type(v: Any) -> str:
            dtype = None
            if isinstance(v, bool):
                dtype = "BOOL"
            elif isinstance(v, int):
                dtype = "INTEGER"
            elif isinstance(v, str):
                dtype = "TEXT"
            elif v is None:
                dtype = "NULL"
            elif isinstance(v, float):
                dtype = "REAL"
            elif isinstance(v, list):
                dtype = "JSONLIST"
            else:
                dtype = "JSON"
            return dtype

        model = {}
        for payload in self.data.loader():
            for field, value in payload.items():
                current_dtype = model.get(field, None)
                dtype = _infer_type(value)
                if current_dtype is None:
                    model[field] = dtype
                    continue
                elif dtype is None and current_dtype != "NULL":
                    continue
                elif current_dtype == "NULL" and dtype != "NULL":
                    model[field] = dtype
                    continue
                elif current_dtype != dtype:
                    raise ValueError("datatypes not the same all along the dataset")
        if "id" not in model.keys():
            raise KeyError("payload must have id field")
        if model.get("id") != "TEXT":
            raise ValueError("Id must be a string")
        model.update({"parent": model["id"], "children": "JSONLIST"})
        self._model = model



if __name__ == "__main__":
    tree = TreeBuilder.build_permanent_tree(name="topics", data="./tags/topics.jl", indexes=["id", "alias"])
    tree.show_tree()
