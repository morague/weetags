from pathlib import Path
from collections import defaultdict, deque

from typing import Any, Type, Literal, Iterator, Optional


from weetags.utils import infer_dtype, infer_loader
from weetags.tree import Tree
from weetags.loaders import Loader, JlLoader, JsonLoader
from weetags.engine.engine import TreeEngine
from weetags.engine.schema import SimpleSqlField, SimpleSqlTable
from weetags.engine.schema import NodesTable, MetadataTable, IndexTable 

StrOrPath = str | Path
DataLoader = list[Type[Loader|JlLoader|JsonLoader]] | None
Data = list[dict[str, Any]] | list[Type[Loader|JlLoader|JsonLoader]] | list[StrOrPath] | None
Loaders = Literal["default", "lazy"]

class TreeBuilder(TreeEngine):
    data: DataLoader = None
    model: dict[str, Any] | None = None
    BATCH_SIZE = 500
    root_id = None

    def __init__(
        self,
        tree_name: str,
        database: Optional[str] = ":memory:",
        data: Optional[Data] = None,
        **params: Optional[Any]
        ) -> None:

        super().__init__(tree_name, database, **params)
        self._set_loaders(data)
        self._infer_model(None)
        self._collect_tables()

    @classmethod
    def build_tree(
        cls,
        tree_name: str,
        database: Optional[str] = ":memory:",
        data: Optional[Data] = None,
        indexes: Optional[list[str]] = None,
        read_only: Optional[bool] = False,
        replace: Optional[bool] = False,
        **params: Any
    ) -> Tree:
        builder = cls(tree_name, database, data, **params)
        if (builder.data is None and not builder._get_tables(tree_name)) or (replace and not builder.data):
            raise ValueError("You must initialize the TreeBuilder with a data or a builded database.")
        
        if replace:
            builder.drop_tree()
        if not builder._get_tables(tree_name) or replace:
            builder.build_tree_tables()
            if indexes:
                builder.build_indexes(indexes)
            builder.populate_tree()
        return Tree(tree_name=tree_name, database=database, read_only=read_only, **params)


    @property
    def iter_data(self) -> Iterator:
        for source in self.data:
            for node in source.loader():
                yield node

    def drop_tree(self) -> None:
        tables = self._get_tables(self.tree_name)
        for table in tables:
            self._drop(table[0])
    
    def build_tree_tables(self) -> None:
        tables = [self.tables["nodes"], self.tables["metadata"]]
        self._create_tables(*tables)

    def build_indexes(self, indexes: list[str]) -> None:
        nodes_table = self.tables["nodes"]
        for fname in indexes:
            field = getattr(self.tables["nodes"], fname.split(".")[0], None)
            if field is None:
                raise ValueError(f"Building Index: field {fname} does not exist")            

            if field.dtype == "JSON":
                target = fname.split(".")[0]
                path_name = fname.replace(".", "_")

                index_table = IndexTable.initialize(self.tree_name, path_name, "TEXT")
                self.tables[path_name] = index_table
                self._create_tables(index_table)
                # self._create_index(index_table, fname)
                self._create_triggers(index_table, target, fname)

            elif field.dtype == "JSONLIST":
                index_table = IndexTable.initialize(self.tree_name, fname, "TEXT")
                self.tables[fname] = index_table
                self._create_tables(index_table)
                self._create_index(index_table, fname)
                self._create_triggers(index_table, fname)
            else:
                self._create_index(nodes_table, fname)

    def populate_tree(self) -> None:
        if self.data is None:
            return
        
        batch, parent2children = deque(), defaultdict(list)
        for node in self.iter_data:
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

    def _build_root(self, node: dict[str, Any]) -> None:
        nodes_table = self.tables["nodes"]._name
        metadata_table = self.tables["metadata"]._name
        self.root_id = node["id"]
        self._builder_write_many(nodes_table, list(node.keys()), [list(node.values())])
        self._builder_write_many(metadata_table, ["nid", "depth", "is_root", "is_leaf"], [[node["id"], 0, True, False]])

    def _build_nodes(
        self,
        batch: deque[dict[str, Any]],
        parent2children: dict[str, list[str]]
    ) -> tuple[deque, dict[str, list[str]]]:

        nodes_table = self.tables["nodes"]._name
        k, v = list(batch[0].keys()), []
        while len(batch) > 0:
            node = batch.popleft()
            children = parent2children.pop(node["id"], [])
            node.update({"children": children})
            v.append(tuple(node.values()))
        self._builder_write_many(nodes_table, k, v, False)
        return (batch, parent2children)

    def _add_remaining_children(
        self,
        parent2children: dict[str, list[str]]
    ) -> dict[str, list[str]]:
        nodes_table = self.tables["nodes"]._name
        remains = self._get_children_from_ids(nodes_table, list(parent2children.keys()))
        for node in remains:
            new_children = parent2children.pop(node["id"])
            children = node["children"] + new_children
            self._builder_update(nodes_table, [("children", children)], [("id","=",node["id"])], commit=False)
        return parent2children

    def _build_metadata(self) -> None:
        nodes_table = self.tables["nodes"]._name
        metadata_table = self.tables["metadata"]._name
        root = self._get_children_from_id(nodes_table, self.root_id)
        current_layer, layers_size, queue, values = 1, defaultdict(int), deque(root["children"]), []
        layers_size[current_layer] += len(root["children"])
        while len(queue) > 0:
            nid = queue.popleft()
            children = self._get_children_from_id(nodes_table, nid)
            values.append(tuple((nid, current_layer, False, not any(children["children"]))))
            queue.extend(children["children"])

            layers_size[current_layer + 1] += len(children["children"])
            layers_size[current_layer] -= 1
            if layers_size[current_layer] == 0:
                current_layer += 1

            if len(values) == self.BATCH_SIZE:
                self._builder_write_many(metadata_table, ["nid", "depth", "is_root", "is_leaf"], values)
                values = []
        if len(values) > 0:
            self._builder_write_many(metadata_table, ["nid", "depth", "is_root", "is_leaf"], values)

    def _collect_tables(self) -> None:
        self.tables = {}
        tables = self._get_tables(self.tree_name)
        if len(tables) > 0:
            for table in tables:
                table_name = table[0]
                info = self._table_info(table_name)
                fk_info = self._table_fk_info(table_name)
                tkey = table_name.split("__")[1]    
                self.tables[tkey] = SimpleSqlTable.from_pragma(table_name, info, fk_info)
        elif self.model is None:
            raise ValueError("Input data files or a database into the TreeBuilder")
        else:
            nodes_fields = {k:SimpleSqlField(k,v) for k,v in self.model.items() if k not in ["nid", "id", "parent", "children"]}
            self.tables["nodes"] = NodesTable.initialize(self.tree_name, **nodes_fields)
            self.tables["metadata"] = MetadataTable.initialize(self.tree_name)

    def _set_loaders(self, data: Data, strategy: Loaders= "lazy") -> None:
        if data is None:
            return
        elif not isinstance(data, list):
            raise TypeError("TreeBuilder.data must be a list.")
        elif isinstance(data, list) and len(data) == 0:
            raise ValueError(f"TreeBuilder.data must contain at least one record or Filepath")
        
        self.data = []
        for d in data:
            if isinstance(d, dict):
                self.data.append(Loader(data))
                break
            elif isinstance(d, str):
                loader = infer_loader(d)
                self.data.append(loader(d, strategy))
            elif type(d) in [Loader, JlLoader, JsonLoader]:
                self.data.append(d)
            else:
                raise ValueError("data must be of type list[dict[str, Any]] | list[str] | None")
        
    def _infer_model(self, model: dict[str, Any] | None) -> None:
        if model is not None:
            return

        model = {}
        for record in self.iter_data:
            if "id" not in record.keys():
                raise KeyError("Data records must have an id field")
            if "parent" not in record.keys():
                raise KeyError("Data records must have a parent field.")

            for field, value in record.items():
                current_dtype = model.get(field, None)
                dtype = infer_dtype(value)
                if current_dtype is None:
                    model[field] = dtype
                elif dtype is None and current_dtype != "NULL":
                    continue
                elif current_dtype == "NULL" and dtype != "NULL":
                    model[field] = dtype
                elif current_dtype != dtype and dtype != "NULL":
                    raise ValueError(f"field `{field}` dtype is not consistent over the dataset.")
                    
        if model.get("id") != "TEXT":
            raise ValueError("Id must be a string")
        self.model = model