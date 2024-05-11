
from collections import defaultdict, deque
from typing import Any, TypeVar, Optional, Literal

from weetags.trees.db import Db
from weetags.trees.permanent_tree import PermanentTree

from weetags.utils import JlLoader, JsonLoader, DataWrapper
from weetags.utils import infer_loader

Payload = dict[str, Any]
Path = TreeName = TableName = FieldName = str
_Nid = TypeVar("_Nid", str, int)
_SqliteTypes = TypeVar("_SqliteTypes", str, int, dict, list, bytes)
Loaders = Literal["default", "lazy"]


from time import perf_counter


class PermanentTreeBuilder(Db):
    BATCH_SIZE = 500
    root_id = None    

    def __init__(
        self, 
        name: TreeName,
        path: str = "db.db", 
        permanent: bool = True
        ) -> None:
        super().__init__(path, permanent=permanent)
        self.name = name
        self.tables = self._base_tables(self.name)
    
    @classmethod
    def build_permanent_tree(
        cls,
        *,
        name: TreeName,
        data: list[Payload] | Path,
        path: Path = "db.db",
        indexes: Optional[list[FieldName]] = None,
        model: Optional[dict[FieldName, _SqliteTypes]] = None,
        strategy: Loaders = "lazy",
        permanent: bool = True,
        read_only: bool = False,
        replace: bool = False
    ) -> PermanentTree:
        
        builder = cls(name = name, path = path, permanent=permanent)
        builder._base_tables(name)
        if isinstance(data, str):
            loader = infer_loader(data)
            data = loader(data, strategy)
        else:
            data = DataWrapper(data) 
        builder.data = data
            
        if model is None:
            builder._infer_model()
        else:
            builder.model = model

        if replace:
            builder.drop_tree()
        if not builder._get_tables(name) or replace:        
            builder.build_node_table()
            builder.build_metadata_table()
            # intermediarytree = PermanentTree(name=name, db=path, permanent=permanent)
            # builder.build_tree(intermediarytree, data)
            builder.opt_build_tree()
            if indexes:
                builder.build_indexes(indexes)
        return PermanentTree(name=name, db=path, read_only=read_only, permanent=permanent)
        
    def drop_tree(self) -> None:
        tables = self._get_tables(self.name)
        for table in tables:
            self._drop(table[0])

    def build_tree(self, tree: PermanentTree, data: list[Payload]) -> None:
        for node in data.loader():
            if node.get("children", None) is None:
                node.update({"children":[]})
            tree.add_node(node)
            
    def opt_build_tree(self) -> None:
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

    def _build_root(self, node: Payload) -> None:
        self.root_id = node["id"]
        self._write(self.tables["nodes"], list(node.keys()), list(node.values()))   
        self._write(self.tables["metadata"], ["nid", "depth", "is_root", "is_leaf"], [node["id"], 0, True, False])

    def _build_nodes(
        self, 
        batch: deque, 
        parent2children: dict[str, list[_Nid]]
        ) -> tuple[deque, dict[str, list[_Nid]]]:
        
        k, v = list(batch[0].keys()), []
        while len(batch) > 0:
            n = batch.popleft()
            children = parent2children.pop(n["id"], [])
            n.update({"children": children})
            v.append(tuple(n.values()))
        self._write_many(self.tables["nodes"], k, v, no_commit=False)
        return (batch, parent2children)

    def _add_remaining_children(self, parent2children: dict[str, list[_Nid]]) -> dict[str, list[_Nid]]:
        anchors = ", ".join(["?" for _ in range(len(parent2children.keys()))])
        remains = self.con.execute(f"SELECT id, children FROM {self.tables['nodes']} WHERE id IN ({anchors});",list(parent2children.keys())).fetchall()
        for n in remains:
            new_children = parent2children.pop(n["id"])
            children = n["children"] + new_children
            self._update(self.tables["nodes"], [("children", children)], [("id","=",n["id"])], no_commit=True)
        return parent2children

    def _build_metadata(self) -> None:
        query = "SELECT n.children FROM {nodes} AS n WHERE n.id = ?;"
        root = self.con.execute(query.format(nodes=self.tables["nodes"]), [self.root_id]).fetchone()
        current_layer, layers_size, queue, values = 1, defaultdict(int), deque(root["children"]), []
        layers_size[current_layer] += len(root["children"])
        while len(queue) > 0:
            nid = queue.popleft()
            children = self.con.execute(query.format(nodes=self.tables["nodes"]), [nid]).fetchone()
            values.append(tuple((nid, current_layer, False, not any(children["children"]))))
            queue.extend(children["children"])
            
            layers_size[current_layer + 1] += len(children["children"])
            layers_size[current_layer] -= 1
            if layers_size[current_layer] == 0:
                current_layer += 1
                
            if len(values) == self.BATCH_SIZE:
                self._write_many(self.tables["metadata"], ["nid", "depth", "is_root", "is_leaf"], values)
                values = []
        if len(values) > 0:
            self._write_many(self.tables["metadata"], ["nid", "depth", "is_root", "is_leaf"], values)
            
        
    def build_indexes(self, indexes: list[FieldName]) -> None:
        for field in indexes:
            _type = self.model.get(field, None)
            if _type is None:
                raise ValueError("trying to build index on a non referenced field.")
    
            if _type == "JSON" and len(field.split(".")) > 1:
                self._index_serialized_field_with_path(field)
            elif _type == "JSON" and len(field.split(".")) == 1:
                self._index_serialized_field(field)
            else:
                self._index(f"{self.name}__nodes", field)

    def build_node_table(self) -> None:
        self._table(self.tables.get("nodes"), self.model, pk=["id"])
        
    def build_metadata_table(self) -> None:
        model_metadata = {"nid": self.model["id"], "depth": "INTEGER", "is_root": "BOOL", "is_leaf": "BOOL"} 
        self._table(self.tables.get("metadata"), model_metadata,  pk=["nid"], fk=[("nid", "nodes", "id")])

    def _base_tables(self, tree_name: str) -> dict[str, str]:
        self.tables = {"nodes": f"{tree_name}__nodes", "metadata": f"{tree_name}__metadata"}

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
        model.update({"parent": model["id"], "children": "JSON"})
        self.model = model

    def _index_serialized_field(self, field: FieldName) -> None:
        nodes_table = self.tables["nodes"]
        data = self.cursor.execute(f"SELECT value, {nodes_table}.id, key FROM {nodes_table}, json_each({field});").fetchall()                      
        model = {field: "TEXT", "nid": self.model["id"], "elm_idx": "INTEGER"}
        columns = [field, "nid", "elm_idx"]
        fk = [("nid", nodes_table, "id")]  
        
        self.tables[field] = f"{self.name}__{field}" 
        self._table(
            f"{self.name}__{field}", 
            model,
            columns,
            fk
        )
        
        self._write_many(f"{self.name}__{field}", columns, data)
        self._index(f"{self.name}__{field}", field)
        self._insert_trigger_on_array(f"{self.name}__{field}_insert_trigger", nodes_table, self.tables[field], field)
        self._delete_trigger_on_array(f"{self.name}__{field}_delete_trigger", nodes_table, self.tables[field])
        self._update_trigger_on_array(f"{self.name}__{field}_update_trigger", nodes_table, self.tables[field], field)     

    
    def _index_serialized_field_with_path(self, field: FieldName) -> None:
        nodes_table = self.tables["nodes"]
        layers = field.split(".")
        base, fname, path = layers[0], layers[-1], '.'.join(layers[1:])
        self._col_gen_from_json(nodes_table, fname, base, path)
        
        self._index(nodes_table, fname)
        self._insert_trigger_on_object(f"{self.name}__{field}_insert_trigger", nodes_table, fname, path)
        self._update_trigger_on_object(f"{self.name}__{field}_update_trigger", nodes_table, fname, path)





    
    
    
    
    


if __name__ == "__main__":
    # import copy
    # import json    
    # from collections import OrderedDict
    # d = []
    # with open("/home/morague/Documents/github/CG/weetags/tags/admWgeo_v6.json", "r") as f:
    #     data = json.load(f)
    #     for node in data:
    #         if node.get("_parent", None) is None:
    #             node.update({"parent": None})
    #         else:
    #             v = node.pop("_parent")
    #             node.update({"parent": v})     
    #         node.pop("_id")
    #         if node.get("name_deu", None) is None:
    #             node.update({"name_deu": None})
    #         if node.get("name_urk", None):
    #             v = node.pop("name_urk", None)
    #             node.update({"name_ukr": v})
    # for node in data:        
    #     dd = OrderedDict()
    #     for field in ['id', 'name_eng', 'name_ukr', 'name_rus', 'alias', 'area', 'geometry', 'wkid', 'parent', 'name_deu']:
    #         dd[field] = node[field]

    #     d.append(dd)

    # with open("./tags/locations.jl", 'w+') as f:
    #     for node in d:
    #         f.write(json.dumps(node) + "\n")


    

    tree = PermanentTreeBuilder.build_permanent_tree(name="locations", data="./tags/locations.jl", replace=True) # 
    # tree.show_tree()
    

