from __future__ import annotations

from collections import deque, defaultdict
from itertools import chain
from typing import Literal, TypeVar, Optional, Any, Callable

from weetags.trees.db import Db

 
Payload = dict[str, Any]
Style = Literal["ascii", "ascii-ex", "ascii-exr", "ascii-emh", "ascii-emv", "ascii-em"]
Path = TreeName = TableName = FieldName = Operator = str
_Nid = TypeVar("_Nid", str, int)
_SqliteTypes = TypeVar("_SqliteTypes", str, int, dict, list, bytes)


class NameSpace(object):
    table: TableName = "nodes"
    index_table: TableName = "indexednodes"
    fname: FieldName = "id"
    ftype: _SqliteTypes = "TEXT"
    
    def __init__(
        self, 
        table: TableName, 
        index_table: TableName,
        fname: FieldName, 
        ftype: _SqliteTypes
        ) -> None:
        
        self.table = table
        self.index_table = index_table
        self.fname = fname
        self.ftype = ftype
    
    def __repr__(self) -> str:
        return f"Namespace(table: {self.table}, index_table: {self.index_table}, fname: {self.fname}, ftype: {self.ftype})"
    
    def join(self, to_table: TableName) -> str:
        if self.index_table.endswith("nodes"):
            raise KeyError("Nodes table must be main table and not joined one.")
        return f"JOIN {self.index_table} ON {to_table}.id = {self.index_table}.nid"
    
    def select(self) -> str:
        return f"{self.table}.{self.fname}"
    
    def where(self, op: str, value: Any) -> str:
        def anchor(op: str) -> str:
            a = "?"
            if "in" in op.lower():
                a = "(?)"
            return a
        return (f"{self.index_table}.{self.fname} {op} {anchor(op)}", value)
        
    def to_join(self) -> bool:
        return self.table.split("__")[1] != "nodes"
    
    def is_metadata(self) -> bool:
        return self.table.split("__")[1] == "metadata"



class PermanentTree(Db):
    """
    
    
    
    """
    
    def __init__(
        self,
        *,
        name: TreeName,
        db: Path = "db.db",
        permanent: bool = True,
        read_only: bool = False,
        auto_rm_oprhans: bool = True
        ) -> None:
        
        super().__init__(path=db, permanent=True, read_only=read_only)
        self.name = name
        self.orphan_rm = auto_rm_oprhans
        self._map_tables()
        self._load_models()
        self._map_namespace()

        self.root_id = None
        if self.tree_size() > 0:
            self.root_id = self.root()

    def tree_size(self) -> int:
        nodes = self.tables["nodes"]
        return self._table_size(nodes)
    
    def tree_depth(self) -> int:
        metadata = self.tables["metadata"]
        return self._max_depth(metadata)
    
    
    # -- GETTER
    def root(self) -> Any:
        node_table = self.tables["nodes"] 
        s = self.namespace['id'].select()
        j = self.namespace['depth'].join(self.tables["nodes"])
        w, v = self.namespace['depth'].where("=", 0)
        return self.cursor.execute(f"SELECT {s} FROM {node_table} {j} WHERE {w}", [v]).fetchone()[0]
    
    
    def node(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> Payload:
        node_table = self.tables["nodes"]
        f = self._parse_fields(fields)
        j = self.namespace['depth'].join(self.tables["nodes"])
        return self.con.execute(f"SELECT {f} FROM {node_table} {j} WHERE id = ?;", [nid]).fetchone()
        
    def nodes_where(
        self,
        conds: list[tuple[FieldName,str, Any]], 
        fields: list[FieldName] = ["*"],
        order: Optional[list[FieldName]] | None = [],
        axis: int = 1,
        limit: Optional[int] = None
        ) -> Payload:
        node_table = self.tables["nodes"]
        
        f = self._parse_fields(fields)
        j = self._parse_join(fields, conds, order)
        w, v = self._parse_where(conds)
        o = self._parse_order(order, axis)
        l = self._parse_limit(limit)
        return self.con.execute(f"SELECT {f} FROM {node_table} {j} WHERE {w} {o} {l};", v).fetchall()
    
    def nodes_relation_where(
        self,
        conds: list[tuple[FieldName,str, Any]], 
        relation: Callable,
        fields: list[FieldName] = ["*"],
        order: Optional[list[FieldName]] | None = [],
        axis: int = 1,
        limit: Optional[int] = None,
        include_base: bool = False
        ) -> list[Payload]:
        nodes = self.nodes_where(conds, list(set(["id"] + fields)), order, axis, limit)
        if include_base:
            return [[n] + relation(n["id"], fields) for n in nodes]
        else:
            return [relation(n["id"], fields) for n in nodes]
        
    
    def parent_node(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> Payload:
        node = self.node(nid, ["id","parent"])
        return self.node(node["parent"], fields)
    
    def children_nodes(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> list[Payload]:
        node = self.node(nid, ["id","children"])        
        return [self.node(c, fields) for c in node["children"]]
    
    def siblings_nodes(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> list[Payload]:
        node = self.node(nid, ["id","parent"])        
        pnode = self.node(node["parent"], ["children"])
        return [self.node(c, fields) for c in pnode["children"] if c != nid]
        
        
    def ancestors_nodes(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> list[Payload]:
        ancestors = []
        node = self.node(nid, ["id","parent"])
        while node["parent"]:
            node = self.node(node["parent"], fields)
            ancestors.append(node)
        return ancestors
            
    def descendants_nodes(self, nid: _Nid, fields: list[FieldName] = ["*"]) -> list[Payload]:
        node = self.node(nid, ["id","children"])
        descendants, queue = [], deque(node["children"])
        while len(queue) > 0:
            cid = queue.pop()
            node = self.node(cid, fields)
            children = self.node(cid, ["children"])
            queue.extendleft(children["children"]) # weird to do that, have to do 2 I/O operations
            descendants.append(node)
        return descendants
            
    def orphans_nodes(
        self, 
        fields: list[FieldName] = ["*"], 
        order: Optional[list[FieldName]] | None = [],
        axis: int = 1,
        limit: Optional[int] = None
        ) -> list[Payload]:
        orphans = self.nodes_where([("parent","is", None)], fields, order, axis, limit)
        for i in range(len(orphans)):
            if orphans[i]["id"] == self.root_id:
                orphans.pop(i)
                break
        return orphans
    
    # -- SETTER
    def add_node(self, node: Payload) -> None:
        pid = node.get("parent", None)
        if pid is None and self.root_id is not None:
            raise ValueError("tree can only have one root")
        
        elif pid is None:
            self._add_node(node, 0, True, True)

        else:        
            pnode = self._add_children(pid, node["id"])
            self._add_node(node, pnode["depth"] + 1)

    def update_node(self, nid: _Nid, set_values:list[tuple[FieldName, _SqliteTypes]]) -> None:
        nodes_table = self.tables["nodes"]
        meta_table = self.tables["metadata"]
        
        queries = defaultdict(list)
        for fname, v in set_values:
            tn = self.namespace[fname].table
            queries[tn].append(tuple((fname, v)))
        
        for tn, setv in queries.items():
            s, v = self._parse_set(setv)
            if tn == nodes_table:
                w, wv = self._parse_update_where([("id", "=", nid)])
            else:
                w, wv = self._parse_update_where([("nid", "=", nid)])
            print(f"UPDATE {tn} SET {s} WHERE {w};")
            self.cursor.execute(f"UPDATE {tn} SET {s} WHERE {w};", v + wv)
        self.con.commit()
        
    def update_nodes_where(
        self, 
        set_values: list[tuple[FieldName, _SqliteTypes]], 
        conds: list[tuple[FieldName, str, Any]]
        ) -> None:
        nodes_table = self.tables["nodes"]
        meta_table = self.tables["metadata"]
        ids = [n["id"] for n in self.nodes_where(conds, ["id"])]
        queries = defaultdict(list)
        for fname, v in set_values:
            tn = self.namespace[fname].table
            queries[tn].append(tuple((fname, v)))

        for tn, setv in queries.items():
            s, v = self._parse_set(setv)
            if tn == nodes_table:
                w, wv = self._parse_update_where([("id", "=", ids)])
            else:
                w, wv = self._parse_update_where([("nid", "=", ids)])
            values = [v + [val] for val in ids]
            self.cursor.executemany(f"UPDATE {tn} SET {s} WHERE {w};", values)
        self.con.commit()
        
    def delete_node(self, nid: _Nid) -> None:
        self._delete_node(nid)
        if self.orphan_rm:
            self.delete_dead_branches()
        
    def delete_nodes_where(self, conds: list[tuple[FieldName, str, Any]]) -> None:
        nodes = self.nodes_where(conds, ["id"])
        for n in nodes:
            nid = n["id"]
            if nid == self.root_id:
                raise ValueError("cannot delete root node")
            self._delete_node(nid)    
        if self.orphan_rm:
            self.delete_dead_branches()
        
    def delete_dead_branches(self) -> None:
        nodes_table = self.tables["nodes"]
        orphans = self.orphans_nodes(["id"])
        nodes = chain.from_iterable([[o] + self.descendants_nodes(o["id"], ["id"]) for o in orphans])
        [self._delete(nodes_table, [("id","=", node["id"])]) for node in nodes]

    def delete_orphans(self, nid: _Nid):
        nodes_table = self.tables["nodes"]
        orphans = self.orphans_nodes(["id"])
        [self._delete(nodes_table, [("id","=", o["id"])]) for o in orphans]

    
    
    # UTILS
    def draw_tree(self, nid: Optional[_Nid] | None = None, style:Style="ascii-ex") -> str:
        dt = {
            "ascii": ("|", "|-- ", "+-- "),
            "ascii-ex": ("\u2502", "\u251c\u2500\u2500 ", "\u2514\u2500\u2500 "),
            "ascii-exr": ("\u2502", "\u251c\u2500\u2500 ", "\u2570\u2500\u2500 "),
            "ascii-em": ("\u2551", "\u2560\u2550\u2550 ", "\u255a\u2550\u2550 "),
            "ascii-emv": ("\u2551", "\u255f\u2500\u2500 ", "\u2559\u2500\u2500 "),
            "ascii-emh": ("\u2502", "\u255e\u2550\u2550 ", "\u2558\u2550\u2550 "),
        }[style]

        if nid is None:
            nid = self.root_id

        root = self.node(nid, ["id", "parent", "children", "depth", "is_leaf"])
        if root["is_leaf"]:
            tree = f"{dt[2]}{root['id']}"
            return tree
        
        tree = f"{root['id']}\n"
        
        INITIAL_DEPTH = root["depth"]
        BLOCK_SIZE = 2
        INDENTATION = 2
                
        def _draw(tree: str, queue: deque):
            seen = set()
            while len(queue) > 0:
                nid = queue.popleft()
                node = self.node(nid, ["id", "parent", "children", "depth", "is_leaf"])
                layer = node["depth"] - INITIAL_DEPTH - 1
                
                if nid not in seen and len(node["children"]) == 0:
                    seen.add(nid)
                    queue.append(nid)
                    continue
                
                # extra space all
                # tree += " " * INDENTATION + (dt[0] + " " * BLOCK_SIZE) * layer + dt[0] + "\n"
                # if node["is_leaf"] != 1:
                #     tree += " " * INDENTATION + (dt[0] + " " * BLOCK_SIZE) * layer + dt[0] + "\n"
                    
                if len(queue) > 0:
                    space = " " * INDENTATION + (dt[0] + " " * BLOCK_SIZE) * layer
                    tree += f"{space}{dt[1]}{node['id']}\n"
                    
                else:
                    space = " " * INDENTATION + (dt[0] + " " * BLOCK_SIZE) * layer
                    tree += f"{space}{dt[2]}{node['id']}\n"   
                                                 
                tree = _draw(tree, deque(node["children"]))
            return tree        
        return _draw(tree, deque(root["children"]))
        
    def show_tree(self, nid: Optional[_Nid] | None= None, style:Style="ascii-ex") -> None:
        tree = self.draw_tree(nid, style)
        print(tree)

    # _
    def _add_node(self, node: Payload, depth: int=0, is_root: bool= False, is_leaf: bool=True) -> None:
        nodes_table = self.tables["nodes"]
        meta_table = self.tables["metadata"]        
        self._write(nodes_table, list(node.keys()), list(node.values()))   
        self._write(meta_table, ["nid", "depth", "is_root", "is_leaf"], [node["id"], depth, is_root, is_leaf])
            
    def _add_children(self, nid: _Nid, cnid: _Nid) -> Payload:
        nodes_table = self.tables["nodes"]
        meta_table = self.tables["metadata"]
        pnode = self.node(nid, ["id", "depth", "children"])
        
        assert(pnode is not None)
        
        pnode.update({"children": list((set(pnode["children"] + [cnid])))})
        self._update(nodes_table, [("children", pnode["children"])], [("id", "=", nid)])
        self._update(meta_table, [("is_leaf", False)], [("nid","=", pnode["id"])])
        return pnode
    
    def _delete_node(self, nid: _Nid) -> None:
        nodes_table = self.tables["nodes"]
        if nid == self.root_id:
            raise ValueError("cannot delete root node")
        
        node = self.node(nid, ["children","parent"])
        [self._remove_parent(c) for c in node["children"]]
        self._remove_children(node["parent"], nid)
        self._delete(nodes_table, [("id", "=", nid)])

    def _remove_children(self, nid: _Nid, cnid):
        nodes_table = self.tables["nodes"]
        node = self.node(nid, ["id", "children"])
        node["children"].remove(cnid)
        self._update(nodes_table, [("children", node["children"])], [("id", "=", node["id"])]) 
        
    def _remove_parent(self, nid: _Nid):
        nodes_table = self.tables["nodes"]
        node = self.node(nid, ["id"])
        self._update(nodes_table, [("parent", None)], [("id", "=", node["id"])]) 
        
        
    # builders
    def _map_tables(self) -> dict[str, TableName]:
        tables = [t[0] for t in self._get_tables(self.name)]
        if len(tables) == 0:
            raise ValueError("No tables found for this tree")
        self.tables = {t.split('__')[1]:t for t in tables}
        
    def _load_models(self) -> dict[FieldName, _SqliteTypes]:
        self.models = {n:{f[1]:f[2] for f in self._table_info(t)} for n,t in self.tables.items()}
    
    def _map_namespace(self) -> dict[str, NameSpace]:
        names_space = {}
        for tk, model in self.models.items():
            table = self.tables[tk]
            tkey = table.split("__")[1]
            
            for fname, ftype in model.items():
                namespace = names_space.get(fname, None)
                
                if fname in ("nid", "elm_idx"):
                    continue
                
                if namespace is None:
                    names_space[fname] = NameSpace(
                        table=table,
                        index_table=table,
                        fname=fname,
                        ftype=ftype,
                    )
                    
                elif namespace and tkey == fname:
                    namespace.index_table = table
        self.namespace = names_space
    
    

        
if __name__ == "__main__":
    tree = PermanentTree(name = "topics")
    # res = tree.nodes_where(
    #     conds=[("alias","=","covid-19")],
    #     fields=["id", "alias", "depth"],
    #     order=["depth"],
    #     axis=1,
    #     limit=10)

    # r = tree.node("Healthcare")
    # print(r)
    # print(type(r["alias"]))
    
    
    # node = {
    #     'id': 'test', 
    #     'name_eng': 'testing', 
    #     'name_ukr': "small_test", 
    #     'parent': 'Healthcare', 
    #     'alias': json.dumps(["test1","test2"]),
    #     'children': json.dumps([])
    # }
    # tree.add_node(node)
    
    # print(tree.node("Healthcare", ["id", "children"]))
    # tree.delete_node("Healthcare")
    # print(tree.orphans_nodes())
    
    # tree.update_node("topicsRoot", [("alias", ["rooty", "rooty2", "rooty3"]), ("name_ukr", "ukr_topics"), ("is_leaf", True)])
    # tree.update_nodes_where([("name_eng", "testing!!!")], [("depth","=",1)])
    
    
    # tree.show_tree()
    

        
        
    