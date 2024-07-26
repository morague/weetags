import json
from sqlite3 import Cursor, Row
from sqlite3 import connect, register_adapter, register_converter
from sqlite3 import PARSE_DECLTYPES

from typing import Any, Optional, TypeVar, Literal

from weetags.database.namespace import NameSpace
from weetags.database.table import Table

Payload = dict[str, Any]
TableName = FieldName = Operator =  str
_Nid = TypeVar("_Nid", str, int)
_SqliteTypes = TypeVar("_SqliteTypes", str, int, dict, list, bytes)

TriggerActions = Literal["INSERT", "DELETE", "UPDATE"]
JsonType = Literal["object", "array"]
Modes = Literal["ro", "rw", "rwc"]

class _Db(object):
    """
    """    
    tables: dict[str, Table]
    namespace: dict[str, NameSpace]

    def __init__(
        self, 
        path: Optional[str] | None = "db.db",
        permanent: bool = True,
        tree_name: Optional[str] | None = None,
        **params
        ) -> None:
        
        self.permanent = permanent
        self._build_uri(path, permanent, **params)
        self.con = connect(self.uri, detect_types=PARSE_DECLTYPES, uri=True)
        self.cursor = self.con.cursor()
        self.con.row_factory = self._record_factory
        
        register_adapter(list, self.serialize)
        register_adapter(dict, self.serialize)
        register_converter("JSON", self.deserialize)
        
        if tree_name is not None:
            # if a tree name is referenced, _Db instance builds up a table & namespace representation.
            self._reference_tables(tree_name)
            self._reference_namespaces()

    @property
    def uri(self):
        return self.__uri


    def create_table(self, *tables: Table) -> None:
        for table in tables:
            query = table.create()
            self.cursor.execute(query)
        self.con.commit()
        
    def create_index(self, table: Table, field_name: str) -> None:
        query = table.add_index(field_name)
        self.cursor.execute(query)
        self.con.commit()
    
    def create_triggers(self, table: Table, target_field: str,  path: Optional[str] | None = None) -> None:
        nodes_table = self.tables["nodes"]
        self.cursor.execute(table.add_insert_trigger(target_field, nodes_table.table_name, path))
        self.cursor.execute(table.add_update_trigger(target_field, nodes_table.table_name, path))
        if path is None:
            self.cursor.execute(table.add_delete_trigger(nodes_table.table_name))
        self.con.commit()

    def add_column(self, table: Table, target_field: str, path: str) -> None:
        self.cursor.execute(table.add_indexing_column(target_field, path))
        self.con.commit()
    
    def write(self, table_name: TableName, column_names: list[FieldName], values: list[_SqliteTypes], commit: bool = True) -> None:
        anchors = self.anchors(values)
        col_names = ' ,'.join(column_names)
        self.cursor.execute(f"INSERT OR IGNORE INTO {table_name}({col_names}) VALUES({anchors});", values)
        if commit:
            self.con.commit()
    
    def write_many(self, table_name: TableName, column_names: list[FieldName], values: list[list[_SqliteTypes]], commit: bool = True) -> None:
        anchors = self.anchors(values[0]) # consider all values are the same size
        col_names = ' ,'.join(column_names)
        self.cursor.executemany(f"INSERT OR IGNORE INTO {table_name}({col_names}) VALUES({anchors});", values)
        if commit:
            self.con.commit()
            
    def delete(self, conds:list[tuple[FieldName, str, Any]] | None = None, commit: bool= True) -> None:
        query, values = ArgumentParser.delete_to_sql(self.tables, self.namespace, conds)
        print(query, values)
        self.cursor.execute(query, values)
        if commit:
            self.con.commit()
            
    def _update(self, table_name: TableName, set: list[tuple[FieldName, Any]], conditions: list[tuple[str, str, Any]] | None = None, commit: bool = True):
        """primitive update that doesn't needs the tree to be fully builded to be used."""
        fields, setvalues = self.update_setter(set)
        conds, values = self.conditions(conditions)
        self.cursor.execute(f"UPDATE {table_name} SET {fields} WHERE {conds};", setvalues + values)
        if commit is False:
            self.con.commit()
            
    def update(self, set_values: list[tuple[FieldName, Any]], conds: list[tuple[str, str, Any]] | None = None, commit: bool = True):
        query, values = ArgumentParser.update_to_sql(self.tables, self.namespace, set_values, conds)
        print(query, values)
        self.cursor.execute(query, values)
        if commit is False:
            self.con.commit()
    
    def read_one(self, fields: list[FieldName] = ["*"], conds: list[tuple[FieldName, Operator, Any]] = None) -> Payload:
        query, values = ArgumentParser.read_to_sql(self.tables, self.namespace, fields=fields, conds=conds)
        print(query, values)
        return self.con.execute(query, values).fetchone()
    
    def read_many(
        self, 
        fields: list[FieldName] = "*", 
        conds: list[tuple[FieldName, Operator, Any]] = None,
        order: list[FieldName] | None = None,
        axis: int = 1,
        limit: int | None = None
        ) -> list[Payload]:
        query, values = ArgumentParser.read_to_sql(self.tables, self.namespace, fields, conds, order, axis, limit)
        return self.con.execute(query, values).fetchall()
    
    def drop(self, table_name: TableName) -> None:
        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        self.con.commit()
    
    def table_info(self, table_name: TableName) -> list[tuple]:
        return self.cursor.execute(f"PRAGMA table_info({table_name});").fetchall()

    def table_fk_info(self, table_name: TableName) -> list[tuple]:
        return self.cursor.execute(f"PRAGMA foreign_key_list({table_name});").fetchall()

    def table_size(self, table_name: TableName) -> int:
        return self.cursor.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]

    def max_depth(self, table_name: TableName) -> int:
        return self.cursor.execute(f"SELECT MAX(depth) FROM {table_name};").fetchone()[0]
    
    def get_tables(self, name: str) -> list[TableName]:
        return self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{name}__%';").fetchall()


    # PREBACKED
    def get_children_from_ids(self, nodes_table: str, ids: list[str]) -> list[tuple[str, list[str]]]:
        """returns list of nid, children pairs"""
        anchors = self.anchors(ids)
        return self.con.execute(f"SELECT id, children FROM {nodes_table} WHERE id IN ({anchors});", ids).fetchall()
    
    def get_children_from_id(self, nodes_table: str, id: str):
        """return children"""
        return self.con.execute(f"SELECT id, children FROM {nodes_table} WHERE id=?;", [id]).fetchone()
    
    

    def _build_uri(
        self, 
        path: Optional[str] | None = "db.db",
        permanent: bool = True,
        **params
        ) -> str:
        base = "file:"
        if path is None and permanent is True:
            ValueError("Select either memory or parma")
        elif permanent is False:
            base += ":memory:"
            params.update({"cache":"shared"})
        else:
            base += path
        
        options = "&".join([f"{k}={v}" for k,v in params.items()])
        if options:
            base += f"?{options}"
        self.__uri = base



    def update_setter(self, set: list[tuple[FieldName, Any]]) -> tuple[str, Any]:
        values, fields = [], []
        [(fields.append(f"{field} = ?"), values.append(val)) for field, val in set]
        return (", ".join(fields), values)
            
    def conditions(self, conditions:list[tuple[FieldName, str, Any]]) -> tuple[str, Any]:
        values, conds = [], []
        for field, op, val in conditions:
            if op.lower() == "in":
                anchors = f"({self.anchors(val)})"
            else:
                anchors = "?"
            conds.append(f"{field} {op} {anchors}")
            values.append(val)
        return (' AND '.join(conds), values)

    def _reference_tables(self, name: str) -> None:
        self.tables = {}
        tables = self.get_tables(name)
        if len(tables) > 0:
            for table in tables:
                table_name = table[0]
                info = self.table_info(table_name)
                fk_info = self.table_fk_info(table_name)
                tkey = table_name.split("__")[1]
                self.tables[tkey] = Table.from_pragma(table_name, info, fk_info)
        elif len(tables) == 0 and len(self.tables.keys()) == 0:
            raise ValueError("Use the TreeBuilder object to build a tree.")
        
    def _reference_namespaces(self) -> None:
        self.namespace = {}
        for table_name, table in self.tables.items():
            for fname, field in table.fields():
                space = self.namespace.get(fname, None)
                if table_name not in ["metadata", "nodes"] and fname in ("nid", "elm_idx"):
                    continue
                elif space is None:
                    self.namespace[fname] = NameSpace(
                        table=table.table_name,
                        index_table=table.table_name,
                        fname=fname,
                        ftype=field.dtype,
                    )
                else:
                    space.index_table = table.table_name

    @staticmethod
    def anchors(values: list[Any]) -> str:
        return ' ,'.join(["?" for _ in range(len(values))])

    @staticmethod
    def _record_factory(cursor: Cursor, row: Row) -> dict[FieldName, _SqliteTypes]:
        fields = [column[0] for column in cursor.description]
        return {k:v for k,v in zip(fields, row)}
    
    @staticmethod
    def serialize(data: dict | list) -> str:
        return json.dumps(data)

    @staticmethod
    def deserialize(data: str) -> dict | list:
        return json.loads(data)

    @staticmethod
    def to_bool(data: int) -> bool:
        return bool(data)
    
    
    
class ArgumentParser(object):
    def __init__(self, tables: dict[str, Table], namespace: dict[str, NameSpace]) -> None:
        self.tables = tables
        self.namespace = namespace
    
    @classmethod
    def read_to_sql(
        cls, 
        tables: dict[str, Table],
        namespace: dict[str, NameSpace],
        fields: list[FieldName] = ["*"],
        conds: list[tuple[FieldName,str, Any]] | None = None,
        order: list[FieldName] | None = None,
        axis: int | None = None,
        limit: int | None = None,
        ) -> tuple[str, list[Any]]:
        """parse argument and return sql query"""
        parser = cls(tables, namespace)
        query, values = parser.parse(fields, conds, order, axis, limit)
        return (query, values)
    
    @classmethod
    def update_to_sql(
        cls,
        tables: dict[str, Table],
        namespace: dict[str, NameSpace],
        set_values: list[tuple[FieldName, Any]],
        conds: list[tuple[FieldName,str, Any]] | None = None
    ) -> tuple[str, list[Any]]:
        parser = cls(tables, namespace)
        table_name = parser.namespace[set_values[0][0]].table
        setter, set_vals = parser.update_setter(set_values)
        where, where_vals = parser.parse_where(conds)
        query = f"UPDATE {table_name} SET {setter} {where};"
        return (query, set_vals + where_vals)

    @classmethod
    def delete_to_sql(
        cls,
        tables: dict[str, Table],
        namespace: dict[str, NameSpace],
        conds: list[tuple[FieldName,str, Any]] | None = None
        ) -> tuple[str, list[Any]]:
        parser = cls(tables, namespace)
        table_name = tables["nodes"].table_name
        where, values = parser.parse_where_subqueries(conds)
        query = f"DELETE FROM {table_name} {where};"
        return (query, values)
    
    @classmethod
    def write_to_sql(cls, tables: dict[str, Table], namespace: dict[str, NameSpace], **kwargs) -> str:
        parser = cls(tables, namespace)
        query = ""
        return query

    def parse(
        self,
        fields: list[FieldName] = ["*"],
        conds: list[tuple[FieldName,str, Any]] | None = None,
        order: list[FieldName] | None = None,
        axis: int = 1,
        limit: int | None = None
        ) -> tuple:
        node_table = self.tables["nodes"].table_name
        f = self.parse_fields(fields)
        j = self.join_tables(fields, conds, order)
        w, v = self.parse_where(conds)
        o = self.parse_order(order, axis)
        l = self.parse_limit(limit)
        query = f"SELECT {f} FROM {node_table} {j} {w} {o} {l};"
        return query, v
        
    def parse_fields(self, fields: list[FieldName]) -> str:
        if "*" in fields:
            return "*"
        return ", ".join([self.namespace[fname].select() for fname in fields])

    def join_tables(self, fields: list[FieldName] = ["*"], conds: list[tuple[FieldName,str, Any]] | None = None, order: list[FieldName] | None = None) -> str:
        tnodes = self.tables["nodes"].table_name
        from_where, from_order, from_select = [], [], [self.namespace["depth"].join(tnodes)] # ['*']
        if conds is not None:
            from_where = [self.namespace[fname].join(tnodes) for fname in [c[0] for c in conds] if self.namespace[fname].to_join()]
            
        if order is not None:
            from_order = [self.namespace[fname].join(tnodes) for fname in order if self.namespace[fname].is_metadata()]
        
        if "*" not in fields:
            from_select = [self.namespace[fname].join(tnodes) for fname in fields if self.namespace[fname].is_metadata()]
        return " ".join(list(set(from_select + from_order + from_where)))

    def parse_where(self, conds: list[tuple[FieldName,str, Any]] | None) -> tuple[str, list[Any]]:
        if conds is None:
            return ("", [])
        where = [self.namespace[fname].where(op, v) for fname, op, v in conds]
        w, v = " AND ".join([w[0] for w in where]), []
        [v.extend(x[1]) if isinstance(x[1], list) else v.append(x[1]) for x in where]
        return (f"WHERE {w}", v)


    def parse_where_subqueries(self, conds: list[tuple[FieldName,str, Any]] | None) -> tuple[str, list[Any]]:
        if conds is None:
            return ("", [])
        
        where, values = [], []
        nodes_table = self.tables["nodes"].table_name
        for field, op, value in conds:
            namespace = self.namespace[field]
            if namespace.table != nodes_table:
                joiner = namespace.join(nodes_table)
                sub_where, sub_val = self.parse_where([((field, op, value))])
                subquery = f"""SELECT {field} FROM {nodes_table} {joiner} {sub_where}"""
                where.append(f" {field} IN ({subquery})")
                values.extend(sub_val)
            else:
                sub_where, sub_val = namespace.where(op, value)
                where.append(sub_where)
                if isinstance(sub_val, list):
                    values.extend(sub_val)
                else:
                    values.append(sub_val)
        where_str = " AND ".join(where)
        return (f"WHERE {where_str}", values)        
        

    def parse_limit(self, limit: Optional[int] | None = None):
        l = ""
        if limit is not None:
            l = f"LIMIT {str(limit)}"
        return l
    
    def parse_order(self, order: list[FieldName] | None, axis: int=1) -> str:
        substr = ""
        if order is None:
            return substr
        f = ", ".join([self.namespace[fname].select() for fname in order])
        substr = f"ORDER BY {f}"
        if axis == 1:
            substr += " ASC"
        else:
            substr += " DESC"
        return substr



    def parse_set(self, set_values: list[tuple[FieldName, _SqliteTypes]]) -> tuple[str, list[Any]]:
        f, v = [], []
        [(f.append(f"{fname} = ?"), v.append(value)) for fname, value in set_values]
        return (", ".join(f), v)


    def join_tables_for_update(self, set_values: list[tuple[FieldName, _SqliteTypes]], conds: list[tuple[FieldName,str, Any]] | None) -> str:
        tnodes = self.tables["nodes"].table_name
        
        from_select, from_where = [], []
        from_select = [self.namespace[fname].join(tnodes) for fname,_ in set_values if self.namespace[fname].to_join()]
        if conds is not None:
            from_where = [self.namespace[fname].join(tnodes) for fname,_,_ in conds if self.namespace[fname].to_join()]
        return " ".join(list(set(from_select + from_where)))

    def parse_where_for_update(self, conds: list[tuple[FieldName,str, Any]]) -> tuple[str, list[Any]]:
        where = [f"{fname} {op} {self.anchors_for_op(op, values)}" for fname, op, values in conds]
        w, v = " AND ".join(where), []
        [v.extend(x[1]) if isinstance(x[1], list) else v.append(x[1]) for _,_,x in conds]
        return (w,v)

    @staticmethod
    def anchors(values: list[Any]) -> str:
        return ' ,'.join(["?" for _ in range(len(values))])    

    @staticmethod
    def update_setter(set: list[tuple[FieldName, Any]]) -> tuple[str, Any]:
        values, fields = [], []
        [(fields.append(f"{field} = ?"), values.append(val)) for field, val in set]
        return (", ".join(fields), values)

    def anchors_for_op(self, op: str, values: Any) -> str:
        anchors = "?"
        if op.lower() == "in" and isinstance(values, list):
            anchors = f"({self.anchors(values)})"
        return anchors







if __name__ == "__main__":
    from pprint import pprint
    a = _Db(path="db.db", tree_name="topics")
    
    a.update([(("name_eng","test"))], conds=[("id", "=", "Healthcare")])
    b = a.read_many(fields=["id", "name_eng"], conds=[("id", "=", "Healthcare")])
    print(b)
    
    