import os
import json
from sqlite3 import Cursor, Row, connect, register_adapter, register_converter, PARSE_DECLTYPES

from typing import Literal, TypeVar, Optional, Any

Payload = dict[str, Any]
TableName = FieldName = str
_Nid = TypeVar("_Nid", str, int)
_SqliteTypes = TypeVar("_SqliteTypes", str, int, dict, list, bytes)

TriggerActions = Literal["INSERT", "DELETE", "UPDATE"]
JsonType = Literal["object", "array"]

def serialize(data: dict | list) -> str:
    return json.dumps(data)

def deserialize(data: str) -> dict | list:
    return json.loads(data)

def to_bool(data: int) -> bool:
    return bool(data)

class Db(object):
    def __init__(
        self, 
        path: str = "db.db",
        permanent: bool = True,
        read_only: bool = False
        ) -> None:
        
        if permanent is False:
            path = "file::memory:"
        elif os.path.exists(path) is False:
            path = path
        elif read_only:
            path = f"file:{path}?mode=ro"
        else:
            path = f"file:{path}?mode=rw"
        self.con = connect(path, detect_types=PARSE_DECLTYPES, uri=True)
        self.cursor = self.con.cursor()
        self.con.row_factory = self._record_factory
        self.namespace = {}
        
        register_adapter(list, serialize)
        register_adapter(dict, serialize)
        register_converter("JSON", deserialize)
        # register_converter("BOOL", to_bool)
        
        
        
    # __
    @staticmethod
    def _record_factory(cursor: Cursor, row: Row) -> dict[FieldName, _SqliteTypes]:
        fields = [column[0] for column in cursor.description]
        return {k:v for k,v in zip(fields, row)}

    def _write(self, tn:TableName, cn: list[FieldName], values: list[_SqliteTypes], no_commit: bool = False) -> int:
        _cn_str, _v_anchors = ' ,'.join(cn), ' ,'.join(["?" for _ in range(len(values))])
        self.cursor.execute(f"INSERT OR IGNORE INTO {tn}({_cn_str}) VALUES({_v_anchors});", tuple(values))
        if no_commit is False:
            self.con.commit()
        return self.cursor.lastrowid 
    
    def _write_many(self, tn:TableName, cn: list[FieldName], values: list[list[_SqliteTypes]], no_commit: bool = False) -> None:
        _cn_str, _v_anchors = ' ,'.join(cn), ' ,'.join(["?" for _ in range(len(values[0]))])
        self.cursor.executemany(f"INSERT OR IGNORE INTO {tn}({_cn_str}) VALUES({_v_anchors});", values)
        if no_commit is False:
            self.con.commit()
    
    def _delete(self, tn: TableName, conds:list[tuple[FieldName, str, Any]]) -> None:
        values, cond = [], []
        [(cond.append(f"{f} {op} ?"), values.append(v)) for f, op, v in conds]
        c = ' AND '.join(cond)
        self.cursor.execute(f"DELETE FROM {tn} WHERE {c};", values)
        self.con.commit()
    
    def _update(self, tn: TableName, set: list[tuple[FieldName, Any]], conds: list[tuple[str, str, Any]], no_commit: bool = False):
        fields, values, cond = [], [], []
        [(fields.append(f"{f} = ?"), values.append(v)) for f, v in set]
        [(cond.append(f"{f} {op} ?"), values.append(v)) for f, op, v in conds]
        f, c = ', '.join(fields), ' AND '.join(cond)
        self.cursor.execute(f"UPDATE {tn} SET {f} WHERE {c};", values)
        if no_commit is False:
            self.con.commit()
    
    def _read(
        self, 
        fields: list[FieldName], 
        table: TableName, 
        joins: list[tuple[TableName, Any, Any]], 
        cond:list[tuple[FieldName, str, Any]]
        ) -> list[Payload]:
        pass
        
    def _table(
        self, 
        tn: TableName,
        model: dict[FieldName, _SqliteTypes], 
        pk: Optional[list[FieldName]] | None = None,
        fk: list[tuple[TableName, str, str]] | None = None,
        if_not_exist: bool = True
        ) -> None:
        
        model_anchors = ", ".join([f"{k} {v}" for k,v in model.items()])
        exist = self._if_not_exist(if_not_exist)
        self.cursor.execute(
            f"""
            CREATE TABLE {exist} {tn} 
                (
                    {model_anchors} 
                    {self._pk_to_str(pk)} 
                    {self._fk_to_str(fk)}
                );
            """)
        self.con.commit()
        
    def _drop(self, tn: TableName) -> None:
        self.cursor.execute(f"DROP TABLE IF EXISTS {tn};")
        self.con.commit()
    
    def _index(self, tn: TableName, field: str):
        self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{tn}_{field} ON {tn}({field});")
        self.con.commit()

    def _table_info(self, tn: TableName) -> list[tuple]:
        return self.cursor.execute(f"PRAGMA table_info({tn})").fetchall()

    def _table_size(self, tn: TableName) -> int:
        return self.cursor.execute(f"SELECT COUNT(*) FROM {tn}").fetchone()[0]

    def _max_depth(self, tn: TableName) -> int:
        return self.cursor.execute(f"SELECT MAX(depth) FROM {tn}").fetchone()[0]
    
    def _get_tables(self, name: str) -> list[TableName]:
        return self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{name}__%';").fetchall()




    # parsers
    def _parse_fields(self, fields: list[FieldName]) -> str:
        if "*" in fields:
            return "*"
        return ", ".join([self.namespace[fname].select() for fname in fields])
    
    def _parse_join(self, fields: list[FieldName], conds: list[tuple[FieldName,str, Any]], order: list[FieldName]) -> str:
        tnodes = self.tables.get("nodes")
        if "*" in fields:
            to_join_for_select = [self.namespace["depth"].join(tnodes)]
        else:
            to_join_for_select = [self.namespace[fname].join(tnodes) for fname in fields if self.namespace[fname].is_metadata()]
        to_join__for_order = [self.namespace[fname].join(tnodes) for fname in order if self.namespace[fname].is_metadata()]
        to_join_for_where = [self.namespace[fname].join(tnodes) for fname in [c[0] for c in conds] if self.namespace[fname].to_join()]
        return " ".join(list(set(to_join_for_select + to_join__for_order + to_join_for_where)))
    
    def _parse_update_join(self, set_values: list[tuple[FieldName, _SqliteTypes]], conds: list[tuple[FieldName,str, Any]]) -> str:
        tnodes = self.tables.get("nodes")
        to_join_select = [self.namespace[fname].join(tnodes) for fname,_ in set_values if self.namespace[fname].to_join()]
        to_join_where = [self.namespace[fname].join(tnodes) for fname,_,_ in conds if self.namespace[fname].to_join()]
        return " ".join(list(set(to_join_select + to_join_where)))
    
    def _parse_where(self, conds: list[tuple[FieldName,str, Any]]) -> tuple[str, list[Any]]:
        where = [self.namespace[fname].where(op, v) for fname, op, v in conds]
        w, v = " AND ".join([w[0] for w in where]), [w[1] for w in where]
        return (w, v)
    
    def _parse_update_where(self, conds: list[tuple[FieldName,str, Any]]) -> tuple[str, list[Any]]:
        def anchor(op: str) -> str:
            a = "?"
            if "in" in op.lower():
                a = "(?)"
            return a
        where = [f"{fname} {op} {anchor(op)}" for fname, op, _ in conds]
        w, v = " AND ".join(where), [v for _,_,v in conds]
        return (w,v)
    
    def _parse_set(self, set_values: list[tuple[FieldName, _SqliteTypes]]) -> tuple[str, list[Any]]:
        f, v = [], []
        [(f.append(f"{fname} = ?"), v.append(value)) for fname, value in set_values]
        f = ", ".join(f)
        return (f, v)
    
    def _parse_order(self, order: list[FieldName], axis: int=1) -> str:
        o = ""
        if len(order) > 0:
            f = ", ".join([self.namespace[fname].select() for fname in order])
            o = f"ORDER BY {f}"
            if axis == 1:
                o += " ASC"
            else:
                o += " DESC"
        return o
    
    def _parse_limit(self, limit: Optional[int] | None = None):
        l = ""
        if limit is not None:
            l = f"LIMIT {str(limit)}"
        return l

    # triggers
    def _insert_trigger_on_array(self, trigger_name: str, tn: TableName, ftable: TableName, fname: FieldName):
        self.cursor.execute(
            f"""
            CREATE TRIGGER {trigger_name} AFTER INSERT ON {tn} BEGIN
                INSERT INTO {ftable}({fname}, nid, elm_idx) 
                SELECT j.value, {tn}.id, j.key FROM {tn}, json_each(NEW.{fname}) as j WHERE {tn}.id = NEW.id;
            END;
            """
        )
        self.con.commit()
    
    def _insert_trigger_on_object(self, trigger_name: str, tn: TableName, fname: FieldName, path: str):
        self.cursor.execute(
        f"""
        CREATE TRIGGER {trigger_name} AFTER INSERT ON {tn} BEGIN
            INSERT INTO {tn}({fname}) SELECT j.value FROM json_each(NEW.json, {path}) as j;
        END;
        """
        )
        self.con.commit()
    
    def _delete_trigger_on_array(self, trigger_name: str, tn: TableName, ftable: TableName):
        self.cursor.execute(
            f"""
            CREATE TRIGGER {trigger_name} AFTER DELETE ON {tn} BEGIN
                DELETE FROM {ftable} where nid = OLD.id;
            END;
            """
        )
        self.con.commit()
        
    # def _delete_trigger_on_object(self, trigger_name: str, tn: TableName):
    #     self.cursor.execute(
    #     f"""
    #     CREATE TRIGGER {trigger_name} AFTER DELETE ON {tn} BEGIN
    #         DELETE FROM {tn} where 
    #     END;
    #     """
    #     )
    #     self.con.commit()
    
    def _update_trigger_on_array(self, trigger_name: str, tn: TableName, ftable: TableName, fname: FieldName):
        self.cursor.execute(
            f"""
            CREATE TRIGGER {trigger_name} AFTER UPDATE OF {fname} ON {tn} BEGIN
                DELETE FROM {ftable} WHERE nid = OLD.id;
                INSERT INTO {ftable}({fname}, nid, elm_idx) 
                SELECT j.value, {tn}.id, j.key FROM {tn}, json_each(NEW.{fname}) as j WHERE {tn}.id = NEW.id;
            END;
            """
        )
        self.con.commit()
    
    def _update_trigger_on_object(self, trigger_name: str, tn: TableName, fname: FieldName, path: str):
        self.cursor.execute(
        f"""
        CREATE TRIGGER {trigger_name} AFTER UPDATE ON {tn} BEGIN
            SET {fname} = json_extract(NEW.json, {path});
        END;
        """
        )
        self.con.commit()
        
        
        
        
    def _col_gen_from_json(self,tn: TableName, fname: FieldName, base: str, path: str):
        elm_path = f"$.{path}"
        if path.startswith("["):
            elm_path = f"${path}"
        self.cursor.execute(f"ALTER TABLE {tn} ADD COLUMN {fname} TEXT AS (json_extract({base}, '{elm_path}'))")
        self.con.commit()    
        
    def _pk_to_str(self, pk: Optional[list[FieldName]] | None = None) -> str:
        s = ""
        if pk:
            pk = ', '.join(pk)
            s = f",PRIMARY KEY({pk} ASC)"
        return s
    
    def _fk_to_str(self, fk: list[tuple[TableName, str, str]] | None = None) -> str:
        s = ""
        if fk:
            s = ","+", ".join([f"FOREIGN KEY({inner_k}) REFERENCES {tn}({outer_k}) ON DELETE CASCADE" for inner_k, tn, outer_k in fk])
        return s

    def _if_not_exist(self, if_not_exist:bool) -> str:
        s = ""
        if if_not_exist:
            s = "IF NOT EXISTS"
        return s