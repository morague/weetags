from attrs import define, field, validators
from collections.abc import Sequence
from enum import Enum

from typing import Any

from weetags.engine.schema import SimpleSqlTable, Namespace

Node = dict[str, Any]
Nodes = list[Node]
Conditions = list[list[tuple[str, str, Any] | str] | str]

DTYPES = {"TEXT": str, "INTEGER": int, "JSONLIST": list, "JSON": dict, "BOOL": bool}

# CREATE
CREATE_TABLE = "CREATE TABLE  IF NOT EXISTS {table_name} ({fields});"
CREATE_INDEX = "CREATE INDEX IF NOT EXISTS idx_{table_name}_{field_name} ON {table_name}({field_name});"
CREATE_EXTRACT_COLUMN = "ALTER TABLE {table_name} ADD COLUMN {target_field}_{path} TEXT AS (json_extract({target_field}, '{path}'))"

# TRIGGERS
ADD_JSON_TRIGGER = """\
INSERT INTO {table_name}({target_field}, nid, elm_idx)
SELECT json_extract({base}, "$.{path}"), {target_table}.id, 1 FROM {target_table} WHERE {target_table}.id = NEW.id;
"""

UPDATE_JSON_TRIGGER = """\
DELETE FROM {table_name} WHERE nid = OLD.id;
INSERT INTO {table_name}({target_field}, nid, elm_idx)
SELECT json_extract({base}, "$.{path}"), {target_table}.id, 1 FROM {target_table} WHERE {target_table}.id = NEW.id;
"""

ADD_JSONLIST_TRIGGER = """\
INSERT INTO {table_name}({target_field}, nid, elm_idx)
SELECT j.value, {target_table}.id, j.key FROM {target_table}, json_each(NEW.{target_field}) as j WHERE {target_table}.id = NEW.id;
"""

UPDATE_JSONLIST_TRIGGER = """\
DELETE FROM {table_name} WHERE nid = OLD.id;
INSERT INTO {table_name}({target_field}, nid, elm_idx)
SELECT j.value, {target_table}.id, j.key FROM {target_table}, json_each(NEW.{target_field}) as j WHERE {target_table}.id = NEW.id;
"""

CREATE_TRIGGER = """\
CREATE TRIGGER {table_name}__insert_trigger AFTER INSERT ON {target_table} BEGIN
{trigger}
END;
"""
DELETE_TRIGGER = """\
CREATE TRIGGER {table_name}__delete_trigger AFTER DELETE ON {target_table} BEGIN
DELETE FROM {table_name} where nid = OLD.id;
END;
"""
UPDATE_TRIGGER = """\
CREATE TRIGGER {table_name}__update_on_{target_field}_trigger AFTER UPDATE OF {target_field} ON {target_table} BEGIN
{trigger}
END;
"""


## infos
INFO = "PRAGMA table_info({table_name});"
FK_INFO = "PRAGMA foreign_key_list({table_name});"
TABLE_SIZE = "SELECT COUNT(*) FROM {table_name};"
TREE_DEPTH = "SELECT MAX(depth) FROM {table_name};"
TABLE_NAMES = "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{tree_name}__%';"

# actions
DROP = "DROP TABLE IF EXISTS {table_name};"
WRITE = "INSERT {on_conflict} INTO {table_name}({col_names}) VALUES({anchors});"
READ_ONE = "SELECT {fields} FROM {node_table} {joins} {conditions} {order} {axis};"
READ_MANY = "SELECT {fields} FROM {node_table} {joins} {conditions} {order} {axis} {limit};"
UPDATE = "UPDATE {table_name} SET {setter} {conditions};"
DELETE = "DELETE FROM {node_table} {conditions};"

SEARCH_SUBQUERY = "id IN ({subquery})"
CHILDREN_FROM_IDS = "SELECT id, children FROM {table_name} WHERE id IN ({anchors});"
CHILDREN_FROM_ID = "SELECT id, children FROM {table_name} WHERE id=?;"
GET_USER = "SELECT username, password_sha256, auth_level, salt, max_age FROM weetags__users WHERE username=?"
GET_RESTRICTION = "SELECT auth_level FROM weetags__restrictions WHERE tree=? AND blueprint=?"

def pk_to_sql(pk: list[str]) -> str:
    s = ""
    if pk:
        s = f"PRIMARY KEY ({', '.join(pk)})"
    return s

class SqlOperator(str, Enum):
    Eq = "="
    Diff = "!="
    Glob = "GLOB"
    Gt = ">"
    Ge = ">="
    Ngt = "!>"
    Lt = "<"
    Le = "<="
    Nlt = "!<"
    Like = "LIKE"
    Ilike = "ILIKE"
    In = "IN"
    Not_in = "NOT IN"
    Is = "IS"
    Is_not = "IS NOT"

    def values():
        return [s.value for s in SqlOperator]

class OnConflict(str, Enum):
    Update = "update"
    Ignore = "ignore"
    Rollback = "rollback"
    Abort = "abort"
    Fail = "fail"
    Replace = "replace"
    NONE = "none"

    def sql(self) -> str:
        if self.value == "none":
            return ""
        return f"OR {str(self).upper()}"


def intOrNone(instance, attribute, value):
    if value is None:
        return
    if not isinstance(value, int):
        raise TypeError(f"attribute {attribute} must be of type int.")

def listOrNone(instance, attribute, value):
    if value is None:
        return
    if not isinstance(value, list):
        raise TypeError(f"attribute {attribute} must be of type list.")

def onConflictOrNone(instance, attribute, value):
    if value is None:
        return
    if not isinstance(value, OnConflict):
        raise TypeError(f"attribute {attribute} must be of type OnConflict.")


@define(kw_only=True)
class _SimpleSqlConverter:
    """
    Works without any namespace nor tables structure.
    Ideally used during building time by the treeBuilder
    """
    table_name: str | None = field(default=None)
    target_columns: list[str] | None = field(default=None, validator=[listOrNone])
    values: list[Any]|list[list[Any]]|None = field(default=None, validator=[listOrNone])
    setter: list[tuple[str, Any]] | None = field(default=None, validator=[listOrNone])
    conds: list[tuple[str, str, Any]] | None = field(default=None, validator=[listOrNone])


    def _children_from_ids(self) -> tuple[str, list[Any]]:
        anchors = self.anchors(self.values)
        stmt = CHILDREN_FROM_IDS.format(table_name=self.table_name, anchors=anchors)
        return (stmt, self.values)

    def _children_from_id(self) -> tuple[str, list[Any]]:
        stmt = CHILDREN_FROM_ID.format(table_name=self.table_name)
        return (stmt, self.values)

    def _write_many(self) -> tuple[str, list[list[Any]]]:
        # add validation for columns names ? and table_name ?

        if not all([self.table_name, self.target_columns, self.values]):
            raise ValueError()

        columns = ", ".join(self.target_columns) # type: ignore
        anchors = self.anchors(self.values[0]) # type: ignore
        stmt =  WRITE.format(table_name=self.table_name, col_names=columns, anchors=anchors, on_conflict="")
        return (stmt, self.values) # type: ignore

    def _update(self) -> tuple[str, list[Any]]:
        def parse_simple_conditions(conds: list[tuple[str, str, Any]] | None) -> tuple[str, list[Any]]:
            if conds is None:
                return ("", [])

            values, conditions = [], []
            for field, op, val in conds:
                anchors = f"({self.condition_anchor(op, val)})"
                conditions.append(f"{field} {op} {anchors}")
                values.append(val)
            return ("WHERE " + ' AND '.join(conditions), values)

        def parse_simple_setter(setter: list[tuple[str, Any]]) -> tuple[str, list[Any]]:
            values, fields = [], []
            [(fields.append(f"{field} = ?"), values.append(val)) for field, val in setter]
            return (", ".join(fields), values)

        if self.setter is None:
            raise ValueError(f"You must Set some pairs of key values to update.")

        conditions, cvalues = parse_simple_conditions(self.conds)
        setter, svalues = parse_simple_setter(self.setter)
        stmt = UPDATE.format(table_name=self.table_name, setter=setter, conditions=conditions)
        return (stmt, svalues + cvalues)


    @staticmethod
    def condition_anchor(op: str, values: Any) -> str:
        """define the right anchor for the given condition operator."""
        anchor = "?"
        if op.lower() == "in" and isinstance(values, list):
            anchors = ", ".join(["?" for _ in range(len(values))])
            anchor = f"({anchors})"
        return anchor

    @staticmethod
    def anchors(values: list[Any]) -> str:
        return ' ,'.join(["?" for _ in range(len(values))])

    @staticmethod
    def update_setter(set: list[tuple[str, Any]]) -> tuple[str, Any]:
        values, fields = [], []
        [(fields.append(f"{field} = ?"), values.append(val)) for field, val in set]
        return (", ".join(fields), values)


@define(kw_only=True)
class SqlConverter:
    namespaces: dict[str, Namespace] = field()
    tables: dict[str, SimpleSqlTable] = field()

    table_name: str | None = field(default=None)
    target_columns: list[str] | None = field(default=None, validator=[listOrNone])
    values: list[Any]|list[list[Any]]|None = field(default=None, validator=[listOrNone])
    on_conflict: OnConflict|None = field(default=None, validator=[onConflictOrNone])
    conds: list[list[tuple[str, str, Any] | str] | str] | None = field(default=None, validator=[listOrNone])
    limit: int | None = field(default=None, validator=[intOrNone])
    fields: list[str] | None = field(default=None, validator=[listOrNone])
    order_by: list[str] | None = field(default=None, validator=[listOrNone])
    axis: int = field(default=1, converter=int, validator=[validators.instance_of(int)])
    setter: list[tuple[str, Any]] | None = field(default=None, validator=[listOrNone])

    def write_one(self) -> tuple[str, list[Any]]:

        # add validation for columns names ? and table_name ?

        if not all([self.table_name, self.target_columns, self.values]):
            raise ValueError()
        target_table = self.tables.get(self.table_name, None) # type: ignore
        if target_table is None:
            raise KeyError(f"Unknown table type: {self.table_name}")
        ttable_name = target_table._name
        columns = " ,".join(self.target_columns) # type: ignore
        anchors = self.anchors(self.values) # type: ignore
        on_conflict = self.parse_conflict_handling()
        stmt = WRITE.format(table_name=ttable_name, col_names=columns, anchors=anchors, on_conflict=on_conflict)
        return (stmt, self.values) # type: ignore

    def write_many(self) -> tuple[str, list[list[Any]]]:

        # add validation for columns names ? and table_name ?

        if not all([self.table_name, self.target_columns, self.values]):
            raise ValueError()

        target_table = self.tables.get(self.table_name, None) # type: ignore
        if target_table is None:
            raise KeyError(f"Unknown table type: {self.table_name}")
        ttable_name = target_table._name
        columns = " ,".join(self.target_columns) # type: ignore
        anchors = self.anchors(self.values[0]) # type: ignore
        on_conflict = self.parse_conflict_handling()
        stmt =  WRITE.format(table_name=ttable_name, col_names=columns, anchors=anchors, on_conflict=on_conflict)
        return (stmt, self.values) # type: ignore

    def read_one(self) -> tuple[str, list[Any]]:
        node_table = self.tables["nodes"]._name
        fields = self.parse_fields()
        conditions, values = self.parse_conditions()
        joins = self.parse_joins()
        order_by = self.parse_order()
        axis = self.parse_axis()
        stmt = READ_ONE.format(
            fields=fields,
            node_table=node_table,
            joins=joins,
            conditions=conditions,
            order=order_by,
            axis=axis
        )
        return (stmt, values)

    def read_many(self) -> tuple[str, list[Any]]:
        node_table = self.tables["nodes"]._name
        fields = self.parse_fields()
        conditions, values = self.parse_conditions()
        joins = self.parse_joins()
        order_by = self.parse_order()
        axis = self.parse_axis()
        limit = self.parse_limit()
        stmt = READ_MANY.format(
            fields=fields,
            node_table=node_table,
            joins=joins,
            conditions=conditions,
            order=order_by,
            axis=axis,
            limit=limit
        )
        return (stmt, values)

    def delete(self) -> tuple[str, list[Any]]:
        node_table = self.tables["nodes"]._name
        conditions, values = self.parse_conditions(with_subqueries=True)
        stmt = DELETE.format(node_table=node_table, conditions=conditions)
        return (stmt, values)

    def update(self) -> tuple[str, list[Any]]:
        table = self.tables.get(self.table_name, None) # type: ignore
        if table is None:
            raise KeyError(f"Unknown table type: {self.table_name}")
        if self.setter is None:
            raise ValueError(f"You must Set some pairs of key values to update.")
        setter, svalues = self.update_setter(self.setter)
        conditions, cvalues = self.parse_conditions()
        stmt = UPDATE.format(table_name=table._name, setter=setter, conditions=conditions)
        return (stmt, svalues + cvalues)

    def parse_joins(self) -> str:
        def parse_sequence(sequence:list[str]|Conditions) -> list[str]:
            if sequence is None or len(sequence) == 0:
                return []
            if isinstance(sequence[0], Sequence) and not isinstance(sequence[0], str):
                fields = self.extract_fieldnames(sequence) # type: ignore
            else:
                fields = sequence

            tnodes = self.tables["nodes"]._name
            buff = []
            for fname in fields:
                if fname == "*":
                    continue
                namespace = self.namespaces.get(fname, None) # type: ignore
                if namespace is None:
                    raise KeyError(f"Unknown table field: {fname}")
                if namespace.is_joinable():
                    buff.append(namespace.join(tnodes))
            return buff

        tnodes = self.tables["nodes"]._name
        stmts = [self.namespaces["depth"].join(tnodes)]
        for sequence in [self.conds, self.order_by, self.fields]:
            stmts.extend(parse_sequence(sequence))
        return " ".join(list(set(stmts)))

    def parse_conditions(self, with_subqueries: bool=False) -> tuple[str, list[Any]]:
        """
        a set of conditions is a list of list condition tuple, possibly seperated by AND/OR operators.
        """
        if self.conds is None:
            return ("", [])

        segments, values = [], []
        for cond in self.conds:
            if isinstance(cond, Sequence) and not isinstance(cond, str):
                set_of_conds, vals = self.parse_set_of_conditions(cond, with_subqueries)
                segments.append(set_of_conds)
                values.extend(vals)
            elif isinstance(cond, str) and cond in ["AND", "OR"]:
                segments.append(cond)
            else:
                raise ValueError("Conditions must be a list of tuple[fieldName, op, value] | AND | OR")
        conditions = self.join_conditions(segments, comma=True)
        return (f"WHERE {conditions}", values)

    def parse_set_of_conditions(self, conds: list[tuple[str, str, Any] | str], with_subqueries: bool=False) -> tuple[str, list[Any]]:
        conditions, values = [], []
        for cond in conds:
            if with_subqueries and isinstance(cond, Sequence) and not isinstance(cond, str):
                converter = SqlConverter(
                    namespaces=self.namespaces,
                    tables=self.tables,
                    fields=["id"],
                    conds=[[cond]]
                )
                subquery, vals = converter.read_many()
                stmt = SEARCH_SUBQUERY.format(subquery=subquery.rstrip(";"))
                conditions.append(stmt)
                values.extend(vals)

            elif isinstance(cond, Sequence) and not isinstance(cond, str):
                f, op, val = cond
                namespace = self.namespaces.get(f, None)
                if namespace is None:
                    raise KeyError(f"Unknown Table Field: {f}")
                if op.upper() not in SqlOperator.values():
                    raise KeyError(f"Unknown Operator: {op}")

                where, val = namespace.where(op, val)
                conditions.append(where)
                if isinstance(val, list):
                    values.extend(val)
                else:
                    values.append(val)

            elif isinstance(cond, str) and cond in ["AND", "OR"]:
                conditions.append(cond)
            else:
                raise ValueError(f"Unknown Condition: {cond}")
        return (self.join_conditions(conditions), values)

    def join_conditions(self, segments: list[str], comma: bool = False) -> str:
        buff = []
        for i in range(len(segments)):
            seg = segments[i]
            if i % 2 != 0 and seg not in ["AND", "OR"]:
                buff.append("AND")
            if comma and seg not in ["AND", "OR"]:
                buff.append(f"({seg})")
            else:
                buff.append(seg)
        return " ".join(buff)

    def extract_fieldnames(self, sequence: Conditions) -> list[str]:
        buff = []
        for cond_set in sequence:
            if isinstance(cond_set, Sequence) and not isinstance(cond_set, str):
                for cond in cond_set:
                    if isinstance(cond, Sequence) and not isinstance(cond, str):
                        buff.append(cond[0])
        return buff

    def parse_fields(self) -> str:
        fields = ["*"]
        if self.fields is not None:
            fields = []
            for fname in self.fields:
                namespace = self.namespaces.get(fname, None)
                if namespace is None:
                    raise KeyError(f"Unknown field name: {fname}")
                fields.append(namespace.select())
        return ", ".join(fields)

    def parse_conflict_handling(self) -> str:
        handler = ""
        if self.on_conflict is not None:
            handler = self.on_conflict.sql()
        return handler

    def parse_limit(self) -> str:
        limit = ""
        if self.limit is not None:
            limit = f"LIMIT {str(self.limit)}"
        return limit

    def parse_order(self) -> str:
        if self.order_by is None:
            return ""
        f = ", ".join([self.namespaces[fname].select() for fname in self.order_by])
        return f"ORDER BY {f}"

    def parse_axis(self) -> str:
        if self.order_by is None:
            return ""
        match self.axis:
            case 1:
                return "ASC"
            case 0:
                return "DESC"
            case _:
                raise ValueError("Axis must be either 1 or 0")

    def update_setter(self, set_values: list[tuple[str, Any]]) -> tuple[str, Any]:
        values, fields = [], []
        for fname, val in set_values:
            namespace = self.namespaces.get(fname, None)
            if namespace is None:
                raise KeyError(f"Unknown field name: {fname}")
            fields.append(f"{fname} = ?")
            values.append(val)
        return (", ".join(fields), values)

    @staticmethod
    def condition_anchor(op: str, values: Any) -> str:
        """define the right anchor for the given condition operator."""
        anchor = "?"
        if op.lower() == "in" and isinstance(values, list):
            anchors = ' ,'.join(["?" for _ in range(len(values))])
            anchor = f"({anchors})"
        return anchor

    @staticmethod
    def anchors(values: list[Any]) -> str:
        return ' ,'.join(["?" for _ in range(len(values))])


