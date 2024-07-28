from typing import Any, Literal, TypeVar

Payload = dict[str, Any]
Style = Literal["ascii", "ascii-ex", "ascii-exr", "ascii-emh", "ascii-emv", "ascii-em"]
Path = str
TreeName = str
TableName = str
FieldName = str
Operator = str
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
