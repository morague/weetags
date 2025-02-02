import sqlite3
import traceback
from pathlib import Path

from typing import Any, Callable

from weetags.engine.sql import DTYPES
from weetags.loaders import JlLoader, JsonLoader

StrOrPath = str | Path




def infer_loader(path: StrOrPath) -> JlLoader | JsonLoader:
    ext = path.split(".")[-1]
    loaders = {
        "json": JsonLoader,
        "jl": JlLoader,
        "jsonlines": JlLoader
    }
    loader = loaders.get(ext, None)
    if loader is None:
        raise ValueError("non recognized file type")
    return loader

def infer_dtype(value: Any) -> str:
    if isinstance(value, bool):
        dtype = "BOOL"
    elif isinstance(value, int):
        dtype = "INTEGER"
    elif isinstance(value, str):
        dtype = "TEXT"
    elif value is None:
        dtype = "NULL"
    elif isinstance(value, float):
        dtype = "REAL"
    elif isinstance(value, list):
        dtype = "JSONLIST"
    else:
        dtype = "JSON"
    return dtype

def valid_creation(f: Callable):
    def wrapped(tree, nid, parent, node_values):
        node = {"id": nid, "parent": parent}
        if node_values is not None:
            node.update(node_values)

        node_table = tree.tables.get("nodes")
        for _, field in node_table.iter_fields:
            value = node.get(field.name, None)
            if field.dtype == "JSON" and value is None:
                node_values.update({field.name:{}})
            elif field.dtype == "JSONLIST" and value is None:
                node_values.update({field.name:[]})
            if value is not None and isinstance(value, DTYPES[field.dtype]) is False:
                raise ValueError(f"node field {field.name} either doesn't exist or has wrong dtype.")

        parent = node.get("parent", False)
        if parent is False: # none  is for root
            raise ValueError(f"A node must have a `parent` field")
        return f(tree, nid, parent, node_values)
    return wrapped

def valid_update(f: Callable):
    def wrapped(*args):
        tree, cond, set_values = args
        node_table = tree.tables.get("nodes")
        meta_table = tree.tables.get("metadata")
        for k,v in set_values:
            field = getattr(node_table, k, None) or getattr(meta_table, k, None)
            if field.name in ["id","nid","parent","children", "depth", "is_root", "is_leaf"]:
                raise KeyError(f"You cannot update the following fields: [`id`, `nid`, `parent`, `children`, `depth`, `is_root`, `is_leaf`]")
            if field is None or isinstance(v, DTYPES[field.dtype]) is False:
                raise ValueError(f"node field {k} either doesn't exist or has wrong dtype.")
        return f(tree, cond, set_values)
    return wrapped

def valid_append(f: Callable):
    def wrapped(*args):
        tree, cond, fname, values = args
        node_table = tree.tables.get("nodes")
        meta_table = tree.tables.get("metadata")
        field = getattr(node_table, fname, None) or getattr(meta_table, fname, None)
        if fname in ["id","nid","parent","children", "depth", "is_root", "is_leaf"]:
            raise KeyError(f"You cannot update the following fields: [`id`, `nid`, `parent`, `children`, `depth`, `is_root`, `is_leaf`]")
        if field is None or field.dtype not in ["JSON","JSONLIST"]:
            raise TypeError("field_name must reference field containing a collection such as a list or a dict")
        return f(tree, cond, fname, values)
    return wrapped    


def apply_handler(decorator: Callable):
    def decorate(cls):
        for attr in cls.__dict__:
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate

def ErrorHandler(f: Callable):
    def wrapped(*args, **kwargs):
        cls = args[0]
        ok, max_tries, tries = False, 5, 0
        while ok is False and tries < max_tries:
            try:
                res = f(*args, **kwargs)
                ok = True
                return res
            except sqlite3.IntegrityError as e:
                tries += 1
                print(f"INTEGRITY {e}")

            except sqlite3.DatabaseError as e:
                print(f"ERROR: {e}")
                tries += 1
            except Exception:
                tries += 1
                traceback.print_exc(limit=0)
                exit(1)
        
    return wrapped
