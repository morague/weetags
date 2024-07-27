
from random import choices
from string import ascii_lowercase, ascii_uppercase, digits

from typing import Any, Generator

from weetags.tools.loaders import JlLoader, JsonLoader

Payload = dict[str, Any]
TableName = FieldName = str

CHARS = ascii_lowercase + ascii_uppercase + digits

def infer_loader(path: str) -> JlLoader | JsonLoader:
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

def generate_uuid() -> str:
    """fewer collisions method"""
    return ''.join(choices(CHARS, k=8))
