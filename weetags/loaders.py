import json
from typing import Any, Generator

from weetags.exceptions import NotImplemented


Payload = dict[str, Any]
TableName = FieldName = str

class Loader(object):
    def __init__(self, data: list[Payload]) -> None:
        self.data = data
        self.loader = self.default_loader

    def default_loader(self) -> Generator:
        for line in iter(self.data):
            yield line


class JsonLoader(Loader):
    def __init__(self, fp: str, strategy: str= "default") -> None:
        self.fp = fp
        self.loader = {
            "default": self.default_loader,
            "lazy": self.lazy_loader
        }[strategy]

    def default_loader(self) -> Generator:
        with open(self.fp) as f:
            data = iter(json.load(f))
            while line := next(data, None):
                yield line

    def lazy_loader(self) -> Generator:
        raise NotImplemented()


class JlLoader(Loader):
    def __init__(self, fp: str, strategy: str= "default") -> None:
        self.fp = fp
        self.loader = {
            "default": self.default_loader,
            "lazy": self.lazy_loader
        }[strategy]

    def default_loader(self) -> Generator:
        with open(self.fp) as f:
            data = iter([json.loads(line) for line in f.readlines()])
            while line := next(data, None):
                yield line

    def lazy_loader(self) -> Generator:
        with open(self.fp) as f:
            while line := f.readline():
                yield json.loads(line.strip("\n"))

    def __call__(self) -> Any:
        yield from self.loader()
