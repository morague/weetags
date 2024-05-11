
from typing import Any, Generator

Payload = dict[str, Any]
TableName = FieldName = str


def infer_loader(path: str) -> str:
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
    


import json
from weetags.exceptions import NotImplemented

class DataWrapper(object):
    def __init__(self, data: list[Payload]) -> None:
        self.data = data
    
    def loader(self) -> Generator:
        for line in iter(self.data):
            yield line
    
    def __call__(self) -> Any:
        yield from self.loader()

class JsonLoader(object):
    def __init__(self, fp: str, strategy: str= "default") -> None:
        self.fp = fp

    def default_loader(self):
        with open(self.fp) as f:
            data = iter(json.load(f))
            while line := next(data, None):
                yield line
                
    def lazy_loader(self):
        raise NotImplemented()
         
         
class JlLoader(list):
    def __init__(self, fp: str, strategy: str= "default") -> None:
        self.fp = fp
        self.loader = {
            "default": self.default_loader,
            "lazy": self.lazy_loader
        }[strategy]

    def default_loader(self):
        with open(self.fp) as f:
            data = iter([json.loads(line) for line in f.readlines()])
            while line := next(data, None):
                yield line

    def lazy_loader(self):
        with open(self.fp) as f:
            while line := f.readline():
                yield json.loads(line.strip("\n"))
    
    def __call__(self) -> Any:
        yield from self.loader()
        
        

# loader = infer_loader("./tags/topics.jl")
# a = loader("./tags/topics.jl")
# for i in a.lazy_loader():
#     print(i)