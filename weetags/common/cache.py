from __future__ import annotations

from pymemcache import PooledClient, serde
from abc import ABC, abstractmethod

from typing import Any





class CacheEngine(ABC):

    @property
    def name(self) -> str:
        return "base"

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def pop(self, key: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def append(self, key: str, value: Any) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_keys_with_prefix(self, prefix: str) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def check(self, key: str) -> bool:
        raise NotImplementedError()
    

class LocalCacheEngine(CacheEngine):
    cache: dict[str, Any]

    def __init__(self, **opts):
        self.cache = {}

    @property
    def name(self) -> str:
        return "local"

    def set(self, key: str, value: Any) -> None:
        self.cache.update({key:value})

    def get(self, key: str, default = None) -> Any:
        return self.cache.get(key, default)

    def pop(self, key: str) -> Any:
        self.cache.pop(key)

    def check(self, key: str) -> bool:
        return key in self.cache.keys()

    def append(self, key: str, value: Any) -> None:
        check = self.check(key)
        if check is False:
            self.set(key, [])
        self.cache[key].append(value)

    def get_keys_with_prefix(self, prefix: str) -> list[str]:
        return [k for k in self.cache.keys() if k.startswith(prefix)]

class MemcachedCacheEngine(CacheEngine):
    client: PooledClient

    def __init__(self, server: str, workers: int = 4, **opts):
        self.client = PooledClient(server, serde=serde.pickle_serde, max_pool_size=workers)

    @property
    def name(self) -> str:
        return "memcached"

    def set(self, key: str, value: Any) -> None:
        self.client.set(key, value, noreply=True)

    def get(self, key: str, default = None):
        return self.client.get(key, default)

    def pop(self, key: str) -> Any:
        self.client.delete(key)

    def check(self, key: str) -> bool:
        check = True
        res = self.get(key, "no")
        if res == "no":
            check = False
        return check

    def append(self, key: str, value: Any) -> None: ...
    def get_keys_with_prefix(self, prefix: str) -> list[str]: ...