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