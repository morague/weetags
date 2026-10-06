from __future__ import annotations

from pymemcache import PooledClient, serde
from abc import ABC, abstractmethod

from typing import Any





class CacheEngine(ABC):

    @property
    def name(self) -> str:
        return "base"

    @abstractmethod
    def flush(self) -> None:
        raise NotImplementedError()

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

    def flush(self) -> None:
        self.cache = {}

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

    def __init__(self, server: str, workers: int = 4, prefix: str | None = None, **opts):
        self.client = PooledClient(server, serde=serde.pickle_serde, max_pool_size=workers)
        self._set_key_prefix(prefix)
        self.flush()

    @property
    def name(self) -> str:
        return "memcached"

    def flush(self) -> None:
        self.client.flush_all()
        self._set_tracker()

    def set(self, key: str, value: Any) -> None:
        exist = self.check(key)
        self.client.set(key, value, noreply=True)
        if exist is False:
            self._append_tracker(key)

    def get(self, key: str, default = None):
        return self.client.get(key, default)

    def pop(self, key: str) -> Any:
        self.client.delete(key)

    def check(self, key: str) -> bool:
        check = True
        res = self.get(key, None)
        if res is None:
            check = False
        return check

    def append(self, key: str, value: Any) -> None:
        if self.check(key) is False:
            values = []
        else:
            values = self.get(key)
            assert isinstance(values, list) 
        values.append(value)
        self.set(key, values)
    
    def get_keys_with_prefix(self, prefix: str) -> list[str]: 
        tracker: str = self.get("_tracker")
        return [k for k in tracker.split(",") if k.startswith(prefix)]

    def _set_tracker(self) -> None:
        self.client.set("_tracker", "")

    def _append_tracker(self, key: str) -> None:
        self.client.append("_tracker", f"{key},")

    def _set_key_prefix(self, prefix: str | None = None) -> None:
        p = prefix or ""
        self.client.key_prefix = p.encode()