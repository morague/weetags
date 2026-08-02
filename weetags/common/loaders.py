from __future__ import annotations
import csv
import re
import os
import sys
import yaml
import glob
import json
import tomllib
from pathlib import Path
from yaml import SafeLoader
from collections import defaultdict
from abc import ABC, abstractmethod

from typing import Any, Type, Generator, TypeVar

Payload = dict[str, Any]
ESCAPED = ["\\", "^", "$", ".", "|", "?", "*", "+", "(", ")", "[", "]", "{", "}"]

T = TypeVar("T", dict[str, Any], list[dict[str, Any]], csv.DictReader)

def select_loader(path: str | Path) -> Type[Loader]:
    path = Path(path)
    match path.suffix:
        case ".yml" | ".yaml":
            return YamlLoader
        case ".toml":
            return TomlLoader
        case _:
            raise ValueError(f"non handled File format: {path.suffix}.")




class DictLoader:
    def lazy_loader(self, data: list[dict[str, Any]]) -> Generator[dict[str, Any]]:
        for payload in data:
            yield payload

class Loader(ABC):
    EXTS: list[str]

    @abstractmethod
    def load(self, fp: str | Path) -> T:
        raise NotImplementedError()

    @abstractmethod
    def load_from_str(self, data: str) -> T:
        raise NotImplementedError()

    def lazy_loader(self, fp: str | Path) -> Generator[dict[str, Any]]:
        raise NotImplementedError()

class JsonLoader(Loader):
    EXTS = [".json"]

    def load(self, fp: str | Path) -> list[dict[str, Any]]:
        with open(fp, "r") as f:
            return json.load(f)

    def load_from_str(self, data: str) -> list[dict[str, Any]]:
        return json.loads(data)

class JLLoader(Loader):
    EXTS = [".jl"]

    def load(self, fp: str | Path) -> list[dict[str, Any]]:
        jsons = []
        with open(fp, "r") as f:
            for line in f.readlines():
                jsons.append(json.loads(line))
        return jsons

    def load_from_str(self, data: str) -> list[dict[str, Any]]:
        jsons = []
        for line in data.split("\n"):
            jsons.append(json.loads(line))
        return jsons

    def lazy_loader(self, fp: str | Path, chunk_size: int = 4048) -> Generator[dict[str, Any]]:
        current = ""
        with open(fp, "r") as f:
            while data := f.read(chunk_size):
                payloads = data.strip("\n").split("\n")
                if len(payloads) == 1 and payloads[0].endswith("}"):
                    yield json.loads(current + payloads.pop(0)) 
                    current = ""
                elif len(payloads) == 1:
                    current += payloads.pop(0)
                else:
                    yield json.loads(current + payloads.pop(0)) 
                    current = payloads.pop()
                    for payload in payloads:
                        yield json.loads(payload)
        if len(current) > 0:
            yield json.loads(current)

class CSVLoader(Loader):
    EXTS = [".csv"]

    def load(self, fp: str | Path) -> csv.DictReader:
        with open(fp, "r") as f:
            c = csv.DictReader(f)
        return c

    def load_from_str(self, data: str) -> csv.DictReader:
        c = csv.DictReader(data)
        return c


class YamlLoader(Loader):
    EXTS = [".yml", ".yaml"]

    def load(self, fp: str | Path) -> dict[str, Any]:
        with open(fp, "r") as f:
            payload = yaml.load(f, SafeLoader)
        return payload

    def load_from_str(self, data: str) -> dict[str, Any]:
        return yaml.load(data, SafeLoader)


class TomlLoader(Loader):
    EXTS = [".toml"]

    def load(self, fp: str | Path) -> dict[str, Any]:
        with open(fp, "rb") as f:
            payload = tomllib.load(f)
        return payload

    def load_from_str(self, data: str) -> dict[str, Any]:
        return tomllib.loads(data)




class Pattern(object):
    prefix: str
    suffix: str

    def __init__(self, prefix: str, suffix: str):
        self.prefix = prefix
        self.suffix = suffix

    @property
    def find(self) -> str:
        prefix, suffix = self._escaped()
        return f"^{prefix}.*?{suffix}$"

    @property
    def sub(self) -> str:
        prefix, suffix = self._escaped()
        return f"^{prefix}|{suffix}"

    def key(self, s: str) -> str:
        return re.sub(re.compile(self.sub), "", s).strip()

    def is_pattern(self, s: str) -> bool:
        return bool(re.search(re.compile(self.find), s))

    def _escaped(self) -> tuple[str, str]:
        prefix = "".join([c if c not in ESCAPED else f"\\{c}" for c in self.prefix])
        suffix = "".join([c if c not in ESCAPED else f"\\{c}" for c in self.suffix])
        return (prefix, suffix)


class ConfigLoader:
    def __init__(
        self,
        d: dict[str, Any] | None = None,
        environ_pattern: Pattern = Pattern("${", "}"),
        template_pattern: Pattern = Pattern("{{", "}}"),
        allow_env_specific_merging: bool = False,
        main_configs_name: str | None = None,
    ) -> None:
        self.environ_pattern = environ_pattern
        self.template_pattern = template_pattern
        self.allow_env_specific_merging = allow_env_specific_merging
        self.main_configs_name = main_configs_name
        self.d = {}
        if d is not None:
            self.d = d

    def load(
        self,
        fp: str | Path,
        loader: Type[Loader] = YamlLoader,
        template: dict[str, Any] | None = None,
        overwrite: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = loader().load(fp)
        payload = self.map(payload, template, overwrite)
        return payload

    def map(
        self,
        d: dict[str, Any],
        template: dict[str, Any] | None = None,
        overwrite: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if template is None:
            template = {}
        if overwrite is None:
            overwrite = {}
        self.d = d
        self.template = template
        self.overwrite = overwrite
        self.d = self._map(self.d, [])

        if self.allow_env_specific_merging:
            self._merge()

        if self.main_configs_name is not None:
            self.d = self.d.get(self.main_configs_name)

        assert self.d is not None
        return self.d

    def _map(self, d: dict[str, Any], location: list[str]) -> dict[str, Any]:
        if location is None:
            location = []
        if d is None:
            raise ValueError("Mapper is missing base dict.")

        for k, v in d.items():
            d[k] = self._v_handler(k, v, location + [k])
        return d

    def _v_handler(self, key: str, value: Any, location: list[str]):
        layer = ".".join(location)
        if isinstance(value, str) and self.environ_pattern.is_pattern(value):
            value = self._get_environ_value(key, value)
        elif isinstance(value, str) and self.template_pattern.is_pattern(value):
            value = self._get_template_value(value)
        elif layer in self.overwrite.keys():
            value = self.overwrite.get(layer)
        elif isinstance(value, list):
            value = [self._v_handler(key, elm, location) for elm in value]
        elif isinstance(value, dict):
            value = self._map(value, location)
        return value

    def _get_environ_value(self, key: str, value: str) -> str:
        env = os.environ.get(self.environ_pattern.key(value), None)
        if env is None:
            raise KeyError(f"ENV variable {key} not found.")
        return env.strip()

    def _get_template_value(self, value: str) -> Any:
        return self.template.get(self.template_pattern.key(value), None)

    def _merge(self) -> dict[str, Any]:
        assert self.d is not None
        assert self.main_configs_name is not None

        env = os.environ.get("ENV", None)
        main_config = self.d.get(self.main_configs_name, None)

        if main_config is None:
            raise KeyError(f"Main configuration dict `app` not found.")

        if env is None:
            env = main_config.get("env", None)

        if env is None:
            raise EnvironmentError(f"`ENV` environment variable is unset.")

        env_config = self.d.get(env, {})
        config = defaultdict(dict)
        for key in list(set(list(main_config.keys()) + list(env_config.keys()))):
            mcfg = main_config.get(key, None)
            ecfg = env_config.get(key, None)

            if mcfg is None and ecfg is None:
                continue

            elif not mcfg or not ecfg:
                config[key] = list(filter(None, [mcfg, ecfg]))[0]

            elif type(mcfg) != type(ecfg):
                raise TypeError(f"unmatching types for {key}.")
            elif isinstance(mcfg, dict) and isinstance(ecfg, dict):
                # -- env cfg must override in case of duplicates
                config[key] = {**mcfg, **ecfg}

            else:
                # -- last case, type is not dict, override with env config
                config[key] = ecfg
        return config


class BatchLoader(ConfigLoader):
    def __init__(
        self,
        d: dict[str, Any] | None = None,
        environ_pattern: Pattern = Pattern("${", "}"),
        template_pattern: Pattern = Pattern("{{", "}}"),
        allow_env_specific_merging: bool = False,
        main_configs_name: str | None = None,
    ) -> None:
        super().__init__(
            d,
            environ_pattern,
            template_pattern,
            allow_env_specific_merging,
            main_configs_name,
        )

    @property
    def _loaders(self) -> Generator:
        module = sys.modules[__name__]
        for v in module.__dict__.values():
            if isinstance(v, type) and issubclass(v, Loader) and v != Loader:
                yield v

    def load_batch(self, paths: list[str]) -> list[dict[str, Any]]:
        return self._load_batch(paths)

    def merge_batch(
        self,
        paths: list[str],
    ) -> dict[str, Any]:
        configs = iter(self._load_batch(paths))
        global_configs = defaultdict()
        global_configs.update(next(configs))
        for config in configs:
            for k, v in config.items():
                global_configs[k].update(v)
        return global_configs

    def _load_batch(self, paths: list[str]) -> list[dict[str, Any]]:
        configs = []
        for g in paths:
            for path in glob.glob(g):
                path = Path(path)
                loader = self._select_loader(path)
                config = self.load(path, loader=loader)
                configs.append(config)
        return configs

    def _select_loader(self, path: Path) -> Type[Loader]:
        for loader in self._loaders:
            if path.suffix in loader.EXTS:
                return loader
        raise ValueError(f"{path.suffix} currently not handler")