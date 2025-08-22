from abc import ABCMeta, abstractmethod
from importlib import import_module
from typing import Type
from pyspark.sql import DataFrame as SDF
from src.skeleton import OutputConfig


class AbstractLoader(metaclass=ABCMeta):
    """
    This is a abstract class that will be inerit by all loaders
    """
    @abstractmethod
    def load(self, df: SDF, uri: str, spark) -> SDF:
        pass

    @staticmethod
    def from_config(config: OutputConfig) -> 'AbstractLoader':
        mod_name, cls_name = config.loader.rsplit('.', 1)
        mod = import_module(mod_name)
        cls: Type[AbstractLoader] = getattr(mod, cls_name)
        if config.parameters:
            return kls(**config.parameters)  # type: ignore
        return cls()