import importlib
from collections import abc
from typing import Type, Union, List
from pyspark.sql import dataframe as SDF
from src.skeleton import InputConfig


class AbstractExtractor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def extract(self, uri: Union[str, List[str]], spark) -> SDF:
        pass

    @staticmethod
    def from_config(config: InputConfig):
        """
        define the child class
        :param config:
        :return:
        """
        print("in from_config")
        mod_name, cls_name = config.extractor.rsplit(".", 1)
        print(mod_name, cls_name, "from config")

        mod = importlib.import_module(mod_name)
        print(mod, "from import")
        cls: Type[AbstractExtractor] = getattr(mod, cls_name)

        print(cls, "from config 2")

        if config.parameters:
            return cls(**config.parameters)
        return cls()
