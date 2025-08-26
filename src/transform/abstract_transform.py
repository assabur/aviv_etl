import importlib
import abc
from typing import Type, Union, Dict
from src.skeleton import TransformConfig
from pyspark.sql import dataframe as SDF


class AbstractTransform(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def transform(self, inputs: Union[Dict[str, SDF], SDF], spark) -> SDF:
        pass

    @staticmethod
    def from_config(config: TransformConfig) -> "AbstractTransform":
        """
        define the child class
        :param config:
        :return:
        """
        print(f"{config.name} ==>{config.transformer}")
        print(config)
        mod_name, cls_name = config.transformer.rsplit(".", 1)
        mod = importlib.import_module(mod_name)
        print(f"{mod_name} ==>{cls_name}")
        cls: Type[AbstractTransform] = getattr(mod, cls_name)
        if config.parameters:
            return cls(**config.parameters)
        return cls()
