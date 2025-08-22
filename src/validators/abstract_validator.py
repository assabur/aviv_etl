from abc import abstractmethod, ABCMeta
from importlib import import_module
from typing import Union, Dict, Type
from pyspark.sql import dataframe as SDF

from src.skeleton import ValidationConfig


class AbstractValidator(metaclass=ABCMeta):
    @abstractmethod
    def validate(self, df: Union[SDF, Dict[str, SDF]], spark) -> bool:
        pass

    @staticmethod
    def from_config(config: ValidationConfig) -> 'AbstractValidator':
        mod_name, cls_name = config.validator.rsplit('.', 1)
        mod = import_module(mod_name)
        cls: Type[AbstractValidator] = getattr(mod, cls_name)
        if config.parameters:
            return kls(**config.parameters)  # type: ignore
        return cls()