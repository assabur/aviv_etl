import importlib
from typing import Dict
from pyspark.sql import DataFrame as SDF
from pyspark.sql import SparkSession
from src.transform import AbstractTransform


class FreeStyleTransformer(AbstractTransform):
    def __init__(self, function_name: str, function_parameters=None) -> None:
        self.function_name = function_name
        self.function_parameters = function_parameters
        self.spark = SparkSession.builder.getOrCreate()

    def transform(self, inputs: Dict[str, SDF], spark) -> SDF:
        function_of_transformation = getattr(importlib.import_module("src.transform"), self.function_name)
        return function_of_transformation(inputs, self.function_parameters)
