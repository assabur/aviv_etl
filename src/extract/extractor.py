from dataclasses import dataclass
from typing import List, Union, Any, Dict, Optional
from pyspark.sql import DataFrame as SDF
from src.extract import AbstractExtractor
import pyspark.sql.types as T


@dataclass
class Extractor(AbstractExtractor):
    columns: Optional[Dict[str, Any]] = None
    filter_on: Optional[str] = None
    format: str = "csv"
    options: Optional[Dict[str, str]] = None
    ddl_schema: Optional[str] = None

    def extract(self, uri: Union[str, List[str]], spark) -> SDF:
        print(f"Extracting of files to the following uri {uri}")

        spark_schema = T._parse_datatype_string(self.ddl_schema) if self.ddl_schema is not None else None
        #spark_schema = DataType.fromDDL(self.ddl_schema) if self.ddl_schema is not None else None

        print("in the  extract  Extractor...")
        if self.options is None:
            dataframe = spark.read.format(self.format)
        else:
            dataframe = spark.read.format(self.format).options(**self.options)

        if spark_schema is not None:
            dataframe = dataframe.schema(spark_schema).load(uri)
        else:
            dataframe = dataframe.load(uri)

        if self.columns is not None:
            if self.columns["type"] == "include":
                dataframe = dataframe.selectExpr(self.columns["list"])
            if self.columns["type"] == "exclude":
                dataframe = dataframe.drop(self.columns["list"])

        if self.filter_on is not None:
            return dataframe.filter(self.filter_on)

        return dataframe

    @staticmethod
    def normalize_uri(path):
        if path.endswith("/"):
            return path
        else:
            return path + '/'

    def column_selector(self, dataframe):
        if self.columns is not None:
            if self.columns["type"] == "include":
                dataframe = dataframe.selectExpr(self.columns["list"])
            if self.columns["type"] == "exclude":
                dataframe = dataframe.drop(self.columns["list"])
        return dataframe

    def apply_filter(self, dataframe):
        if self.filter_on is not None:
            return dataframe.filter(self.filter_on)
        return None
