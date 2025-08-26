from typing import Dict
from pyspark.sql import DataFrame as SDF
from src.load import AbstractLoader, ParquetLoader
from src.transform import AbstractTransform
from src.skeleton import Config
from src.validators import AbstractValidator
from src.extract import AbstractExtractor


class Pipeline:
    def __init__(self, config: Config, spark) -> None:
        """
        Parameters
        -----------
        config: Config
            The config for one job (workflow)
        spark: SparkSession
            The spark session that will be used through all the project
        """
        self.config = config
        self.spark = spark


    def set_extract_results(self, outputs: Dict[str, SDF]) -> None:
        """
        This function will run the extract part of the workflow
        Parameters
        ----------
        outputs: Dict[str, SDF] : map contains as a key the name of
        the dataframe and the value is the dataframe itself
        """

        for config in self.config.inputs:
            outputs[config.name] = AbstractExtractor.from_config(
                config
            ).extract(uri=config.uri, spark=self.spark)

    def set_quality_gate(self, outputs: Dict[str, SDF]) -> None:
        """
        This function will run the rules for the pre-validations of the dataframe
        Parameters like data quality checks etc
        ----------
        outputs: dataframe or json store on the silver layer to make dashboards later
        """

        if not self.config.quality_gate:
            return
        for config in self.config.quality_gate:
            if isinstance(config.input, str):
                print("In Quality Gate Section")
                AbstractValidator.from_config(config).validate(
                    outputs[config.input], self.spark)
            if isinstance(config.input, dict):
                inputs: Dict[str, SDF] = {}
                for k, v in config.input.items():
                    inputs[k] = outputs[v]
                AbstractValidator.from_config(config).validate(
                    inputs, self.spark)

    def set_transform_results(self, outputs: Dict[str, SDF]) -> None:
        if not self.config.transforms:
            return
        for config in self.config.transforms:
            inputs: Dict[str, SDF] = {}
            if isinstance(config.inputs, dict):
                for k, v in config.inputs.items():
                    inputs[k] = outputs[v]
                outputs[config.name] = AbstractTransform.from_config(
                    config
                ).transform(inputs, self.spark)
            if isinstance(config.inputs, str):
                outputs[config.name] = AbstractTransform.from_config(
                    config
                ).transform(outputs[config.inputs], self.spark)
            if config.inputs is None:
                outputs[config.name] = AbstractTransform.from_config(
                    config
                ).transform(None, self.spark)


    def set_load_results(self, outputs: Dict[str, SDF]) -> None:
        """
        This function will load the dataframe ; on S3; MongoDb or Atlas
        Parameters
        ----------
        outputs: Dict[str, SDF] : map contains as a key the name of
        the dataframe and the value is the dataframe itself
        """
        print("In Load Section")

        for config in self.config.outputs:
            dataframe = outputs[config.input]

            """outputs[config.name] = ParquetLoader(config).load(
                           dataframe, config.uri,
                           self.spark)"""

            outputs[config.name] = AbstractLoader.from_config(config).load(
                dataframe, config.uri,
                self.spark)


    def run(self) -> Dict[str, SDF]:
        outputs: Dict[str, SDF] = {}
        self.set_extract_results(outputs)
        self.set_quality_gate(outputs)
        self.set_transform_results(outputs)
        self.set_load_results(outputs)
        #self.spark.catalog.clearCache()

        return outputs
