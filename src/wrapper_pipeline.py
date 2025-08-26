import time
from src.pipeline import Pipeline
from src.skeleton import GroupConfig
from pyspark.sql import SparkSession


class WrapperPipeline:
    """
    this class is the wrapper of the job
    It contains a group of jobs,
    And this group it contains in the class GroupConfig
    And the function launch allow to launch the set of job sequentially.

    """

    def __init__(self, configs: GroupConfig, spark: SparkSession) -> None:
        self.configs = configs
        self.spark = spark
        self.status = []

    def launch(self):
        errors = []
        for config in self.enable_jobs:
            start_time = time.time()
            try:
                pipeline = Pipeline(config, spark=self.spark)
                pipeline.run()
                end_time = time.time()
                job_duration = round(end_time - start_time, 1)
                self.status.append((config.name, job_duration, "Success"))
                print(f"********************* {config.name} ===> {job_duration} seconds *************************")
            except Exception as exception:
                self.status.append((config.name, 0, "Fails"))
                errors.append((config.name, exception))

    @property
    def enable_jobs(self):
        configs = sorted(
            [config for config in self.configs.configs if config.enable],
            key=lambda x: x.order,
        )
        return configs
