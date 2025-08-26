from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


class SparkHelper:
    def __init__(self):
        pass

    def build_config(self):
        base_configs = {**self.setup_core_config()}
        return base_configs

    def get_spark(self):
        configs = self.build_config()
        tuple_configs = [(k, v) for k, v in configs.items()]
        spark_config = SparkConf().setAll(tuple_configs)
        spark_context = SparkContext.getOrCreate(conf=spark_config)
        spark = SparkSession.builder.getOrCreate()
        spark_context.setLogLevel("ERROR")
        return spark

    @staticmethod
    def setup_core_config():
        return {
            "spark.sql.parquet.writeLegacyFormat": "true",
            "spark.sql.session.timeZone": "UTC",
            "spark.jars.packages": "org.postgresql:postgresql:42.7.4",
            "spark.app.name": "MyLocalApp",
            "spark.master": "local[4]",  # 4 threads
            "spark.driver.memory": "4g",  # mémoire driver
            "spark.executor.memory": "2g",  # mémoire executors
            "spark.executor.cores": "2",  # nb de cores par executor
            "spark.sql.shuffle.partitions": "8",
        }
