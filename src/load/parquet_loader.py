from typing import Optional, List
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SDF


class ParquetLoader:
    """
    This class is about the Parquet format loader
    """

    def __init__(
        self,
        table_name: str = None,
        partitions_fields: Optional[List[str]] = [],
        num_partitions: Optional[int] = None,
        mode: str = "overwrite",
        columns: Optional[List[str]] = None,
    ) -> None:
        """
        :param table_name:   table_name to create or to update
        :param mode: write mode , overwrite, error etc
        """
        self.num_partitions = num_partitions
        self.mode = mode
        self.table_name = table_name
        self.partitions_fields = partitions_fields
        self.columns = columns

    def load(self, df: SDF, uri: str, spark) -> None:
        partition_col = "eventdate"
        df = df.withColumn(partition_col, F.date_format(F.current_date(), "yyyy-MM-dd"))

        df.printSchema()
        df.write.mode("overwrite").format("parquet").partitionBy(partition_col).save(uri)
        print("uri", uri)
        if self.partitions_fields:
            df.write.mode(self.mode).partitionBy(self.partitions_fields.append("eventdate")).parquet(uri)
