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
            mode: str = 'overwrite',
            columns: Optional [List[str]] = None
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



        #add eventdate column
        df = df.withColumn("eventdate",
                           F.date_format(F.current_date(), "yyyy-MM-dd")
                           )

        df.show()
        df.printSchema()
        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save("/Users/popylamerveille/PycharmProjects/Etl/datalake/silver/listing")
        print("uri", uri)
        if self.partitions_fields:
            print('in the IFFFFFFFFFf')
            df.write.mode(self.mode).option("compression", "snappy").partitionBy(self.partitions_fields.append("eventdate")).parquet(uri)
        print("uri", uri)
        df.write.mode("overwrite").orc("/Users/popylamerveille/Desktop/output_parquet")

        print("uri",uri)




