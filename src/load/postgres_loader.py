import os
from typing import Optional, List
from dotenv import load_dotenv
from pyspark.sql import DataFrame as SDF


class PostgresLoader:
    """
    This class is about to store data in Postgres table
    """

    def __init__(
        self,
        table_name: str = None,
        mode: str = "overwrite",
        columns: Optional[List[str]] = None,
    ) -> None:
        """
        :param table_name:   table_name to create or to update
        :param mode: write mode , overwrite, error etc
        """
        self.mode = mode
        self.table_name = table_name
        self.columns = columns

    def load(self, df: SDF, uri: str, spark) -> None:
        load_dotenv()

        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        # Connexion PostgreSQL
        pg_url = f"jdbc:postgresql://localhost:5432/{db_name}"
        pg_properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver",
        }
        print("Before load", self.table_name)
        df.write.mode("overwrite").option("truncate", "true").jdbc(
            url=pg_url, table=self.table_name, properties=pg_properties
        )

        print(f"write {self.table_name} on postgresql")
