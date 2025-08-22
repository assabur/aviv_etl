from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
spark = (
    SparkSession.builder
    .appName("CSV_to_Postgres")
    .master("local[*]")
    # Laisse Spark récupérer le driver JDBC PostgreSQL automatiquement
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
    .config("spark.driver.memory", "2g")   # optionnel
    .getOrCreate()
)

# Lecture CSV
df = spark.read.csv(
    "/Users/popylamerveille/PycharmProjects/aviv_etl/datalake/raw/sample.csv",
    header=True,
    inferSchema=True
)

df.show(5)

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")

# Connexion PostgreSQL
pg_url = f"jdbc:postgresql://localhost:5432/{db_name}"
pg_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# Écriture (schéma explicite recommandé)
df.write \
  .mode("overwrite") \
  .option("truncate", "true") \
  .jdbc(url=pg_url, table="public.my_table", properties=pg_properties)

print("Données écrites dans public.my_table")
