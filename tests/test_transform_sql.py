from pyspark.sql import Row
from src.transform.sql_transformer import SqlTransformer

def test_sql_transformer_basic(spark):
    df = spark.createDataFrame([Row(x=1), Row(x=2), Row(x=3)])
    tr = SqlTransformer(sql="select sum(x) as s from t")
    out = tr.transform({"t": df}, spark)
    row = out.collect()[0]
    assert row["s"] == 6
