from pyspark.sql import Row
from src.transform.utils import (
    create_audit_columns,
    create_technical_key_col,
    replace_nulls_values,
    create_technical_column,
    clean_types_columns,
)

def test_create_audit_columns_adds_expr_columns(spark):
    df = spark.createDataFrame([Row(a=1)])
    df2 = create_audit_columns(df, {"ingestion_ts": "current_timestamp()"})
    assert "ingestion_ts" in df2.columns

def test_create_technical_key_col_builds_md5(spark):
    df = spark.createDataFrame([Row(a="X", b="Y"), Row(a=None, b="Y")])
    df2 = create_technical_key_col(df, tech_col_name="id_tech", columns=["a", "b"])
    assert "id_tech" in df2.columns
    assert df2.count() == 2



def test_create_technical_column_calls_helper(spark):
    df = spark.createDataFrame([Row(a="1", b="2")])
    out = create_technical_column(df, {"tech": {"id_tech": ["a", "b"]}})
    assert "id_tech" in out.columns


