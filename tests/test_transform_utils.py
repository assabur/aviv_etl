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

def test_replace_nulls_values(spark):
    df = spark.createDataFrame([Row(a=None, b=None)])
    df2 = replace_nulls_values(df, {"columns_to_clean": {"a": "NA", "b": "0"}})
    r = df2.collect()[0]
    assert r["a"] == "NA" and r["b"] == "0"

def test_create_technical_column_calls_helper(spark):
    df = spark.createDataFrame([Row(a="1", b="2")])
    out = create_technical_column(df, {"tech": {"id_tech": ["a", "b"]}})
    assert "id_tech" in out.columns

def test_clean_types_columns_strips_prefix(spark):
    df = spark.createDataFrame([Row(t="ITEM_TYPE.HOUSE"), Row(t="TRANSACTION_TYPE.SELL"), Row(t="OTHER")])
    out = clean_types_columns(df, {"columns_to_clean": ["t"]})
    vals = [r["t"] for r in out.orderBy("t").collect()]
    assert set(vals) == {"HOUSE", "OTHER", "SELL"}
