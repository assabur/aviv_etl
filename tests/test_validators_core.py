import os
from pyspark.sql import Row
from src.validators.dataframe_validator import CoreValidator

def test_run_ge_rule_to_ndjson_writes_file(tmp_path, spark, monkeypatch):
    df = spark.createDataFrame([Row(a=1), Row(a=None), Row(a=3)])

    silver = tmp_path.as_posix() + "/"
    monkeypatch.setenv("SILVER", silver)

    validator = CoreValidator(
        columns="a",
        rule="expect_column_values_to_not_be_null",
        dataset_name="df_test",
        output_dir="ge_events/",
        output_table_name="unit",
        result_format="BASIC",
    )
    out_path = validator.run_ge_rule_to_ndjson(
        dataframe=df,
        rule="expect_column_values_to_not_be_null",
        columns="a",
        rule_kwargs={"column": "a"},
        dataset_name="df_test",
        output_dir="ge_events/",
        output_table_name="unit",
    )
    assert os.path.exists(out_path)
    with open(out_path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    assert len(lines) >= 1
