from pyspark.sql import functions as F
from src.extract.extractor import Extractor

def test_normalize_uri():
    assert Extractor.normalize_uri("/tmp/path") == "/tmp/path/"
    assert Extractor.normalize_uri("/tmp/path/") == "/tmp/path/"

def test_column_selector_and_filter(tmp_path, spark):
    csv = tmp_path / "data.csv"
    csv.write_text("name,age,city\nAlice,30,Paris\nBob,40,Lyon\n", encoding="utf-8")

    extr = Extractor(
        columns={"type": "include", "list": ["name", "age", "city"]},
        options={"header": "true", "inferSchema": "true"},
        format="csv"
    )
    df = extr.extract(str(csv), spark)
    assert set(df.columns) == {"name", "age", "city"}

    extr_excl = Extractor(
        columns={"type": "exclude", "list": "age"},
        options={"header": "true", "inferSchema": "true"},
        format="csv"
    )
    df2 = extr_excl.extract(str(csv), spark)
    assert "age" not in df2.columns

    extr_filter = Extractor(
        filter_on="city = 'Paris'",
        options={"header": "true", "inferSchema": "true"},
        format="csv"
    )
    df3 = extr_filter.extract(str(csv), spark)
    assert df3.count() == 1
    assert df3.collect()[0]["name"] == "Alice"
