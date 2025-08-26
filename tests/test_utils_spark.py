from src.utils.spark import SparkHelper

def test_setup_core_config_contains_expected_keys():
    helper = SparkHelper()
    cfg = helper.setup_core_config()
    expected = {"spark.app.name", "spark.master"}
    assert expected.issubset(set(cfg.keys()))
