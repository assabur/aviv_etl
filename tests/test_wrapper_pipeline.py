from types import SimpleNamespace
from src.wrapper_pipeline import WrapperPipeline

def test_enable_jobs_sorts_and_filters_enabled():
    cfgs = [
        SimpleNamespace(name="job3", enable=True, order=3),
        SimpleNamespace(name="job1", enable=True, order=1),
        SimpleNamespace(name="job2", enable=False, order=2),
    ]
    group = SimpleNamespace(configs=cfgs)
    wp = WrapperPipeline(configs=group, spark=None)
    names = [c.name for c in wp.enable_jobs]
    assert names == ["job1", "job3"]
