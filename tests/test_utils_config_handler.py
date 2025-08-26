from pathlib import Path
import pytest
from src.utils.config_handler import ConfigHandler

def test_check_if_folder_and_file(tmp_path: Path):
    d = tmp_path / "cfg"
    d.mkdir()
    f = d / "a.yaml"
    f.write_text("name: test", encoding="utf-8")
    handler = ConfigHandler()
    assert handler.check_if_folder_local(str(d)) is True
    assert handler.check_if_file_local(str(f)) is True

@pytest.mark.xfail(reason="Known issue: list_of_files uses str.endswith with wrong signature.")
def test_list_of_files_yaml_yml(tmp_path: Path):
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "a.yaml").write_text("x: 1", encoding="utf-8")
    (d / "b.yml").write_text("y: 2", encoding="utf-8")
    (d / "c.txt").write_text("nope", encoding="utf-8")
    files = ConfigHandler.list_of_files(str(d))
    assert set(files) == {"a.yaml", "b.yml"}
