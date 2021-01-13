from typing import Any

import yaml
from migrator import models


def load_yaml(fname: str) -> Any:
    with open(fname) as f:
        return yaml.safe_load(f.read())


def test_basic_parse() -> None:
    r = models.Repo.parse("test/migrator.yml")
    m = r.revisions[1].migration
    assert m.post_deploy == []
    [step1] = m.pre_deploy
    assert step1.run_ddl is not None
