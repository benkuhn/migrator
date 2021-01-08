import yaml
from migrator import models

def load_yaml(fname):
    with open(fname) as f:
        return yaml.safe_load(f.read())
        
def test_basic_parse():
    r = models.Repo.parse("test/migrator.yml")
    m = r.revisions[0].migration
    assert m.post_deploy == []
    [step1] = m.pre_deploy
    assert step1.run_ddl is not None
