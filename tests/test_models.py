import yaml
from migrator import models

def load_yaml(fname):
    with open(fname) as f:
        return yaml.safe_load(f.read())
        
def test_basic_parse():
    m = models.Migration.parse_obj(load_yaml("test/migrations/1-create-table.yml"))
    assert m.post_deploy == []
    [step1] = m.pre_deploy
    assert step1.run_ddl is not None

