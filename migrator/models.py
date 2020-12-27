from __future__ import annotations

import os.path
import glob
import yaml
import abc
from pydantic import BaseModel, root_validator
from dataclasses import dataclass, field
from typing import List, Union, Optional

def get_revision_number(filename):
    return int(os.path.basename(filename).split("-", 1)[0])

def load_yaml(fname):
    with open(fname) as f:
        return yaml.safe_load(f.read())

@dataclass
class Repo:
    revisions: List[Revision]

    @staticmethod
    def parse(dir: str) -> Repo:
        assert os.path.isdir(dir)
        revisions = []
        for f in glob.glob(os.path.join(dir, "*.yml")):
            revisions.append(Revision.parse(f))
        revisions.sort(key=lambda r: r.number)
        # check no missing revisions
        assert revisions[0].number == 1
        for (cur, next) in zip(revisions[:-1], revisions[1:]):
            assert cur.number + 1 == next.number
        return Repo(revisions)

@dataclass
class Revision:
    # TODO validate
    number: int
    migration_filename: str

    @property
    def schema_filename(self):
        return os.path.join(
            os.path.dirname(self.migration_filename),
            f"{self.number}-schema.sql"
        )

    @staticmethod
    def parse(filename: str) -> Revision:
        assert os.path.isfile(filename)
        number = get_revision_number(filename)
        return Revision(number, filename)

    @property
    def migration(self) -> Migration:
        return Migration.parse_obj(load_yaml(self.migration_filename))

class Migration(BaseModel):
    revision: int
    message: str
    pre_deploy: List[StepWrapper] = []
    post_deploy: List[StepWrapper] = []

class AbstractStep(abc.ABC):
    pass

class StepWrapper(BaseModel):
    run_ddl: Optional[DDLStep] = None

    @property
    def step(self) -> AbstractStep:
        result = self.run_ddl
        assert result is not None
        return result

class DDLStep(AbstractStep, BaseModel):
    up: str
    down: str

class OtherStep(AbstractStep, BaseModel):
    up: str
    down: str

AnyStep = Union[DDLStep, OtherStep]

for s in BaseModel.__subclasses__(): # type: ignore
    s.update_forward_refs() # type: ignore
