from __future__ import annotations

import os.path
import glob
import yaml
import abc
from datetime import datetime
import hashlib
from pydantic import BaseModel, root_validator
from dataclasses import dataclass, field
from typing import List, Union, Optional, Dict, Any

def get_revision_number(filename: str) -> int:
    return int(os.path.basename(filename).split("-", 1)[0])

def load_yaml(fname: str) -> Dict[Any, Any]:
    with open(fname) as f:
        return yaml.safe_load(f.read()) # type: ignore

def sibling(fname: str, path: str) -> str:
    return os.path.join(os.path.dirname(fname), path)
    
@dataclass
class Repo:
    config_path: str
    config: RepoConfig
    revisions: List[Revision]

    @staticmethod
    def parse(config_path: str) -> Repo:
        config = RepoConfig.parse_obj(load_yaml(config_path))
        revisions = Repo.parse_revlist(
            sibling(config_path, config.migrations_dir)
        )
        return Repo(config_path, config, revisions)

    @staticmethod
    def parse_revlist(dir: str) -> List[Revision]:
        assert os.path.isdir(dir)
        revisions = []
        for f in glob.glob(os.path.join(dir, "*.yml")):
            revisions.append(Revision.parse(f))
        revisions.sort(key=lambda r: r.number)
        # check no missing revisions
        # TODO: probably move this to a validation function
        assert revisions[0].number == 1
        for (cur, next) in zip(revisions[:-1], revisions[1:]):
            assert cur.number + 1 == next.number
        return revisions

class RepoConfig(BaseModel):
    schema_dump_command: str
    migrations_dir: str = "migrations"
    crash_on_incompatible_version: bool = True
    
@dataclass
class Revision:
    # TODO validate
    number: int
    migration_filename: str

    @property
    def _migration_text(self) -> str:
        with open(self.migration_filename) as f:
            return f.read()

    @property
    def migration_hash(self) -> bytes:
        return hashlib.sha256(self._migration_text.encode('ascii')).digest()

    @property
    def migration(self) -> Migration:
        return Migration.parse_obj(load_yaml(self.migration_filename))

    @property
    def schema_filename(self) -> str:
        return sibling(self.migration_filename, f"{self.number}-schema.sql")

    @property
    def _schema_text(self) -> str:
        with open(self.schema_filename) as f:
            return f.read()
        
    @property
    def schema_hash(self) -> bytes:
        return hashlib.sha256(self._schema_text.encode('ascii')).digest()

    @staticmethod
    def parse(filename: str) -> Revision:
        assert os.path.isfile(filename)
        number = get_revision_number(filename)
        return Revision(number, filename)

@dataclass
class MigrationPart:
    revision: int
    migration_hash: bytes
    pre_deploy: bool
    phase: int
    subphase: int

@dataclass
class MigrationAudit:
    id: int
    started_at: datetime
    finished_at: Optional[datetime]
    revert_started_at: Optional[datetime]
    revert_finished_at: Optional[datetime]
    part: MigrationPart
    
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

for s in BaseModel.__subclasses__():
    s.update_forward_refs()
