from __future__ import annotations

import dataclasses
import os.path
import glob
from contextlib import contextmanager

import pydantic
import yaml
import abc
from datetime import datetime
import hashlib
from pydantic import BaseModel, root_validator, PrivateAttr
from dataclasses import dataclass, field
from typing import List, Union, Optional, Dict, Any, Iterator, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from . import step_types

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
    revisions: Dict[int, Revision]

    @staticmethod
    def parse(config_path: str) -> Repo:
        config = RepoConfig.parse_obj(load_yaml(config_path))
        revisions = Repo.parse_revlist(
            sibling(config_path, config.migrations_dir)
        )
        return Repo(config_path, config, revisions)

    @staticmethod
    def parse_revlist(dir: str) -> Dict[int, Revision]:
        assert os.path.isdir(dir)
        revisions = {}
        for f in glob.glob(os.path.join(dir, "*.yml")):
            rev = Revision.parse(f)
            revisions[rev.number] = rev
        assert list(revisions.keys()) == list(range(1, len(revisions) + 1))
        return revisions

    @property
    def ordered_revisions(self) -> Iterator[Tuple[int, Revision]]:
        yield from sorted(self.revisions.items())

    def next_parts(self, part: Optional[MigrationPart]) -> Iterator[MigrationPart]:
        """Yields each remaining migration-part that should be run after the given part.

        If the part refers to a migration not in the repo, raises MigrationNotFound.
        """
        found_part = (part is None)
        subphase_0 = None if part is None else dataclasses.replace(part, subphase=0)
        for num, revision in self.ordered_revisions:
            for step in revision.migration.pre_deploy:
                if found_part:
                    yield from step.parts
                if step._first_subphase == subphase_0:
                    found_part = True
                    yield from step.next_parts(part)

    def get(self, part: MigrationPart) -> Tuple[Revision, step_types.StepWrapper, step_types.Subphase]:
        rev = self.revisions[part.revision]
        sw = rev.migration.get(part)
        subphase = sw.step.get(part)
        return (rev, sw, subphase)


class RepoConfig(BaseModel):
    schema_dump_command: str
    migrations_dir: str = "migrations"
    crash_on_incompatible_version: bool = True


class ValidationError(Exception):
    def __init__(self, filename: str, inner: pydantic.ValidationError) -> None:
        super().__init__(f"File {filename}:\n{inner}")
        self.filename = filename
        self.inner = inner

@contextmanager
def parsing_file(filename: str) -> Iterator[None]:
    try:
        yield None
    except pydantic.ValidationError as e:
        raise ValidationError(filename, e) from e

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
        with parsing_file(self.migration_filename):
            m = Migration(parent=self, **load_yaml(self.migration_filename))
            return m

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
    schema_hash: bytes
    pre_deploy: bool
    phase: int
    subphase: int

    @property
    def first_step(self) -> MigrationPart:
        return dataclasses.replace(self, pre_deploy=True, phase=0, subphase=0)

    @property
    def first_subphase(self) -> MigrationPart:
        return dataclasses.replace(self, subphase=0)

@dataclass
class MigrationAudit:
    id: int
    started_at: datetime
    finished_at: Optional[datetime]
    revert_started_at: Optional[datetime]
    revert_finished_at: Optional[datetime]
    part: MigrationPart

@pydantic.dataclasses.dataclass
class Migration:
    message: str
    parent: Revision
    pre_deploy: List[step_types.StepWrapper] = dataclasses.field(default_factory=list)
    post_deploy: List[step_types.StepWrapper] = dataclasses.field(default_factory=list)

    @property
    def first_step(self) -> MigrationPart:
        return MigrationPart(
            revision=self.parent.number,
            migration_hash=self.parent.migration_hash,
            schema_hash=self.parent.schema_hash,
            pre_deploy=True,
            phase=0,
            subphase=0
        )

    def get(self, part: MigrationPart) -> step_types.StepWrapper:
        assert part.first_step == self.first_step, f"{part.first_step} != {self.first_step}"
        steps = self.pre_deploy if part.pre_deploy else self.post_deploy
        return steps[part.phase]


    def __post_init_post_parse__(self) -> None:
        self._populate_wrapper_fields(self.pre_deploy, True)
        self._populate_wrapper_fields(self.post_deploy, False)

    def _populate_wrapper_fields(self, ws: List[step_types.StepWrapper], pre_deploy: bool) -> None:
        for phase, sw in enumerate(ws):
            sw._migration = self
            sw._first_subphase = MigrationPart(
                revision=self.parent.number,
                migration_hash=self.parent.migration_hash,
                schema_hash=self.parent.schema_hash,
                pre_deploy=pre_deploy,
                phase=phase,
                subphase=0
            )


from . import step_types
for s in BaseModel.__subclasses__():
    s.update_forward_refs()
