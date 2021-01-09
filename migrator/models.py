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
    from . import changes

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
        for num, revision in self.ordered_revisions:
            if part and part.revision < num:
                continue
            if part and part.revision == num:
                assert revision.first_step == part.first_step  # FIXME error
            for next_part, step, subphase in revision.next_parts(part):
                yield next_part, revision, step, subphase


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
        # FIXME: hack so that we can create a partial revision in order to serialize a
        # migration...
        if not os.path.exists(self.migration_filename):
            return ''
        with open(self.migration_filename) as f:
            return f.read()

    @property
    def migration_hash(self) -> bytes:
        return hashlib.sha256(self._migration_text.encode('ascii')).digest()

    @property
    def migration(self) -> Migration:
        with parsing_file(self.migration_filename):
            m = Migration(**load_yaml(self.migration_filename))
            return m

    @property
    def schema_filename(self) -> str:
        return sibling(self.migration_filename, f"{self.number}-schema.sql")

    @property
    def _schema_text(self) -> str:
        # FIXME: hack so that we can create a partial revision in order to serialize a
        # migration...
        if not os.path.exists(self.migration_filename):
            return ''
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

    def next_parts(self, part: Optional[MigrationPart]) -> Iterator[PartStepSubphase]:
        if part and part.revision > self.number:
            return
        for next_part, step, subphase in self.parts():
            if not part or next_part > part:
                yield next_part, step, subphase


    def parts(self) -> Iterator[PartStepSubphase]:
        part = self.first_step
        for i_phase, sw in enumerate(self.migration.pre_deploy):
            for i_subphase, subphase in enumerate(sw.inner.phases):
                newpart = dataclasses.replace(part, phase=i_phase, subphase=i_subphase)
                yield newpart, sw, subphase


    @property
    def first_step(self) -> MigrationPart:
        return MigrationPart(
            revision=self.number,
            migration_hash=self.migration_hash,
            schema_hash=self.schema_hash,
            pre_deploy=True,
            phase=0,
            subphase=0
        )


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

    @property
    def sortkey(self) -> Tuple[int, int, int, int]:
        return (
            self.revision,
            0 if self.pre_deploy else 1,
            self.phase,
            self.subphase
        )

    def __gt__(self, other: MigrationPart) -> bool:
        return self.sortkey > other.sortkey

PartStepSubphase = Tuple[MigrationPart, "changes.StepWrapper", "changes.Subphase"]
PartRevisionStepSubphase = Tuple[
    MigrationPart, Revision, "changes.StepWrapper", "changes.Subphase"
]



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
    pre_deploy: List[changes.Change] = dataclasses.field(default_factory=list)
    post_deploy: List[changes.Change] = dataclasses.field(default_factory=list)

from . import changes
for s in BaseModel.__subclasses__():
    s.update_forward_refs()
