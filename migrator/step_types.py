from __future__ import annotations

import abc
import dataclasses
from typing import List, Optional

import pydantic
from pydantic import BaseModel, PrivateAttr

from . import models, db


@pydantic.dataclasses.dataclass
class StepWrapper:
    _migration: models.Migration = PrivateAttr()
    _first_subphase: models.MigrationPart = PrivateAttr()
    run_ddl: Optional[DDLStep] = None

    @property
    def step(self) -> AbstractStep:
        result = self.run_ddl
        assert result is not None
        return result

    def __post_init_post_parse__(self) -> None:
        self.step._parent = self

    @property
    def parts(self) -> List[models.MigrationPart]:
        return [
            dataclasses.replace(self._first_subphase, subphase=s)
            for s in range(len(self.step.subphases))
        ]

    def next_parts(self, part: models.MigrationPart) -> List[models.MigrationPart]:
        assert part.first_subphase == self._first_subphase
        return self.parts[part.subphase + 1:]


class AbstractStep(abc.ABC):
    _parent: StepWrapper
    _subphases_saved: List[Subphase] = []

    @property
    def subphases(self) -> List[Subphase]:
        if self._subphases_saved == []:
            self._subphases_saved = self._subphases()
            for i, s in enumerate(self.subphases):
                s.parent = self
                s.part = dataclasses.replace(self._parent._first_subphase, subphase=i)
        return self._subphases_saved

    @abc.abstractmethod
    def _subphases(self) -> List[Subphase]:
        pass

    def get(self, part: models.MigrationPart) -> Subphase:
        assert part.first_subphase == self._parent._first_subphase
        return self.subphases[part.subphase]

class Subphase(abc.ABC):
    parent: AbstractStep
    part: models.MigrationPart

    @abc.abstractmethod
    def run(self, db: db.Database) -> None:
        pass


class TransactionalSubphase(Subphase):

    def run(self, db: db.Database) -> None:
        with db.tx():
            audit = db.audit_part_start(self.part)
            self.run_inner(db)
            db.audit_part_end(audit)

    @abc.abstractmethod
    def run_inner(self, db: db.Database) -> None:
        pass


@dataclasses.dataclass
class DDLSubphase(TransactionalSubphase):
    up: str
    down: str

    def run_inner(self, db: db.Database) -> None:
        db.cur.execute(self.up)


@pydantic.dataclasses.dataclass
class DDLStep(AbstractStep):
    up: str
    down: str

    def _subphases(self) -> List[Subphase]:
        return [DDLSubphase(self.up, self.down)]

class OtherStep(AbstractStep, BaseModel):
    up: str
    down: str


