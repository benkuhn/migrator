from __future__ import annotations

import abc
import dataclasses
from typing import List, Optional

from pydantic import BaseModel, PrivateAttr

from . import models, db


class StepWrapper(BaseModel):
    _migration: models.Migration = PrivateAttr()
    _first_subphase: models.MigrationPart = PrivateAttr()
    run_ddl: Optional[DDLStep] = None

    @property
    def step(self) -> AbstractStep:
        result = self.run_ddl
        assert result is not None
        return result

    @property
    def parts(self) -> List[models.MigrationPart]:
        return [dataclasses.replace(self._first_subphase, subphase=s) for s in range(self.step.n_subphases())]

    def next_parts(self, part: models.MigrationPart) -> List[models.MigrationPart]:
        assert part.first_subphase == self._first_subphase
        return self.parts[part.subphase + 1:]


class AbstractStep(abc.ABC):
    parent: StepWrapper

    @abc.abstractmethod
    def n_subphases(self) -> int:
        pass

    @abc.abstractmethod
    def subphases(self) -> List[Subphase]:
        pass

    def get(self, part: models.MigrationPart) -> Subphase:
        assert part.first_subphase == self.parent._first_subphase
        return self.subphases()[part.subphase]

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

class DDLStep(AbstractStep, BaseModel):
    up: str
    down: str

    def n_subphases(self) -> int:
        return 0 # FIXME

    def subphases(self) -> List[Subphase]:
        return []

class OtherStep(AbstractStep, BaseModel):
    up: str
    down: str


