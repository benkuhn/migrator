from __future__ import annotations

import abc
import dataclasses
from typing import List, Optional

import pydantic
from pydantic import BaseModel, PrivateAttr

from . import models, db


class StepWrapper(pydantic.BaseModel):
    run_ddl: Optional[DDLStep] = None
    create_index: Optional[CreateIndex] = None
    drop_index: Optional[DropIndex] = None

    @property
    def step(self) -> AbstractStep:
        result = self.run_ddl or self.create_index or self.drop_index
        assert result is not None
        return result


class AbstractStep(abc.ABC):
    @property
    def subphases(self) -> List[Subphase]:
        return self._subphases()

    @abc.abstractmethod
    def _subphases(self) -> List[Subphase]:
        pass

    @abc.abstractmethod
    def wrap(self) -> StepWrapper:
        pass


class Subphase(abc.ABC):
    @abc.abstractmethod
    def run(self, db: db.Database, part: models.MigrationPart) -> None:
        pass


class TransactionalSubphase(Subphase):
    def run(self, db: db.Database, part: models.MigrationPart) -> None:
        with db.tx():
            audit = db.audit_part_start(part)
            self.run_inner(db)
            db.audit_part_end(audit)

    @abc.abstractmethod
    def run_inner(self, db: db.Database) -> None:
        pass


class IdempotentSubphase(Subphase):
    def run(self, db: db.Database, part: models.MigrationPart) -> None:
        with db.tx():
            # FIXME: what happens if we already started?
            audit = db.audit_part_start(part)
        self.run_inner(db)
        with db.tx():
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


@dataclasses.dataclass
class IdempotentDDLSubphase(IdempotentSubphase):
    up: str
    down: str

    def run_inner(self, db: db.Database) -> None:
        db.cur.execute(self.up)


class DDLStep(BaseModel, AbstractStep):
    up: str
    down: str

    def _subphases(self) -> List[Subphase]:
        return [DDLSubphase(self.up, self.down)]

    def wrap(self) -> StepWrapper:
        return StepWrapper(run_ddl=self)


def q(id: str) -> str:
    """Quotes the identifier"""
    return id


class IndexMixin(BaseModel):
    unique: bool
    name: str
    table: str
    expr: str
    where: Optional[str] = None
    using: Optional[str] = None

    @property
    def create_sql(self) -> str:
        unique = "UNIQUE" if self.unique else ""
        using = f"USING {self.using}" if self.using else ""
        where = f"WHERE {self.where}" if self.where else ""
        return f"""
        CREATE {unique} INDEX CONCURRENTLY IF NOT EXISTS
        {q(self.name)} on {q(self.table)} {using} ({self.expr}) {where}
        """

    @property
    def drop_sql(self) -> str:
        return f"DROP INDEX CONCURRENTLY IF EXISTS {q(self.name)}"


class CreateIndex(IndexMixin, AbstractStep):
    def wrap(self) -> StepWrapper:
        return StepWrapper(create_index=self)

    def _subphases(self) -> List[Subphase]:
        return [IdempotentDDLSubphase(self.create_sql, self.drop_sql)]


class DropIndex(IndexMixin, AbstractStep):
    def wrap(self) -> StepWrapper:
        return StepWrapper(drop_index=self)

    def _subphases(self) -> List[Subphase]:
        return [IdempotentDDLSubphase(self.drop_sql, self.create_sql)]


class OtherStep(AbstractStep, BaseModel):
    up: str
    down: str


