from __future__ import annotations

import abc
import dataclasses
from typing import List, Optional

import pydantic
from pydantic import BaseModel, PrivateAttr

from . import models, db


class Change(pydantic.BaseModel):
    run_ddl: Optional[DDLStep] = None
    create_index: Optional[CreateIndex] = None
    drop_index: Optional[DropIndex] = None
    add_check_constraint: Optional[AddCheckConstraint] = None
    drop_check_constraint: Optional[DropCheckConstraint] = None

    @property
    def inner(self) -> AbstractChange:
        result = (
                self.run_ddl
                or self.create_index
                or self.drop_index
                or self.add_check_constraint
                or self.drop_check_constraint
        )
        assert result is not None
        return result


class AbstractChange(abc.ABC):
    @property
    def phases(self) -> List[Phase]:
        return self._phases()

    @abc.abstractmethod
    def _phases(self) -> List[Phase]:
        pass

    @abc.abstractmethod
    def wrap(self) -> Change:
        pass


class Phase(abc.ABC):
    @abc.abstractmethod
    def run(self, db: db.Database, index: models.PhaseIndex) -> None:
        pass


class TransactionalPhase(Phase):
    def run(self, db: db.Database, index: models.PhaseIndex) -> None:
        with db.tx():
            audit = db.audit_phase_start(index)
            self.run_inner(db)
            db.audit_phase_end(audit)

    @abc.abstractmethod
    def run_inner(self, db: db.Database) -> None:
        pass


class IdempotentPhase(Phase):
    def run(self, db: db.Database, index: models.PhaseIndex) -> None:
        with db.tx():
            # FIXME: what happens if we already started?
            audit = db.audit_phase_start(index)
        self.run_inner(db)
        with db.tx():
            db.audit_phase_end(audit)

    @abc.abstractmethod
    def run_inner(self, db: db.Database) -> None:
        pass


@dataclasses.dataclass
class TransactionalDDLPhase(TransactionalPhase):
    up: Optional[str]
    down: Optional[str]

    def run_inner(self, db: db.Database) -> None:
        if self.up:
            db.cur.execute(self.up)


@dataclasses.dataclass
class IdempotentDDLPhase(IdempotentPhase):
    up: Optional[str]
    down: Optional[str]

    def run_inner(self, db: db.Database) -> None:
        if self.up:
            db.cur.execute(self.up)


class DDLStep(BaseModel, AbstractChange):
    up: str
    down: str

    def _phases(self) -> List[Phase]:
        return [TransactionalDDLPhase(self.up, self.down)]

    def wrap(self) -> Change:
        return Change(run_ddl=self)


def q(id: str) -> str:
    """Quotes the identifier"""
    return id


class IndexMixin(BaseModel):
    unique: bool = False
    name: str
    table: str  # TODO: I'd prefer to use "on" here but it needs to be escaped :(
    expr: str
    using: Optional[str] = None
    where: Optional[str] = None

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


class CreateIndex(IndexMixin, AbstractChange):
    def wrap(self) -> Change:
        return Change(create_index=self)

    def _phases(self) -> List[Phase]:
        return [IdempotentDDLPhase(self.create_sql, self.drop_sql)]


class DropIndex(IndexMixin, AbstractChange):
    def wrap(self) -> Change:
        return Change(drop_index=self)

    def _phases(self) -> List[Phase]:
        return [IdempotentDDLPhase(self.drop_sql, self.create_sql)]


class CheckConstraintMixin(BaseModel):
    table: Optional[str] = None
    domain: Optional[str] = None
    name: str
    expr: str

    @property
    def alter(self):
        objtype = "TABLE" if self.table else "DOMAIN"
        name = self.table or self.domain
        return f"ALTER {objtype} {q(name)}"

    @property
    def add_sql(self) -> str:
        return f"""
        {self.alter} ADD CONSTRAINT {q(self.name)}
        CHECK {self.expr} NOT VALID"""

    @property
    def validate_sql(self) -> str:
        return f"{self.alter} VALIDATE CONSTRAINT {q(self.name)}"

    @property
    def drop_sql(self) -> str:
        return f"{self.alter} DROP CONSTRAINT {q(self.name)}"


class AddCheckConstraint(CheckConstraintMixin, AbstractChange):
    def wrap(self) -> Change:
        return Change(add_check_constraint=self)

    def _phases(self) -> List[Phase]:
        return [
            TransactionalDDLPhase(self.add_sql, self.drop_sql),
            TransactionalDDLPhase(self.validate_sql, None),
        ]


class DropCheckConstraint(CheckConstraintMixin, AbstractChange):
    def wrap(self) -> Change:
        return Change(drop_check_constraint=self)

    def _phases(self) -> List[Phase]:
        return [
            TransactionalDDLPhase(self.drop_sql, self.add_sql),
            TransactionalDDLPhase(None, self.validate_sql),
        ]


