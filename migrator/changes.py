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

    @property
    def inner(self) -> AbstractChange:
        result = self.run_ddl or self.create_index or self.drop_index
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
    def run(self, db: db.Database, part: models.MigrationPart) -> None:
        pass


class TransactionalPhase(Phase):
    def run(self, db: db.Database, part: models.MigrationPart) -> None:
        with db.tx():
            audit = db.audit_part_start(part)
            self.run_inner(db)
            db.audit_part_end(audit)

    @abc.abstractmethod
    def run_inner(self, db: db.Database) -> None:
        pass


class IdempotentPhase(Phase):
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
class TransactionalDDLPhase(TransactionalPhase):
    up: str
    down: str

    def run_inner(self, db: db.Database) -> None:
        db.cur.execute(self.up)


@dataclasses.dataclass
class IdempotentDDLPhase(IdempotentPhase):
    up: str
    down: str

    def run_inner(self, db: db.Database) -> None:
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
