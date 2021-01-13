from __future__ import annotations

import abc
import dataclasses
from typing import List, Optional, Dict, Tuple, Iterable

import pydantic
from pydantic import BaseModel

from . import models, db, constants


class Change(pydantic.BaseModel):
    run_ddl: Optional[DDLStep] = None
    create_index: Optional[CreateIndex] = None
    drop_index: Optional[DropIndex] = None
    add_constraint: Optional[AddConstraint] = None
    drop_constraint: Optional[DropConstraint] = None
    begin_rename: Optional[BeginRename] = None
    finish_rename: Optional[FinishRename] = None

    @property
    def inner(self) -> AbstractChange:
        for field in self.__fields_set__:
            result = getattr(self, field)
            if result is not None:
                assert isinstance(result, AbstractChange)
                return result
        assert False


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


@dataclasses.dataclass
class Phase(abc.ABC):
    up: PhaseDirection
    down: PhaseDirection

    def run(self, db: db.Database, index: models.PhaseIndex) -> None:
        return self.up.run(db, index)

    def revert(self, db: db.Database, index: models.PhaseIndex) -> None:
        return self.down.revert(db, index)


class PhaseDirection(abc.ABC):
    @abc.abstractmethod
    def run(self, db: db.Database, index: models.PhaseIndex) -> None:
        pass

    @abc.abstractmethod
    def revert(self, db: db.Database, index: models.PhaseIndex) -> None:
        pass


class TransactionalPhase(PhaseDirection):
    def run(self, db: db.Database, index: models.PhaseIndex) -> None:
        with db.tx():
            audit = db.audit_phase_start(index)
            self.run_inner(db, index)
            db.audit_phase_end(audit)

    def revert(self, db: db.Database, index: models.PhaseIndex) -> None:
        with db.tx():
            audit = db.get_audit(index)
            audit = db.audit_phase_revert_start(audit)
            self.run_inner(db, index)
            db.audit_phase_revert_end(audit)

    @abc.abstractmethod
    def run_inner(self, db: db.Database, index: models.PhaseIndex) -> None:
        pass


class IdempotentPhase(PhaseDirection):
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

    def revert(self, db: db.Database, index: models.PhaseIndex) -> None:
        with db.tx():
            # FIXME: what happens if we already started?
            audit = db.get_audit(index)
            audit = db.audit_phase_revert_start(audit)
        self.run_inner(db)
        with db.tx():
            db.audit_phase_revert_end(audit)


@dataclasses.dataclass
class TxDDL(TransactionalPhase):
    ddl: str

    def run_inner(self, db: db.Database, index: models.PhaseIndex) -> None:
        db.cur.execute(self.ddl)


@dataclasses.dataclass
class NoOp(TransactionalPhase):
    def run_inner(self, db: db.Database, index: models.PhaseIndex) -> None:
        pass


@dataclasses.dataclass
class IdempotentDDL(IdempotentPhase):
    ddl: str

    def run_inner(self, db: db.Database) -> None:
        db.cur.execute(self.ddl)


class DDLStep(BaseModel, AbstractChange):
    up: str
    down: str

    def _phases(self) -> List[Phase]:
        return [Phase(TxDDL(self.up), TxDDL(self.down))]

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
        return [Phase(IdempotentDDL(self.create_sql), IdempotentDDL(self.drop_sql))]


class DropIndex(IndexMixin, AbstractChange):
    def wrap(self) -> Change:
        return Change(drop_index=self)

    def _phases(self) -> List[Phase]:
        return [Phase(IdempotentDDL(self.drop_sql), IdempotentDDL(self.create_sql))]


class ConstraintMixin(BaseModel):
    table: Optional[str] = None
    domain: Optional[str] = None
    name: str
    check: Optional[str] = None
    foreign_key: Optional[str] = None
    references: Optional[str] = None

    """
    @pydantic.root_validator
    def validate(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # TODO: Work around pydantic bug: we get re-validated when passed to a
        # containing class (e.g. in .wrap())
        if not isinstance(values, dict):
            return values
        if values.get("check"):
            assert values.get("foreign_key") is None
            assert values.get("references") is None
        else:
            assert values.get("foreign_key") is not None
            assert values.get("references") is not None
        return values
    """

    @property
    def alter(self) -> str:
        objtype = "TABLE" if self.table else "DOMAIN"
        name = self.table or self.domain
        assert name
        return f"ALTER {objtype} {q(name)}"

    @property
    def add_sql(self) -> str:
        assert self.check or self.foreign_key
        return f"""
        {self.alter} ADD CONSTRAINT {q(self.name)}
        {self.descr} NOT VALID"""

    @property
    def descr(self) -> str:
        if self.check:
            return f"CHECK {self.check}"
        assert self.references
        return f"FOREIGN KEY ({self.foreign_key}) REFERENCES {self.references}"

    @property
    def validate_sql(self) -> str:
        return f"{self.alter} VALIDATE CONSTRAINT {q(self.name)}"

    @property
    def drop_sql(self) -> str:
        return f"{self.alter} DROP CONSTRAINT {q(self.name)}"


class AddConstraint(ConstraintMixin, AbstractChange):
    def wrap(self) -> Change:
        return Change(add_constraint=self)

    def _phases(self) -> List[Phase]:
        return [
            Phase(TxDDL(self.add_sql), TxDDL(self.drop_sql)),
            Phase(TxDDL(self.validate_sql), NoOp()),
        ]


class DropConstraint(ConstraintMixin, AbstractChange):
    def wrap(self) -> Change:
        return Change(drop_constraint=self)

    def _phases(self) -> List[Phase]:
        return [
            Phase(NoOp(), TxDDL(self.validate_sql)),
            Phase(TxDDL(self.drop_sql), TxDDL(self.add_sql)),
        ]


class RenameMixin(BaseModel):
    table: str
    renames: Dict[str, str]

    def rename_sql(self, map: Iterable[Tuple[str, str]]) -> str:
        return "; ".join(
            [f"ALTER TABLE {self.table} RENAME COLUMN {old} TO {new}"
             for old, new in map]
        )

    @property
    def up_rename_sql(self) -> str:
        return self.rename_sql(self.renames.items())

    @property
    def down_rename_sql(self) -> str:
        return self.rename_sql((new, old) for old, new in self.renames.items())


class BeginRename(RenameMixin, AbstractChange):
    def wrap(self) -> Change:
        return Change(begin_rename=self)

    def _phases(self) -> List[Phase]:
        return [
            Phase(CreateRenameViewPhase(**self.dict()), RenameDropViewPhase(**self.dict()))
        ]


class FinishRename(RenameMixin, AbstractChange):
    def wrap(self) -> Change:
        return Change(finish_rename=self)

    def _phases(self) -> List[Phase]:
        return [
            Phase(TxDDL(self.up_rename_sql), TxDDL(self.down_rename_sql)),
            Phase(
                RenameDropViewPhase(**self.dict()),
                CreateRenameViewPhase(
                    table=self.table,
                    renames={v: k for k, v in self.renames.items()}
                )
            )
        ]


class CreateRenameViewPhase(RenameMixin, TransactionalPhase):

    def run_inner(self, db: db.Database, index: models.PhaseIndex) -> None:
        colnames = db._fetch_tx("""
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name   = %s
        """, [self.table])
        aliases = []
        for (colname, ) in colnames:
            newname = self.renames.pop(colname, None)
            if newname is None:
                aliases.append(colname)
            else:
                aliases.append(f"{colname} as {newname}")
        if len(self.renames) != 0:
            raise AssertionError(
                "Columns not present: " + ",".join(self.renames.keys())
            )
        schema = constants.SHIM_SCHEMA_FORMAT % index.revision
        db.cur.execute(f"""
        CREATE VIEW {schema}.{self.table} AS SELECT
          {", ".join(aliases)}
        FROM public.{self.table}
        """)

class RenameDropViewPhase(RenameMixin, TransactionalPhase):

    def run_inner(self, db: db.Database, index: models.PhaseIndex) -> None:
        schema = constants.SHIM_SCHEMA_FORMAT % index.revision
        db.cur.execute(f"DROP VIEW {schema}.{self.table}")
