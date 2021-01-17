"""Abstraction layer over a database"""
from __future__ import annotations

import abc
import contextlib
import dataclasses
import os
import random
import re

from .constants import SCHEMA_NAME, SHIM_SCHEMA_FORMAT
from contextlib import contextmanager
from typing import (
    Any,
    List,
    Iterator,
    Optional,
    TypeVar,
    Sequence,
    Generic,
    Type,
    Callable,
    cast,
    Dict,
)

import psycopg2

from . import models

T = TypeVar("T")
U = TypeVar("U")

SCHEMA_DDL = f"""
CREATE SCHEMA {SCHEMA_NAME};

CREATE TABLE {SCHEMA_NAME}.revisions (
  revision INT NOT NULL,
  migration_hash BYTEA NOT NULL,
  schema_hash BYTEA NOT NULL,
  migration_text TEXT NOT NULL,
  schema_text TEXT NOT NULL,
  is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (revision, migration_hash, schema_hash)
);

CREATE UNIQUE INDEX migration_unique_revision ON {SCHEMA_NAME}.revisions (revision)
    WHERE NOT is_deleted;

CREATE TABLE {SCHEMA_NAME}.migration_audit (
  id SERIAL PRIMARY KEY,
  revision INT NOT NULL,
  migration_hash BYTEA NOT NULL,
  schema_hash BYTEA NOT NULL,
  pre_deploy BOOL NOT NULL,
  change INT NOT NULL,
  phase INT NOT NULL,
  started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  finished_at TIMESTAMP WITH TIME ZONE,
  revert_started_at TIMESTAMP WITH TIME ZONE,
  revert_finished_at TIMESTAMP WITH TIME ZONE,
  CHECK (revert_started_at IS NULL OR finished_at IS NOT NULL),
  CHECK (revert_finished_at IS NULL or revert_started_at IS NOT NULL)
--  FOREIGN KEY (revision, migration_hash, schema_hash) REFERENCES {SCHEMA_NAME}.revisions
--    (revision, migration_hash, schema_hash)
);

CREATE UNIQUE INDEX migration_audit_unique ON {SCHEMA_NAME}.migration_audit ((1))
  WHERE (finished_at IS NULL OR (
    revert_started_at IS NOT NULL AND revert_finished_at IS NULL));

CREATE TABLE {SCHEMA_NAME}.connections (
  pid INT NOT NULL PRIMARY KEY,
  revision INT NOT NULL,
  schema_hash BYTEA NOT NULL,
  backend_start TIMESTAMP WITH TIME ZONE NOT NULL
);
"""


class Mapper(abc.ABC, Generic[T, U]):
    fields: List[str]
    insert_fields: List[str]
    table: str

    @classmethod
    @abc.abstractmethod
    def map(cls, row: Sequence[Any]) -> T:
        pass

    @classmethod
    @abc.abstractmethod
    def obj_to_insertable(cls, obj: U) -> Sequence[Any]:
        pass

    @classmethod
    def columns(cls) -> str:
        return ", ".join(cls.fields)

    @classmethod
    def insert_columns(cls) -> str:
        return ", ".join(cls.insert_fields)

    @classmethod
    def insert_placeholder(cls) -> str:
        return ", ".join(["%s" for _ in cls.insert_fields])


class AuditMapper(Mapper[models.MigrationAudit, models.PhaseIndex]):
    _my_fields = list(f.name for f in dataclasses.fields(models.MigrationAudit))[:-1]
    _index_fields = list(f.name for f in dataclasses.fields(models.PhaseIndex))

    fields = _my_fields + _index_fields
    insert_fields = _index_fields
    table = "migration_audit"

    @classmethod
    def map(cls, row: Sequence[Any]) -> models.MigrationAudit:
        index = models.PhaseIndex(*row[-6:])
        return models.MigrationAudit(*row[:-6], index)  # type: ignore

    @classmethod
    def obj_to_insertable(cls, obj: models.PhaseIndex) -> Sequence[Any]:
        return dataclasses.astuple(obj)


class RevisionMapper(Mapper[models.DbRevision, models.Revision]):
    insert_fields = [
        "revision",
        "migration_hash",
        "schema_hash",
        "migration_text",
        "schema_text",
    ]
    fields = insert_fields + ["is_deleted"]
    table = "revisions"

    @classmethod
    def map(cls, row: Sequence[Any]) -> models.DbRevision:
        rev, mig_h, sch_h, mig_t, sch_t, is_del = row
        result = models.DbRevision(rev, mig_t, sch_t, is_del)
        assert result.migration_hash == bytes(mig_h)
        assert result.schema_hash == bytes(sch_h)
        return result

    @classmethod
    def obj_to_insertable(cls, obj: models.Revision) -> Sequence[Any]:
        return [obj.number] + [getattr(obj, f) for f in cls.insert_fields[1:]]


class ConnectionMapper(Mapper[models.AppConnection, None]):
    fields = ["pid", "revision", "schema_hash", "backend_start"]

    @classmethod
    def map(cls, row: Sequence[Any]) -> models.AppConnection:
        return models.AppConnection(*row)

    @classmethod
    def obj_to_insertable(cls, obj: None) -> Sequence[Any]:
        raise NotImplementedError()


class Results(List[T]):
    def first(self) -> Optional[T]:
        return self[0] if self else None

    def one(self) -> T:
        (obj,) = self
        return obj

    def map(self, row_to_obj: Callable[[T], U]) -> Results[U]:
        return Results(row_to_obj(t) for t in self)


class Database:
    def __init__(self, database_url: str) -> None:
        self.url = database_url
        self.conn = psycopg2.connect(database_url)
        self.conn.set_session(autocommit=True)
        self.cur = self.conn.cursor()
        self.in_tx = False

    def _fetch(self, query: str, args: Any) -> Results[Any]:
        self.cur.execute(query, args)
        result = Results(self.cur.fetchall())
        return result

    @contextmanager
    def tx(self) -> Iterator[None]:
        assert not self.in_tx
        with self.conn:
            try:
                self.in_tx = True
                yield
            finally:
                self.in_tx = False

    def is_set_up(self) -> bool:
        args = {"schema": SCHEMA_NAME}
        result = self._fetch(
            """
        SELECT EXISTS (
          SELECT FROM information_schema.schemata
          WHERE schema_name = %(schema)s
        );
        """,
            args,
        )[0][0]
        return cast(bool, result)

    def create_schema(self) -> None:
        with self.tx():
            self.cur.execute(SCHEMA_DDL)

    def select(
        self, mapper: Type[Mapper[T, Any]], rest: str, args: Any = None
    ) -> Results[T]:
        return self._fetch(
            f"""
        SELECT {mapper.columns()}
        FROM {SCHEMA_NAME}.{mapper.table}
        {rest}
        """,
            args,
        ).map(mapper.map)

    def insert(self, mapper: Type[Mapper[T, U]], obj: U, rest: str = "") -> T:
        # TODO: remove transactional assertion
        args = mapper.obj_to_insertable(obj)
        return (
            self._fetch(
                f"""
        INSERT INTO {SCHEMA_NAME}.{mapper.table}
          ({mapper.insert_columns()})
        VALUES ({mapper.insert_placeholder()})
        {rest}
        RETURNING {mapper.columns()}""",
                args,
            )
            .map(mapper.map)
            .one()
        )

    def update(
        self, mapper: Type[Mapper[T, Any]], set_where: str, args: Sequence[Any]
    ) -> Results[T]:
        # TODO: remove transactional assertion
        return self._fetch(
            f"""
        UPDATE {SCHEMA_NAME}.{mapper.table}
        {set_where}
        RETURNING {mapper.columns()}
        """,
            args,
        ).map(mapper.map)

    def get_last_finished(self) -> Optional[models.MigrationAudit]:
        return self.select(
            AuditMapper,
            """
            WHERE finished_at IS NOT NULL AND revert_finished_at IS NOT NULL
            ORDER BY id DESC LIMIT 1
            """,
        ).first()

    def audit_phase_start(self, index: models.PhaseIndex) -> models.MigrationAudit:
        return self.insert(AuditMapper, index)

    def audit_phase_end(self, audit: models.MigrationAudit) -> models.MigrationAudit:
        return self.update(
            AuditMapper,
            "SET finished_at = now() WHERE id = %s AND finished_at IS NULL",
            (audit.id,),
        ).one()

    def get_audit(self, index: models.PhaseIndex) -> models.MigrationAudit:
        return self.select(
            AuditMapper,
            f"""
        WHERE revision = %(revision)s
        AND migration_hash = %(migration_hash)s
        AND schema_hash = %(schema_hash)s
        AND pre_deploy = %(pre_deploy)s
        AND phase = %(phase)s
        AND change = %(change)s
        ORDER BY id DESC LIMIT 1
        """,
            dataclasses.asdict(index),
        ).one()

    def audit_phase_revert_start(
        self, audit: models.MigrationAudit
    ) -> models.MigrationAudit:
        return self.update(
            AuditMapper,
            "SET revert_started_at = now() WHERE id = %s AND revert_started_at IS NULL",
            (audit.id,),
        ).one()

    def audit_phase_revert_end(
        self, audit: models.MigrationAudit
    ) -> models.MigrationAudit:
        return self.update(
            AuditMapper,
            "SET revert_finished_at = now() WHERE id = %s AND revert_finished_at IS NULL",
            (audit.id,),
        ).one()

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def temp_db_url(self) -> Iterator[str]:
        with temp_db_url(self.conn) as url:
            yield url

    def upsert_revision(self, revision: models.Revision) -> models.DbRevision:
        return self.insert(RevisionMapper, revision, "ON CONFLICT DO NOTHING")

    def get_revisions(self) -> Dict[int, models.Revision]:
        results = self.select(RevisionMapper, "WHERE NOT is_deleted")
        return {rev.number: rev for rev in results}

    def create_shim_schema(self, revision: int) -> None:
        shim_schema = SHIM_SCHEMA_FORMAT % revision
        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {shim_schema}")

    def drop_shim_schema(self, revision: int) -> None:
        shim_schema = SHIM_SCHEMA_FORMAT % revision
        self.cur.execute(f"DROP SCHEMA IF EXISTS {shim_schema}")


@contextlib.contextmanager
def temp_db_url(control_conn: Any) -> Iterator[str]:
    cur = control_conn.cursor()
    db_name = "".join(random.choices("qwertyuiopasdfghjklzxcvbnm", k=10))
    cur.execute("CREATE DATABASE " + db_name)
    try:
        url = os.environ["DATABASE_URL"]
        name = replace_db(url, db_name)
        yield name
    finally:
        cur.execute("DROP DATABASE " + db_name)
        cur.close()


def replace_db(database_url: str, db_name: str) -> str:
    name = re.sub("/[^/]+$", "/" + db_name, database_url)
    return name
