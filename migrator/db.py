"""Abstraction layer over a database"""
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
    Iterable,
    Sequence,
    Dict,
    Generic,
    Tuple,
)

T = TypeVar("T")

import psycopg2

from . import models

SCHEMA_DDL = f"""
CREATE SCHEMA {SCHEMA_NAME};

CREATE TABLE {SCHEMA_NAME}.migrations (
  revision INT NOT NULL,
  migration_hash BYTEA NOT NULL,
  schema_hash BYTEA NOT NULL,
  file TEXT NOT NULL,
  is_deleted BOOLEAN NOT NULL,
  PRIMARY KEY (revision, migration_hash, schema_hash)
);

CREATE UNIQUE INDEX migration_unique_revision ON {SCHEMA_NAME}.migrations (revision)
    WHERE NOT is_deleted;

CREATE TABLE {SCHEMA_NAME}.migration_audit (
  id SERIAL PRIMARY KEY,
  revision INT NOT NULL,
  migration_hash BYTEA NOT NULL,
  schema_hash BYTEA NOT NULL,
  pre_deploy BOOL NOT NULL,
  change INT NOT NULL,
  phase INT NOT NULL,
  started_at TIMESTAMP WITH TIME ZONE NOT NULL,
  finished_at TIMESTAMP WITH TIME ZONE,
  revert_started_at TIMESTAMP WITH TIME ZONE,
  revert_finished_at TIMESTAMP WITH TIME ZONE,
  CHECK (revert_started_at IS NULL OR finished_at IS NOT NULL),
  CHECK (revert_finished_at IS NULL or revert_started_at IS NOT NULL)
--  FOREIGN KEY (revision, migration_hash, schema_hash) REFERENCES {SCHEMA_NAME}.migrations
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

PHASE_INDEX_FIELDS = "revision, migration_hash, schema_hash, pre_deploy, change, phase"


class Mapper(abc.ABC, Generic[T]):
    pk_fields: List[str]
    fields: List[str]

    @classmethod
    @abc.abstractmethod
    def row_to_obj(cls, row: Sequence[Any]) -> T:
        pass

    @classmethod
    @abc.abstractmethod
    def obj_to_row(cls, obj: T) -> List[Any]:
        pass

    @classmethod
    def columns(cls) -> str:
        return ", ".join(cls.pk_fields + cls.fields)


class AuditMapper(Mapper[models.MigrationAudit]):
    pk_fields = ["id"]
    my_fields = list(f.name for f in dataclasses.fields(models.MigrationAudit))[1:-1]
    index_fields = list(f.name for f in dataclasses.fields(models.PhaseIndex))
    fields = my_fields + index_fields

    @classmethod
    def row_to_obj(cls, row: Sequence[Any]) -> models.MigrationAudit:
        index = models.PhaseIndex(*row[-6:])
        return models.MigrationAudit(*row[:-6], index)  # type: ignore

    @classmethod
    def obj_to_row(cls, obj: models.MigrationAudit) -> List[Any]:
        return [*obj[:-1], *obj.index]


class Database:
    def __init__(self, database_url: str) -> None:
        self.url = database_url
        self.conn = psycopg2.connect(database_url)
        self.conn.set_session(autocommit=True)
        self.cur = self.conn.cursor()
        self.in_tx = False

    def _fetch_inner(self, query: str, args: Any) -> List[Any]:
        self.cur.execute(query, args)
        result = self.cur.fetchall()
        return result

    def _fetch(self, query: str, args: Sequence[Any] = (), **kwargs: Any) -> List[Any]:
        assert not self.in_tx
        return self._fetch_inner(query, args or kwargs)

    def _fetch_tx(
        self, query: str, args: Sequence[Any] = (), **kwargs: Any
    ) -> List[Any]:
        assert self.in_tx
        return self._fetch_inner(query, args or kwargs)

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
        return self._fetch(
            """
        SELECT EXISTS (
          SELECT FROM information_schema.schemata
          WHERE schema_name = %(schema)s
        );
        """,
            schema=SCHEMA_NAME,
        )[0][0]

    def create_schema(self) -> None:
        with self.tx():
            self.cur.execute(SCHEMA_DDL)

    def get_last_finished(self) -> Optional[models.MigrationAudit]:
        result = self._fetch(
            f"""
        SELECT {AuditMapper.columns()}
        FROM {SCHEMA_NAME}.migration_audit
        WHERE finished_at IS NOT NULL
        AND revert_finished_at IS NOT NULL
        ORDER BY id DESC
        LIMIT 1"""
        )
        if len(result) == 0:
            return None
        return AuditMapper.row_to_obj(result[0])

    def audit_phase_start(self, index: models.PhaseIndex) -> models.MigrationAudit:
        result = self._fetch_tx(
            f"""
        INSERT INTO {SCHEMA_NAME}.migration_audit
            (started_at, {PHASE_INDEX_FIELDS})
        VALUES
            (now(), %s, %s, %s, %s, %s, %s)
        RETURNING {AuditMapper.columns()}""",
            dataclasses.astuple(index),
        )
        return AuditMapper.row_to_obj(result[0])

    def audit_phase_end(self, audit: models.MigrationAudit) -> models.MigrationAudit:
        result = self._fetch_tx(
            f"""
        UPDATE {SCHEMA_NAME}.migration_audit
            SET finished_at = now()
        WHERE id = %s AND finished_at IS NULL
        RETURNING {AuditMapper.columns()}""",
            (audit.id,),
        )
        return AuditMapper.row_to_obj(result[0])

    def get_audit(self, index: models.PhaseIndex) -> models.MigrationAudit:
        result = self._fetch_tx(
            f"""
        SELECT {AuditMapper.columns()} FROM {SCHEMA_NAME}.migration_audit
        WHERE revision = %(revision)s
        AND migration_hash = %(migration_hash)s
        AND schema_hash = %(schema_hash)s
        AND pre_deploy = %(pre_deploy)s
        AND phase = %(phase)s
        AND change = %(change)s
        ORDER BY id DESC LIMIT 1
        """,
            **dataclasses.asdict(index),
        )
        return AuditMapper.row_to_obj(result[0])

    def audit_phase_revert_start(
        self, audit: models.MigrationAudit
    ) -> models.MigrationAudit:
        result = self._fetch_tx(
            f"""
        UPDATE {SCHEMA_NAME}.migration_audit
            SET revert_started_at = now()
        WHERE id = %s AND revert_started_at IS NULL
        RETURNING {AuditMapper.columns()}""",
            (audit.id,),
        )
        if not result:
            x = 1
            pass
        return AuditMapper.row_to_obj(result[0])

    def audit_phase_revert_end(
        self, audit: models.MigrationAudit
    ) -> models.MigrationAudit:
        result = self._fetch_tx(
            f"""
        UPDATE {SCHEMA_NAME}.migration_audit
            SET revert_finished_at = now()
        WHERE id = %s AND revert_finished_at IS NULL
        RETURNING {AuditMapper.columns()}""",
            (audit.id,),
        )
        return AuditMapper.row_to_obj(result[0])

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def temp_db_url(self) -> Iterator[str]:
        with temp_db_url(self.conn) as url:
            yield url

    def upsert(self, revision: models.Revision) -> None:
        MIGRATION_FIELDS = "revision, migration_hash, schema_hash, file, is_deleted"
        result = self._fetch(
            f"""
        INSERT INTO {SCHEMA_NAME}.migrations
            (revision, migration_hash, schema_hash, file, is_deleted)
        VALUES (%s, %s, %s, %s, TRUE)
        ON CONFLICT DO NOTHING
        RETURNING {MIGRATION_FIELDS}
        """,
            (
                revision.number,
                revision.migration_hash,
                revision.schema_hash,
                revision.migration_text,
            ),
        )
        # Sanity check that the migration is the same on upsert
        assert result[0][3] == revision.migration_text

    def create_shim_schema(self, revision: int) -> None:
        shim_schema = SHIM_SCHEMA_FORMAT % revision
        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {shim_schema}")

    def drop_shim_schema(self, revision: int) -> None:
        shim_schema = SHIM_SCHEMA_FORMAT % revision
        self.cur.execute(f"DROP SCHEMA IF EXISTS {shim_schema}")


@contextlib.contextmanager
def temp_db_url(control_conn: Any) -> Iterator[str]:  # type: ignore
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
