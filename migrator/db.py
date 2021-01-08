"""Abstraction layer over a database
"""
from .constants import NAME
from contextlib import contextmanager
import hashlib
from typing import Any, List, Iterator, Tuple, Callable, Optional, TypeVar, Iterable

T = TypeVar("T")

import psycopg2

from . import models

SCHEMA_DDL = f"""
CREATE SCHEMA {NAME};

CREATE TABLE {NAME}.migrations (
  revision INT NOT NULL,
  file_hash BYTEA NOT NULL,
  schema_hash BYTEA NOT NULL,
  file TEXT NOT NULL
);

CREATE TABLE {NAME}.migration_audit (
  id INT PRIMARY KEY,
  started_at TIMESTAMP WITH TIME ZONE NOT NULL,
  revision INT NOT NULL,
  migration_hash BYTEA NOT NULL,
  schema_hash BYTEA NOT NULL,
  pre_deploy BOOL NOT NULL,
  phase INT NOT NULL,
  subphase INT NOT NULL,
  finished_at TIMESTAMP WITH TIME ZONE,
  revert_started_at TIMESTAMP WITH TIME ZONE,
  revert_finished_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE {NAME}.connections (
  revision INT NOT NULL,
  schema_hash BYTEA NOT NULL,
  pid INT NOT NULL,
  backend_start TIMESTAMP WITH TIME ZONE NOT NULL
);
"""

MIGRATION_PART_FIELDS = "revision, migration_hash, pre_deploy, phase, subphase"

AUDIT_FIELDS = "id, started_at, finished_at, revert_started_at, revert_finished_at, {MIGRATION_PART_FIELDS}"

def map_audit(row: Iterable[Any]) -> models.MigrationAudit:
    fields = list(row)
    part = models.MigrationPart(*fields[-5:])
    return models.MigrationAudit(*fields[:-5], part) # type: ignore

class Database:
    def __init__(self, database_url: str) -> None:
        self.conn = psycopg2.connect(database_url)
        self.cur = self.conn.cursor()
        self.in_tx = False

    def _fetch(self, query: str, **kwargs: Any) -> List[Any]:
        self.cur.execute(query, kwargs)
        result = self.cur.fetchall()
        self.conn.commit()
        return result

    def _exec(self, query: str, *args: Any, mapper: Callable[[Iterable[Any]], T] = None, **kwargs: Any) -> List[T]:
        pass
    
    @contextmanager
    def tx(self) -> Iterator[None]:
        assert not self.in_tx
        try:
            self.in_tx = True
            yield
            self.conn.commit()
        except:
            self.conn.rollback()
            raise
        finally:
            self.in_tx = False
    
    def is_set_up(self) -> bool:
        return self._fetch("""
        SELECT EXISTS (
          SELECT FROM information_schema.schemata
          WHERE schema_name = %(schema)s
        );
        """, schema=NAME)[0][0]

    def create_schema(self) -> None:
        with self.tx():
            self.cur.execute(SCHEMA_DDL)

    def get_last_finished(self) -> Optional[models.MigrationAudit]:
        result = self._fetch(f"""
        SELECT revision, migration_hash, pre_deploy, phase, subphase, id
        FROM {NAME}.migration_audit
        WHERE finished_at IS NOT NULL
        AND revert_finished_at IS NOT NULL
        ORDER BY id DESC
        LIMIT 1""")
        if len(result) == 0:
            return None
        return map_audit(result[0])

    def audit_part_start(self, part: models.MigrationPart) -> models.MigrationAudit:
        result = self.cur.execute(f"""
        INSERT INTO {NAME}.migration_audit
            (started_at, {MIGRATION_PART_FIELDS})
        VALUES
            (now(), %s, %s, %s, %s, %s)
        RETURNING {AUDIT_FIELDS}""", part)
        return map_audit(result[0])

    def close(self) -> None:
        self.conn.close()
