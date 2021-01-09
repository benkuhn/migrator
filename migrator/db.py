"""Abstraction layer over a database
"""
import contextlib
import dataclasses
import os
import random
import re

from .constants import NAME
from contextlib import contextmanager
import hashlib
from typing import Any, List, Iterator, Tuple, Callable, Optional, TypeVar, Iterable, \
    Dict

T = TypeVar("T")

import psycopg2

from . import models

SCHEMA_DDL = f"""
CREATE SCHEMA {NAME};

CREATE TABLE {NAME}.migrations (
  revision INT NOT NULL,
  migration_hash BYTEA NOT NULL,
  schema_hash BYTEA NOT NULL,
  file TEXT NOT NULL
);

CREATE TABLE {NAME}.migration_audit (
  id SERIAL PRIMARY KEY,
  started_at TIMESTAMP WITH TIME ZONE NOT NULL,
  revision INT NOT NULL,
  migration_hash BYTEA NOT NULL,
  schema_hash BYTEA NOT NULL,
  pre_deploy BOOL NOT NULL,
  change INT NOT NULL,
  phase INT NOT NULL,
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

PHASE_INDEX_FIELDS = "revision, migration_hash, schema_hash, pre_deploy, change, phase"

AUDIT_FIELDS = f"id, started_at, finished_at, revert_started_at, revert_finished_at, {PHASE_INDEX_FIELDS}"

def map_audit(row: Iterable[Any]) -> models.MigrationAudit:
    fields = list(row)
    index = models.PhaseIndex(*fields[-6:])
    return models.MigrationAudit(*fields[:-6], index) # type: ignore

class Database:
    def __init__(self, database_url: str) -> None:
        self.conn = psycopg2.connect(database_url)
        self.conn.set_session(autocommit=True)
        self.cur = self.conn.cursor()
        self.in_tx = False

    def _fetch_inner(self, query: str, args: Any) -> List[Any]:
        self.cur.execute(query, args)
        result = self.cur.fetchall()
        return result

    def _fetch(self, query: str, **kwargs: Any) -> List[Any]:
        assert not self.in_tx
        return self._fetch_inner(query, kwargs)

    def _fetch_tx(self, query: str, args: List[Any], **kwargs: Any) -> List[Any]:
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
        SELECT {AUDIT_FIELDS}
        FROM {NAME}.migration_audit
        WHERE finished_at IS NOT NULL
        AND revert_finished_at IS NOT NULL
        ORDER BY id DESC
        LIMIT 1""")
        if len(result) == 0:
            return None
        return map_audit(result[0])

    def audit_phase_start(self, index: models.PhaseIndex) -> models.MigrationAudit:
        result = self._fetch_tx(f"""
        INSERT INTO {NAME}.migration_audit
            (started_at, {PHASE_INDEX_FIELDS})
        VALUES
            (now(), %s, %s, %s, %s, %s, %s)
        RETURNING {AUDIT_FIELDS}""", dataclasses.astuple(index))
        return map_audit(result[0])

    def audit_phase_end(self, audit: models.MigrationAudit) -> models.MigrationAudit:
        result = self._fetch_tx(f"""
        UPDATE {NAME}.migration_audit
            SET finished_at = now()
        WHERE id = %s
        RETURNING {AUDIT_FIELDS}""", (audit.id, ))
        return map_audit(result[0])

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def temp_db_url(self) -> Iterator[str]:
        with temp_db_url(self.conn) as url:
            yield url


@contextlib.contextmanager
def temp_db_url(control_conn: Any) -> Iterator[str]:
    cur = control_conn.cursor()
    db_name = ''.join(random.choices("qwertyuiopasdfghjklzxcvbnm", k=10))
    cur.execute("CREATE DATABASE " + db_name)
    try:
        yield re.sub("/[^/]+$", "/" + db_name, os.environ["DATABASE_URL"])
    finally:
        cur.execute("DROP DATABASE " + db_name)
        cur.close()
