"""Abstraction layer over a database"""
import contextlib
import dataclasses
import os
import random
import re

from .constants import SCHEMA_NAME
from contextlib import contextmanager
from typing import Any, List, Iterator, Optional, TypeVar, Iterable, Sequence

T = TypeVar("T")

import psycopg2

from . import models

SCHEMA_DDL = f"""
CREATE SCHEMA {SCHEMA_NAME};

CREATE TABLE {SCHEMA_NAME}.migrations (
  revision INT NOT NULL,
  migration_hash BYTEA NOT NULL,
  schema_hash BYTEA NOT NULL,
  file TEXT NOT NULL
);

CREATE TABLE {SCHEMA_NAME}.migration_audit (
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

CREATE TABLE {SCHEMA_NAME}.connections (
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
        self.url = database_url
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

    def _fetch_tx(self, query: str, args: Sequence[Any] = (), **kwargs: Any) -> List[Any]:
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
        """, schema=SCHEMA_NAME)[0][0]

    def create_schema(self) -> None:
        with self.tx():
            self.cur.execute(SCHEMA_DDL)

    def get_last_finished(self) -> Optional[models.MigrationAudit]:
        result = self._fetch(f"""
        SELECT {AUDIT_FIELDS}
        FROM {SCHEMA_NAME}.migration_audit
        WHERE finished_at IS NOT NULL
        AND revert_finished_at IS NOT NULL
        ORDER BY id DESC
        LIMIT 1""")
        if len(result) == 0:
            return None
        return map_audit(result[0])

    def audit_phase_start(self, index: models.PhaseIndex) -> models.MigrationAudit:
        result = self._fetch_tx(f"""
        INSERT INTO {SCHEMA_NAME}.migration_audit
            (started_at, {PHASE_INDEX_FIELDS})
        VALUES
            (now(), %s, %s, %s, %s, %s, %s)
        RETURNING {AUDIT_FIELDS}""", dataclasses.astuple(index))
        return map_audit(result[0])

    def audit_phase_end(self, audit: models.MigrationAudit) -> models.MigrationAudit:
        result = self._fetch_tx(f"""
        UPDATE {SCHEMA_NAME}.migration_audit
            SET finished_at = now()
        WHERE id = %s AND finished_at IS NULL
        RETURNING {AUDIT_FIELDS}""", (audit.id, ))
        return map_audit(result[0])

    def get_audit(self, index: models.PhaseIndex) -> models.MigrationAudit:
        result = self._fetch_tx(
            f"""
        SELECT {AUDIT_FIELDS} FROM {SCHEMA_NAME}.migration_audit
        WHERE revision = %(revision)s
        AND migration_hash = %(migration_hash)s
        AND schema_hash = %(schema_hash)s
        AND pre_deploy = %(pre_deploy)s
        AND phase = %(phase)s
        AND change = %(change)s
        ORDER BY id DESC LIMIT 1
        """, **dataclasses.asdict(index))
        return map_audit(result[0])

    def audit_phase_revert_start(self, audit: models.MigrationAudit):
        result = self._fetch_tx(
            f"""
        UPDATE {SCHEMA_NAME}.migration_audit
            SET revert_started_at = now()
        WHERE id = %s AND revert_started_at IS NULL
        RETURNING {AUDIT_FIELDS}""",
            (audit.id, )
        )
        if not result:
            x = 1
            pass
        return map_audit(result[0])

    def audit_phase_revert_end(self, audit: models.MigrationAudit):
        result = self._fetch_tx(
            f"""
        UPDATE {SCHEMA_NAME}.migration_audit
            SET revert_finished_at = now()
        WHERE id = %s AND revert_finished_at IS NULL
        RETURNING {AUDIT_FIELDS}""",
            (audit.id, )
        )
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
        url = os.environ["DATABASE_URL"]
        name = replace_db(url, db_name)
        yield name
    finally:
        cur.execute("DROP DATABASE " + db_name)
        cur.close()


def replace_db(database_url, db_name):
    name = re.sub("/[^/]+$", "/" + db_name, database_url)
    return name
