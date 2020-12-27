"""Abstraction layer over a database
"""
from .constants import NAME
from contextlib import contextmanager
import psycopg2

from typing import Any, List, Iterator

SCHEMA_DDL = f"""
CREATE SCHEMA {NAME};

CREATE TABLE {NAME}.migrations (
  revision INT NOT NULL,
  file_hash BYTEA NOT NULL,
  schema_hash BYTEA NOT NULL,
  file TEXT NOT NULL
);

CREATE TABLE {NAME}.migration_audit (
  started_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  revision INT NOT NULL,
  file_hash BYTEA NOT NULL,
  pre_deploy BOOL NOT NULL,
  phase INT NOT NULL,
  subphase INT NOT NULL
);

CREATE TABLE {NAME}.connections (
  revision INT NOT NULL,
  schema_hash BYTEA NOT NULL,
  pid INT NOT NULL,
  backend_start TIMESTAMP WITH TIME ZONE NOT NULL
);
"""

class Database:
    def __init__(self, database_url: str) -> None:
        self.conn = psycopg2.connect(database_url)
        self.cur = self.conn.cursor()

    def _fetch(self, query: str, **kwargs: Any) -> List[Any]:
        self.cur.execute(query, kwargs)
        result = self.cur.fetchall()
        self.conn.commit()
        return result

    @contextmanager
    def tx(self) -> Iterator[None]:
        try:
            yield
            self.conn.commit()
        except:
            self.conn.rollback()
            raise
    
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
