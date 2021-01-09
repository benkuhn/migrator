from contextlib import contextmanager
from typing import Any, Dict, Iterator
from urllib.parse import urlparse

import pyrseas.database

Database = Any

class DummyOptions:
    multiple_files = False
    no_owner = True
    no_privs = True
    schemas = []
    onetrans = False
    update = False
    revert = False

def db_url_to_config(db_url: str) -> Dict[str, Any]:
    parsed = urlparse(db_url)
    assert parsed.scheme in {'postgres', 'postgresql'}
    return {
        "database": {
            "host": parsed.hostname,
            "username": parsed.username,
            "password": parsed.password,
            "port": parsed.port or 5432,
            "dbname": parsed.path.lstrip("/")
        },
        "options": DummyOptions
    }


@contextmanager
def load(url: str) -> Iterator[Database]:
    config = db_url_to_config(url)
    db = pyrseas.database.Database(config)
    try:
        yield db
    finally:
        conn = db.dbconn.conn
        if conn is not None:
            conn.close()
