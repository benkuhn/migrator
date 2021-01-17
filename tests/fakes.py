import hashlib
import os
import tempfile
from typing import Any, NoReturn, TextIO, List, Tuple, Dict, cast

import psycopg2

import migrator.logic

from migrator.logic import Context, UserInterface, text
from migrator import db


class FakeExit(Exception):
    pass


class FakeUserInterface(UserInterface):
    def __init__(self) -> None:
        self.outputs: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
        self.responses: Dict[str, List[str]] = {}
        self.tmpdir = tempfile.TemporaryDirectory()

    def respond_to(self, message: str, response: str) -> None:
        self.responses.setdefault(message, []).append(response)

    def respond_yes_no(self, message: str, response: str) -> None:
        self.respond_to(f"{message} {text.PROMPT_YES_NO}", response)

    def print(self, *args: Any, **kwargs: Any) -> None:
        self.outputs.append((args, kwargs))

    def input(self, prompt: str) -> str:
        return self.responses[prompt].pop()

    def exit(self, status: int) -> NoReturn:
        raise FakeExit(status)

    def open(self, filename: str, mode: str) -> TextIO:
        assert not os.path.isabs(filename)
        dir = os.path.join(self.tmpdir.name, os.path.dirname(filename))
        os.makedirs(dir, exist_ok=True)
        print(f"open {filename}")
        return cast(TextIO, open(os.path.join(dir, os.path.basename(filename)), mode))

    def close(self) -> None:
        self.tmpdir.cleanup()


def schema_db_url(conn: Any, schema_sql: str) -> str:
    hash = hashlib.md5(schema_sql.encode("ascii")).hexdigest()[:10]
    db_name = f"test_{hash}"
    url = db.replace_db(os.environ["DATABASE_URL"], db_name)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM pg_database WHERE datname = %s", (db_name,))
        results = cur.fetchall()
        if len(results):
            regen_all = False
            # uncomment this line to regen all schemas
            # regen_all = True
            if regen_all:
                cur.execute(f"DROP DATABASE {db_name}")
            else:
                return url
        cur.execute(f"CREATE DATABASE {db_name}")
        with psycopg2.connect(url) as conn2, conn2.cursor() as cur2:
            try:
                # Recreate the public schema so that when we diff this database
                # against the main test database, which has had the public schema
                # dropped and recreated, the "description" and privileges of that
                # schema don't show up in the diff.
                cur2.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
                cur2.execute(schema_sql)
                return url
            except:
                conn.rollback()
                cur.execute(f"DROP DATABASE {db_name}")
                raise


class FakeContext(Context):
    # stub to help tests typecheck
    ui: FakeUserInterface
