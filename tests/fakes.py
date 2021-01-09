import hashlib
import os
import tempfile
from typing import Any, NoReturn, TextIO

import psycopg2

from migrator.commands import UserInterface, text
from migrator import db


class FakeExit(Exception):
    pass


class FakeUserInterface(UserInterface):
    def __init__(self) -> None:
        self.outputs = []
        self.responses = {}
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
        return open(os.path.join(dir, os.path.basename(filename)), mode)

    def close(self) -> None:
        self.tmpdir.cleanup()


def schema_db_url(conn: Any, schema_sql: str) -> str:
    hash = hashlib.md5(schema_sql.encode("ascii")).hexdigest()[:10]
    db_name = f"test_{hash}"
    url = db.replace_db(os.environ["DATABASE_URL"], db_name)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM pg_database WHERE datname = %s", (db_name, ))
        results = cur.fetchall()
        if len(results):
            return url
        cur.execute(f"CREATE DATABASE {db_name}")
        with psycopg2.connect(url) as conn2, conn2.cursor() as cur2:
            try:
                cur2.execute(schema_sql)
                return url
            except:
                conn.rollback()
                cur.execute(f"DROP DATABASE {db_name}")
                raise
