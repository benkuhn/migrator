import contextlib
import random
import re
from typing import Any, NoReturn
import os
import pytest
import psycopg2

from migrator.commands import Context, UserInterface, text, up

class FakeExit(Exception):
    pass

class FakeUserInterface(UserInterface):
    def __init__(self) -> None:
        self.outputs = []
        self.responses = {}

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

@pytest.fixture(scope="session")
def control_conn():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.set_session(autocommit=True)
    return conn

@pytest.fixture
def db_url(control_conn) -> str:
    cur = control_conn.cursor()
    db_name = ''.join(random.choices("qwertyuiopasdfghjklzxcvbnm", k=10))
    cur.execute("CREATE DATABASE " + db_name)
    control_conn.commit()
    yield re.sub("/[^/]+$", "/" + db_name, os.environ["DATABASE_URL"])
    cur.execute("DROP DATABASE " + db_name)
    control_conn.commit()

@pytest.fixture
def ctx(db_url: str) -> Context:
    ctx = Context(
        "test/migrator.yml",
        db_url,
        FakeUserInterface()
    )
    with contextlib.closing(ctx):
        yield ctx

def test_init(ctx: Context) -> None:
    ctx.ui.respond_yes_no(text.ASK_TO_INITIALIZE_DB, "y")
    up.up(ctx)
