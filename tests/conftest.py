import contextlib
import os
import random
import re

import psycopg2
import pytest

from migrator.commands import Context
from tests.fakes import FakeUserInterface


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
    old_dir = os.getcwd()
    os.chdir("test")
    try:
        ctx = Context(
            "migrator.yml",
            db_url,
            FakeUserInterface()
        )
        with contextlib.closing(ctx):
            yield ctx
    finally:
        os.chdir(old_dir)
