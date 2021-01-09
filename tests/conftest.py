import contextlib
import os
from collections import Iterator

import psycopg2
import pytest

from migrator.commands import Context
from migrator.db import temp_db_url
from tests.fakes import FakeUserInterface


@pytest.fixture(scope="session")
def control_conn():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.set_session(autocommit=True)
    return conn


@pytest.fixture
def db_url(control_conn) -> Iterator[str]:
    with temp_db_url(control_conn) as url:
        yield url


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
