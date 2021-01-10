import contextlib
import os
from collections import Iterator

import psycopg2
import pytest

from migrator.commands import Context
from migrator.db import temp_db_url
from migrator.constants import SCHEMA_NAME
from tests.fakes import FakeUserInterface


@pytest.fixture(scope="session")
def control_conn():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.set_session(autocommit=True)
    return conn


RESET_DDL = f"""
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE; 
"""

@pytest.fixture
def test_db_url(control_conn) -> str:
    try:
        yield os.environ["DATABASE_URL"]
    finally:
        with control_conn.cursor() as cur:
            cur.execute(RESET_DDL)


@pytest.fixture
def ctx(test_db_url: str) -> Context:
    old_dir = os.getcwd()
    os.chdir("test")
    try:
        ctx = Context(
            "migrator.yml",
            test_db_url,
            FakeUserInterface()
        )
        with contextlib.closing(ctx):
            yield ctx
    finally:
        os.chdir(old_dir)
