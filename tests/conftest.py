import contextlib
import os
from collections import Iterator

import psycopg2
import pytest

from migrator.commands import Context
from migrator.db import temp_db_url, NAME
from tests.fakes import FakeUserInterface


@pytest.fixture(scope="session")
def control_conn():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.set_session(autocommit=True)
    return conn


RESET_DDL = f"""
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
DROP SCHEMA IF EXISTS {NAME} CASCADE; 
"""

@pytest.fixture
def ctx(control_conn) -> Context:
    old_dir = os.getcwd()
    os.chdir("test")
    try:
        ctx = Context(
            "migrator.yml",
            os.environ["DATABASE_URL"],
            FakeUserInterface()
        )
        with contextlib.closing(ctx):
            yield ctx
    finally:
        with control_conn.cursor() as cur:
            cur.execute(RESET_DDL)
        os.chdir(old_dir)
