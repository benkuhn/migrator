import psycopg2.errors
import pytest

from migrator import models
from migrator.commands import revision
from migrator.constants import SHIM_SCHEMA_FORMAT
from migrator.logic import Context

EXPECTED_MIGRATION = """message: a new revision
post_deploy:
- run_ddl:
    up: ALTER TABLE public.users DROP COLUMN email;
    down: |-
      ALTER TABLE public.users
          ADD COLUMN email text NOT NULL;
"""


def test_revision(ctx: Context) -> None:
    revision.revision(ctx, "a new revision")
    with ctx.ui.open("migrations/3-schema.sql", "r") as f:
        dumped_schema = f.read()
    with open("schema.sql", "r") as f2:
        source_schema = f2.read()
    assert dumped_schema == source_schema

    with ctx.ui.open("migrations/3-migration.yml", "r") as f:
        text = f.read()
        assert text == EXPECTED_MIGRATION
    # FIXME: assert something about UI output


def test_incantation(ctx: Context) -> None:
    incantation = revision.format_incantation(ctx.repo().revisions[1])
    shim_schema = SHIM_SCHEMA_FORMAT % 1
    next_shim_schema = SHIM_SCHEMA_FORMAT % 2
    db = ctx.db()
    db.cur.execute(
        f"""
    CREATE SCHEMA {shim_schema};
    CREATE SCHEMA {next_shim_schema};
    CREATE TABLE {shim_schema}.foo (id INT);
    INSERT INTO {shim_schema}.foo VALUES (1);
    CREATE TABLE {next_shim_schema}.foo (id INT);
    INSERT INTO {next_shim_schema}.foo VALUES (2);
    """
    )
    test_shim = "SELECT id FROM foo;"
    db.create_schema()
    with pytest.raises(psycopg2.errors.UndefinedTable):
        args = ()
        db._fetch(test_shim, args)
    db.cur.execute(incantation)
    # TODO replace with ORM
    args1 = ()
    assert (
        db._fetch("SELECT count(*) FROM migrator_status.connections", args1)[0][0] == 1
    )
    args2 = ()
    assert db._fetch(test_shim, args2)[0][0] == 1
    # Test that running incantation again upserts
    db.cur.execute(incantation)
    # TODO replace with ORM
    args3 = ()
    assert (
        db._fetch("SELECT count(*) FROM migrator_status.connections", args3)[0][0] == 1
    )
