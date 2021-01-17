import shlex
import subprocess
import os.path
import textwrap
from collections import Iterator
from contextlib import contextmanager

import psycopg2
import yaml

from ..constants import SHIM_SCHEMA_FORMAT, SCHEMA_NAME
from ..logic import Context
from .. import models, diff, db

MIGRATION_TEMPLATE = """
message: {message}
pre_deploy:
- run_ddl:
    up: |
{up_ddl}
    down: |
{down_ddl}
"""


def format_incantation(rev: models.Revision) -> str:
    shim_schema = SHIM_SCHEMA_FORMAT % rev.number
    return f"""
    SELECT set_config(
      'search_path',
      '{shim_schema},'||current_setting('search_path'),
      false -- not transaction-local
    );

    INSERT INTO {SCHEMA_NAME}.connections (pid, revision, schema_hash, backend_start)
    VALUES (
      pg_backend_pid(),
      {rev.number},
      decode('{rev.schema_hash.hex()}', 'hex'),
      (select backend_start from pg_stat_activity where pid = pg_backend_pid())
    )
    ON CONFLICT (pid) DO UPDATE SET
      revision = excluded.revision,
      schema_hash = excluded.schema_hash,
      backend_start = excluded.backend_start;
    """


DDL_INDENT = " " * 6


def revision(ctx: Context, message: str) -> None:
    repo = ctx.repo()
    db = ctx.db()

    num = len(repo.revisions) + 1
    dir = repo.config.migrations_dir
    migration_path = os.path.join(dir, f"{num}-migration.yml")
    new_schema_path = os.path.join(dir, f"{num}-schema.sql")
    old_schema_path = os.path.join(dir, f"{num - 1}-schema.sql")
    cmd = shlex.split(repo.config.schema_dump_command)
    with ctx.ui.open(new_schema_path, "w") as f:
        subprocess.check_call(cmd, stdout=f)
    with ctx.ui.open(new_schema_path, "r") as f:
        new_schema_sql = f.read()
    with open(old_schema_path, "r") as f:
        old_schema_sql = f.read()

    with temp_db_with_schema(db, old_schema_sql) as old_url, temp_db_with_schema(
        db, new_schema_sql
    ) as new_url:
        pre_deploy, post_deploy = diff.diff(old_url, new_url)

    migration = models.Migration(
        message=message, pre_deploy=pre_deploy, post_deploy=post_deploy
    )
    with ctx.ui.open(migration_path, "w") as f:
        f.write(yaml.safe_dump(migration.dict(exclude_defaults=True), sort_keys=False))

    rev = models.FileRevision(number=num, migration_filename=migration_path)
    with ctx.ui.open(repo.config.incantation_path, "w") as f:
        f.write(format_incantation(rev))


@contextmanager
def temp_db_with_schema(db: db.Database, schema: str) -> Iterator[str]:
    with db.temp_db_url() as url:
        with psycopg2.connect(url) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(schema)
        conn.close()
        yield url


def get_migration_ddl(from_url: str, to_url: str) -> str:
    with diff.load(from_url) as from_db, diff.load(to_url) as to_db:
        in_map = to_db.to_map()
        stmts = from_db.diff_map(in_map)
        # FIXME eliminate REVOKEs in a less hacky way
        return "\n".join(stmt + ";" for stmt in stmts if not stmt.startswith("REVOKE"))
