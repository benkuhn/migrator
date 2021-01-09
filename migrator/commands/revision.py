import shlex
import subprocess
import os.path
import textwrap
from contextlib import contextmanager

import psycopg2

from . import Context, text
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
        subprocess.check_call(cmd, stdout=f, encoding='utf-8')
    with ctx.ui.open(new_schema_path, "r") as f:
        new_schema_sql = f.read()
    with open(old_schema_path, "r") as f:
        old_schema_sql = f.read()

    with temp_db_with_schema(db, old_schema_sql) as old_url, \
        temp_db_with_schema(db, new_schema_sql) as new_url:
        down_ddl = get_migration_ddl(new_url, old_url)
        up_ddl = get_migration_ddl(old_url, new_url)

    with ctx.ui.open(migration_path, "w") as f:
        f.write(MIGRATION_TEMPLATE.format(
            num=num,
            up_ddl=textwrap.indent(up_ddl, DDL_INDENT),
            down_ddl=textwrap.indent(down_ddl, DDL_INDENT),
            message=message
        ))

@contextmanager
def temp_db_with_schema(db: db.Database, schema: str) -> str:
    with db.temp_db_url() as url:
        with psycopg2.connect(url) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(schema)
        conn.close()
        yield url


def get_migration_ddl(from_url, to_url) -> str:
    with diff.load(from_url) as from_db, \
        diff.load(to_url) as to_db:
        in_map = to_db.to_map()
        stmts = from_db.diff_map(in_map)
        # FIXME eliminate REVOKEs in a less hacky way
        return "\n".join(stmt + ';' for stmt in stmts if not stmt.startswith("REVOKE"))
