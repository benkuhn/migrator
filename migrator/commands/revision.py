import shlex
import subprocess

from . import Context, text
from .. import models

def revision(ctx: Context, message: str) -> None:
    repo = ctx.repo()
    db = ctx.db()
    if not db.is_set_up():
        ctx.ui.die("Can't do anything with an uninitialized db.")

    num = len(repo.revisions)
    migration_filename = f"{num}-migration.yml"
    schema_filename = f"{num}-schema.sql"
    cmd = shlex.split(repo.config.schema_dump_command)
    with ctx.ui.open(schema_filename, "w") as f:
        subprocess.check_call(cmd, stdout=f, encoding='utf-8')

