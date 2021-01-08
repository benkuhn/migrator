import shlex
import subprocess
import os.path

from . import Context, text
from .. import models

def revision(ctx: Context, message: str) -> None:
    repo = ctx.repo()
    db = ctx.db()

    num = len(repo.revisions) + 1
    dir = repo.config.migrations_dir
    migration_path = os.path.join(dir, f"{num}-migration.yml")
    schema_path = os.path.join(dir, f"{num}-schema.sql")
    cmd = shlex.split(repo.config.schema_dump_command)
    with ctx.ui.open(schema_path, "w") as f:
        subprocess.check_call(cmd, stdout=f, encoding='utf-8')
