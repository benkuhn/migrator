from . import Context, text
from .. import models


def initdb(ctx: Context) -> None:
    repo = ctx.repo()
    db = ctx.db()
    if not db.is_set_up():
        if not ctx.ui.ask_yes_no(text.ASK_TO_INITIALIZE_DB):
            ctx.ui.die("Can't do anything with an uninitialized db.")
        db.create_schema()
