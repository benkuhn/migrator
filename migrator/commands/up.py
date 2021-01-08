from . import Context, text
from .. import models

def up(ctx: Context) -> None:
    repo = ctx.repo()
    db = ctx.db()
    if not db.is_set_up():
        if not ctx.ui.ask_yes_no(text.ASK_TO_INITIALIZE_DB):
            ctx.ui.die("Can't do anything with an uninitialized db.")
        # TODO factor this into initdb maybe?
        db.create_schema()

    last = db.get_last_finished()
    for part in repo.next_parts(None if last is None else last.part):
        migration, step, subphase = repo.get(part)
        subphase.run(db)
