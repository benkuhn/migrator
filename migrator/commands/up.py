from ..logic import Context, text
from ..logic.migrate import upgrade
from ..logic.init import init_db


def up(ctx: Context) -> None:
    db = ctx.db()
    if not db.is_set_up():
        if not ctx.ui.ask_yes_no(text.ASK_TO_INITIALIZE_DB):
            ctx.ui.die("Can't do anything with an uninitialized db.")
        # TODO factor this into initdb maybe?
        init_db(ctx)

    # TODO check that disk + db migrations agree
    upgrade(ctx)
