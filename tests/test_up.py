from migrator.commands import Context, text, up

def test_init(ctx: Context) -> None:
    ctx.ui.respond_yes_no(text.ASK_TO_INITIALIZE_DB, "y")
    up.up(ctx)
    ctx.db().cur.execute("select u_id, email, mobile from users")
