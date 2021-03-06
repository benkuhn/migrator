from migrator.commands import up
from migrator.logic import Context, text
from tests.fakes import FakeContext


def test_init(ctx: FakeContext) -> None:
    ctx.ui.respond_yes_no(text.ASK_TO_INITIALIZE_DB, "y")
    up.up(ctx)
    ctx.db().cur.execute("select u_id, email, mobile from users")
