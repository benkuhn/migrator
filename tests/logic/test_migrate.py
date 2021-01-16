from migrator.commands import text
from migrator.logic import migrate, init
from tests.fakes import FakeContext


def test_up(ctx: FakeContext) -> None:
    init.init(ctx)
    migrate.upgrade(ctx)
    db = ctx.db()
    db.cur.execute("select u_id, email, mobile from users")
    # TODO replace with call to ORM
    assert (
        db._fetch(
            f"""
    select count(*) from migrator_status.migrations;
    """
        )[0][0]
        == 2
    )
