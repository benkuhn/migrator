from migrator.logic import migrate, init
from tests.fakes import FakeContext


def test_upgrade(ctx: FakeContext) -> None:
    init.init_db(ctx)
    migrate.upgrade(ctx)
    db = ctx.db()
    db.cur.execute("select u_id, email, mobile from users")
    assert len(db.get_revisions()) == 2
