from migrator.logic import migrate, init
from tests.fakes import FakeContext


def test_upgrade(ctx: FakeContext) -> None:
    init.init_db(ctx)
    migrate.upgrade(ctx)
    db = ctx.db()
    db.cur.execute("select u_id, email, mobile from users")
    assert len(db.get_revisions()) == 2

    migrate.downgrade(ctx, to_revision=1)
    # This statement will fail on revision 2
    db.cur.execute("insert into users values (1, '2')")
