from . import Context


def upgrade(ctx: Context) -> None:
    db = ctx.db()
    repo = ctx.repo()

    last = db.get_last_finished()
    for tup in repo.next_phases(None if last is None else last.index):
        index, migration, change, phase = tup
        if index.is_first_for_revision:
            db.upsert(migration)
        phase.run(db, index)
