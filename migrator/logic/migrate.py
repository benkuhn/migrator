from . import Context


def upgrade(ctx: Context) -> None:
    db = ctx.db()
    repo = ctx.repo()

    last = db.get_last_finished()
    for tup in repo.next_phases(None if last is None else last.index):
        index, revision, change, phase = tup
        if index == revision.first_index:
            db.create_shim_schema(revision.number)
            db.upsert_revision(revision)
        phase.run(db, index)
        if index == revision.last_index:
            db.drop_shim_schema(revision.number)
