from . import Context


def upgrade(ctx: Context) -> None:
    db = ctx.db()
    repo = ctx.repo()

    last = db.get_last_finished()
    for (index, revision, change, phase) in repo.next_phases(
        None if last is None else last.index, inclusive=last.is_revert
    ):
        if index == revision.first_index:
            db.create_shim_schema(revision.number)
            db.upsert_revision(revision)
        phase.run(db, index)
        if index == revision.last_index:
            db.drop_shim_schema(revision.number)


def downgrade(ctx: Context, to_revision: int) -> None:
    db = ctx.db()

    revisions = db.get_revisions()
    target = revisions[to_revision]
    for (index, revision, change, phase) in reversed(
        list(revisions.next_phases(target.last_index))
    ):
        # TODO: allow reverts from non-final index
        if index == revision.last_index:
            db.create_shim_schema(revision.number)
        phase.revert(db, index)
        if index == revision.first_index:
            db.drop_shim_schema(revision.number)
