import dataclasses

from . import Context
from .. import models


def upgrade(ctx: Context) -> None:
    db = ctx.db()
    repo = ctx.repo()

    last = db.get_latest_audit()
    if last:
        assert last.finished_at is not None
        phases = repo.revisions.get_phases(
            models.PhaseSlice(start=last.index, start_inclusive=last.is_revert)
        )
    else:
        phases = repo.revisions.get_phases(models.PhaseSlice())
    for (index, revision, change, phase) in phases:
        if index == revision.first_index:
            db.create_shim_schema(revision.number)
            db.upsert_revision(revision)
        phase.run(db, index)
        if index == revision.last_index:
            db.drop_shim_schema(revision.number)


def downgrade(ctx: Context, to_revision: int) -> None:
    db = ctx.db()

    revisions = db.get_revisions()
    last_downgrade_to_run = revisions[to_revision + 1].first_index
    slc = models.PhaseSlice(start=last_downgrade_to_run, start_inclusive=True)
    last = db.get_latest_audit()
    if last:
        assert last.finished_at is not None
        slc = dataclasses.replace(slc, end=last.index, end_inclusive=not last.is_revert)
    for (index, revision, change, phase) in reversed(list(revisions.get_phases(slc))):
        if index == revision.last_index:
            db.create_shim_schema(revision.number)
        phase.revert(db, index)
        if index == revision.first_index:
            db.drop_shim_schema(revision.number)
