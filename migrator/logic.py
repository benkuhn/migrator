from . import db, models


def migrate_up(d: db.Database, r: models.Repo) -> None:
    """Magic happens here. Run as many migration phases as we can."""
