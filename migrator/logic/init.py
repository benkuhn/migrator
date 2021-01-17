from migrator.logic import Context


def init_db(ctx: Context) -> None:
    ctx.db().create_schema()


def init_repo(ctx: Context) -> None:
    pass
