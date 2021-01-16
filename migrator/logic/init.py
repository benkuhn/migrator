from migrator.logic import Context


def init(ctx: Context) -> None:
    ctx.db().create_schema()
