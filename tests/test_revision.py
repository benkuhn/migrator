from migrator.commands import Context, text, revision

def test_revision(ctx: Context) -> None:
    revision.revision(ctx, "a new revision")
    with ctx.ui.open("migrations/3-schema.sql", "r") as f:
        dumped_schema = f.read()
    with open("schema.sql", "r") as f2:
        source_schema = f2.read()
    assert dumped_schema == source_schema
