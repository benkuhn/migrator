from migrator.commands import Context, text, revision

EXPECTED_MIGRATION = """
revision: 3
message: a new revision
pre_deploy:
- run_ddl:
    up: |
      ALTER TABLE public.users
          ADD COLUMN name text NOT NULL;
    down: |
      ALTER TABLE public.users DROP COLUMN name;
"""

def test_revision(ctx: Context) -> None:
    revision.revision(ctx, "a new revision")
    with ctx.ui.open("migrations/3-schema.sql", "r") as f:
        dumped_schema = f.read()
    with open("schema.sql", "r") as f2:
        source_schema = f2.read()
    assert dumped_schema == source_schema

    with ctx.ui.open("migrations/3-migration.yml", "r") as f:
        assert f.read() == EXPECTED_MIGRATION
    # FIXME: assert something about UI output
