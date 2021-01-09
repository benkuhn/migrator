from migrator.commands import Context, text, revision

EXPECTED_MIGRATION = """message: a new revision
pre_deploy:
- run_ddl:
    down: 'GRANT ALL ON SCHEMA public TO chimelife;

      GRANT ALL ON SCHEMA public TO PUBLIC;'
    up: 'REVOKE ALL ON SCHEMA public FROM chimelife;

      REVOKE ALL ON SCHEMA public FROM PUBLIC;'
- run_ddl:
    down: ''
    up: "ALTER TABLE public.users\n    ADD COLUMN name text NOT NULL;"
"""

def test_revision(ctx: Context) -> None:
    revision.revision(ctx, "a new revision")
    with ctx.ui.open("migrations/3-schema.sql", "r") as f:
        dumped_schema = f.read()
    with open("schema.sql", "r") as f2:
        source_schema = f2.read()
    assert dumped_schema == source_schema

    with ctx.ui.open("migrations/3-migration.yml", "r") as f:
        text = f.read()
        assert text == EXPECTED_MIGRATION
    # FIXME: assert something about UI output
