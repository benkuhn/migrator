from typing import Optional, Union, Literal, Iterator

import pytest
import yaml
import pydantic

from migrator import models, diff, db, constants
from tests.fakes import schema_db_url


class DiffFixture(pydantic.BaseModel):
    test: str
    before: str
    after: str
    migration: models.Migration
    reverse: Optional[models.Migration] = None
    test_codegen: bool = True
    test_execution: bool = True

    def swap(self):
        return DiffFixture(
            test=self.test + " reversed",
            before=self.after,
            after=self.before,
            migration=self.reverse,
            reverse=self.migration
        )

    def run_codegen(self, control_conn) -> None:
        before_url = schema_db_url(control_conn, self.before)
        after_url = schema_db_url(control_conn, self.after)
        pre_deploy, post_deploy = diff.diff(before_url, after_url)
        migration = models.Migration(
            message=self.migration.message,
            pre_deploy=pre_deploy,
            post_deploy=post_deploy
        )
        assert migration == self.migration

    def run_execution(self, mdb: db.Database) -> None:
        schema_name = constants.SHIM_SCHEMA_FORMAT % 0
        try:
            mdb.create_schema()
            mdb.cur.execute(f"CREATE SCHEMA {schema_name}")
            with mdb.conn.cursor() as cur:
                cur.execute(self.before)
            i_first = models.PhaseIndex(0, b'', b'', True, 0, 0)
            for index, change, phase in self.migration.phases(i_first):
                phase.run(mdb, index)
            mdb.cur.execute(f"DROP SCHEMA {constants.SCHEMA_NAME} CASCADE")
            mdb.cur.execute(f"DROP SCHEMA {schema_name} CASCADE")
            actual_map = diff.to_map(mdb.url)
            expected_map = diff.to_map(schema_db_url(mdb.conn, self.after))
            assert actual_map == expected_map
        finally:
            mdb.cur.execute(f"DROP SCHEMA IF EXISTS {constants.SCHEMA_NAME} CASCADE")
            mdb.cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")

    def __str__(self) -> str:
        return self.test


def fixtures() -> Iterator[DiffFixture]:
    with open("fixtures/diff/index.yml") as f:
        fixtures = [DiffFixture(**obj) for obj in yaml.safe_load(f.read())]

    for f in fixtures:
        yield f
        if f.reverse:
            yield f.swap()


@pytest.mark.parametrize("case", [f for f in fixtures() if f.test_codegen], ids=lambda x: x.test)
def test_codegen(control_conn, case):
    if case.test == "FOREIGN KEY constraint reversed":
        pytest.xfail("Pyrseas bug with fkey columns")
    case.run_codegen(control_conn)


@pytest.mark.parametrize("case", list(fixtures()), ids=lambda x: x.test)
def test_exec(test_db_url, case):
    case.run_execution(db.Database(test_db_url))
