from typing import Optional, Union, Literal, Iterator

import psycopg2
import pytest
import yaml
import pydantic

from migrator import models, diff, db
from tests.fakes import schema_db_url


class DiffFixture(pydantic.BaseModel):
    test: str
    before: str
    after: str
    migration: models.Migration
    reverse: Optional[models.Migration] = None

    def swap(self):
        return DiffFixture(
            test=self.test + " reversed",
            before=self.after,
            after=self.before,
            migration=self.reverse,
            reverse=self.migration
        )

    def test_codegen(self, control_conn) -> None:
        before_url = schema_db_url(control_conn, self.before)
        after_url = schema_db_url(control_conn, self.after)
        pre_deploy, post_deploy = diff.diff(before_url, after_url)
        migration = models.Migration(
            message=self.migration.message,
            pre_deploy=pre_deploy,
            post_deploy=post_deploy
        )
        assert migration == self.migration

    def test_exec(self, db: db.Database) -> None:
        db.create_schema()
        with db.conn.cursor() as cur:
            cur.execute(self.before)
        i_first = models.PhaseIndex(0, b'', b'', True, 0, 0)
        for index, change, phase in self.migration.phases(i_first):
            phase.run(db, index)

    def __str__(self) -> str:
        return self.test

def fixtures() -> Iterator[DiffFixture]:
    with open("fixtures/diff/index.yml") as f:
        fixtures = [DiffFixture(**obj) for obj in yaml.safe_load(f.read())]

    for f in fixtures:
        yield f
        if f.reverse:
            yield f.swap()


@pytest.mark.parametrize("case", list(fixtures()), ids=lambda x: x.test)
def test_codegen(control_conn, case):
    case.test_codegen(control_conn)


@pytest.mark.parametrize("case", list(fixtures()), ids=lambda x: x.test)
def test_exec(test_db_url, case):
    case.test_exec(db.Database(test_db_url))
