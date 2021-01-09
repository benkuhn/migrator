from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Union, Tuple
from urllib.parse import urlparse

import pyrseas.database

from . import step_types

Database = Any

class DummyOptions:
    multiple_files = False
    no_owner = True
    no_privs = True
    schemas = []
    onetrans = False
    update = False
    revert = False


def db_url_to_config(db_url: str) -> Dict[str, Any]:
    parsed = urlparse(db_url)
    assert parsed.scheme in {'postgres', 'postgresql'}
    return {
        "database": {
            "host": parsed.hostname,
            "username": parsed.username,
            "password": parsed.password,
            "port": parsed.port or 5432,
            "dbname": parsed.path.lstrip("/")
        },
        "options": DummyOptions
    }


@contextmanager
def load(url: str) -> Iterator[MigratorDatabase]:
    config = db_url_to_config(url)
    db = MigratorDatabase(config)
    try:
        yield db
    finally:
        conn = db.dbconn.conn
        if conn is not None:
            conn.close()


def diff(from_url: str, to_url: str) -> Tuple[
    List[step_types.StepWrapper], List[step_types.StepWrapper]
]:
    with load(from_url) as from_db, load(to_url) as to_db:
        in_map = to_db.to_map()
        pre_deploy, post_deploy = from_db.diff_map_steps(in_map)
        return flatten_steps(pre_deploy), flatten_steps(post_deploy)


@dataclass
class StepHolder:
    obj: str
    deps: List[str]
    step: step_types.AbstractStep


def ddlify(stmts):
    return "\n".join(stmt + ";" for stmt in pyrseas.database.flatten(stmts))


def flatten_steps(steps: List[StepHolder]) -> List[step_types.StepWrapper]:
    ret = []
    for holder in steps:
        step = holder.step
        if isinstance(step, step_types.DDLStep):
            if not (step.up or step.down):
                continue
        ret.append(step.wrap())
    return ret


class MigratorDatabase(pyrseas.database.Database):
    """Subclass of pyrseas Database that knows how to emit migration steps instead of
    SQL statements."""

    def diff_map_steps(self, input_map, quote_reserved=True):
        """Generate SQL to transform an existing database
        :param input_map: a YAML map defining the new database
        :param quote_reserved: fetch reserved words
        :return: list of SQL statements
        Compares the existing database definition, as fetched from the
        catalogs, to the input YAML map and generates SQL statements
        to transform the database into the one represented by the
        input.
        """
        from pyrseas.dbobject.table import Table
        from pyrseas.database import fetch_reserved_words, itemgetter, flatten

        if not self.db:
            self.from_catalog()
        opts = self.config['options']
        if opts.schemas:
            schlist = ['schema ' + sch for sch in opts.schemas]
            for sch in list(input_map.keys()):
                if sch not in schlist and sch.startswith('schema '):
                    del input_map[sch]
            self._trim_objects(opts.schemas)

        # quote_reserved is only set to False by most tests
        if quote_reserved:
            fetch_reserved_words(self.dbconn)

        langs = [lang[0] for lang in self.dbconn.fetchall(
            "SELECT tmplname FROM pg_pltemplate")]
        self.from_map(input_map, langs)
        if opts.revert:
            (self.db, self.ndb) = (self.ndb, self.db)
            del self.ndb.schemas['pg_catalog']
            self.db.languages.dbconn = self.dbconn

        # First sort the objects in the new db in dependency order
        new_objs = []
        for _, d in self.ndb.all_dicts():
            pairs = list(d.items())
            pairs.sort()
            new_objs.extend(list(map(itemgetter(1), pairs)))

        new_objs = self.dep_sorted(new_objs, self.ndb)

        # Then generate the sql for all the objects, walking in dependency
        # order over all the db objects

        pre_deploy_steps = []
        for new in new_objs:
            d = self.db.dbobjdict_from_catalog(new.catalog)
            old = d.get(new.key())
            if old is not None:
                pre_deploy_steps.append(
                    StepHolder(
                        obj=new,
                        deps=new.get_deps(self.ndb),
                        step=step_types.DDLStep(
                            # FIXME: this is wrong for tables
                            up=ddlify(old.alter(new)),
                            down=ddlify(new.alter(old))
                        )
                    )
                )
            else:
                pre_deploy_steps.append(
                    StepHolder(
                        obj=new,
                        deps=new.get_deps(self.ndb),
                        step=step_types.DDLStep(
                            up=ddlify(new.create_sql(self.dbconn.version)),
                            down=ddlify(new.drop())
                        )
                    )
                )

                # Check if the object just created was renamed, in which case
                # don't try to delete the original one
                if getattr(new, 'oldname', None):
                    try:
                        origname, new.name = new.name, new.oldname
                        oldkey = new.key()
                    finally:
                        new.name = origname
                    # Intentionally raising KeyError as tested e.g. in
                    # test_bad_rename_view -- ok Joe?
                    old = d[oldkey]
                    old._nodrop = True

        # Order the old database objects in reverse dependency order
        old_objs = []
        for _, d in self.db.all_dicts():
            pairs = list(d.items())
            pairs.sort
            old_objs.extend(list(map(itemgetter(1), pairs)))
        old_objs = self.dep_sorted(old_objs, self.db)
        old_objs.reverse()

        post_deploy_steps = []
        # Drop the objects that don't appear in the new db
        for old in old_objs:
            d = self.ndb.dbobjdict_from_catalog(old.catalog)
            if isinstance(old, Table):
                new = d.get(old.key())
                if new is not None:
                    post_deploy_steps.append(
                        StepHolder(
                            obj=old,
                            deps=old.get_deps(self.db),
                            step=step_types.DDLStep(
                                # FIXME: this is wrong
                                up=ddlify(old.alter_drop_columns(new)),
                                down=ddlify(new.alter_drop_columns(old))
                            )
                        )
                    )
            if not getattr(old, '_nodrop', False) and old.key() not in d:
                post_deploy_steps.append(
                    StepHolder(
                        obj=old,
                        deps=old.get_deps(self.db),
                        step=step_types.DDLStep(
                            up=ddlify(old.drop()),
                            down=ddlify(old.create_sql(self.dbconn.version)),
                        )
                    )
                )

        if 'datacopy' in self.config:
            opts.data_dir = self.config['files']['data_path']
            # stmts.append(self.ndb.schemas.data_import(opts))
            assert False

        """
        stmts = [s for s in flatten(stmts)]
        funcs = False
        for s in stmts:
            if "LANGUAGE sql" in s and (
                    s.startswith("CREATE FUNCTION ") or
                    s.startswith("CREATE OR REPLACE FUNCTION ")):
                funcs = True
                break
        if funcs:
            stmts.insert(0, "SET check_function_bodies = false")
        """

        return pre_deploy_steps, post_deploy_steps


