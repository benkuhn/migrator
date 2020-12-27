$NAME is a language-agnostic tool for managing your Postgres schema, designed to make downtime-free schema changes as easy as possible to develop and run.

## High-level overview

$NAME improves database migration ergonomics in five major ways:

1. It stores previous versions of your schema in source control and auto-generates scaffolding for new migrations by "diffing" them with the current schema. This ensures that your production schema never drifts out of sync from the schema defined by your application.

1. It automates many recipes for doing tricky migrations like column renames "online," without requiring downtime or a synchronized application deploy. Naive methods of renaming a column require multiple application deploys and costly table rebuilds. $NAME's recipe doesn't require either.

1. It eliminates the risk of [inadvertent downtime][gocardless] by breaking migrations up into multiple database transactions, so that no transaction can wait for or hold an important lock for too long. Each transaction is idempotent so that migrations are crash-safe and can be run with a short timeout.

    [gocardless]: https://gocardless.com/blog/zero-downtime-postgres-migrations-the-hard-parts/

1. It stores the full history of migration runs inside the database, so that you can never get your database into a broken state by editing your migration on disk.

1. It tracks which steps of each migration must be run before or after the application code is deployed, and which schema revision is expected by each current Postgres connection, preventing "operator error" from running migrations at the wrong time.

## Configuration

Add a config file:

```toml
# A shell command that prints your current database schema to standard out.
schema_dump_command = "python3 -m models.db.emit_sql"
# The directory to put the migrations in.
migrations_dir = "migrations"
# Whether to (attempt to) crash the application on startup if migration records
# show that we're running against an incompatible schema.
crash_on_incompatible_version = true
```

Create the directory and files:

```bash
$ $cmd init
```

Finally, in order to make table and column renames work, edit your application to run the SQL code in `$migrations_dir/init.sql` before issuing any queries. The init script does a couple things:

1. It errors out if the database it's talking to hasn't had the right migrations run against it. This is intended as a safety check to prevent you from deploying a new version of your code before the associated migrations have run.

1. It marks the current database connection as being associated with a particular schema version. This provides another safety check that prevents you from running any migrations that would break current database clients.

1. Finally, it causes your application to look for tables first in a migration-specific schema, before searching in the `public` (default) schema.

**Note:** $NAME does not support column renames for tables not in the `public` schema.

## Usage

Generate a new revision:

```bash
$ $cmd revision -m 'description of your revision'
Created migrations/7-description-of-your-revision.yml
```

(You probably want to edit the generated migration; see [Migration format](#migration-format) below.)

Update the latest revision in-place (useful if you're iterating on a branch):

```bash
$ $cmd revision --amend
Updated migrations/7-description-of-your-revision.yml
```

Test your last 10 revisions (we recommend adding this to your CI pipeline):

```bash
$ $cmd test --last 10
Testing...
  Instantiating initial schema
  Running #1. Create users table
  ERROR: schemas differ!
@@ -1,8 +1,9 @@
 CREATE TABLE users (
   u_id INT PRIMARY KEY,
-  name TEXT
+  name TEXT NOT NULL
 )
```

Show current state:

```bash
$ $cmd status
Recent revisions:
CURRENT> #7 Add non-null users.name column
         #8 Break addresses into separate table
LATEST > #9 Make users.email unique, oops
```

Run a migration:

```bash
$ $cmd up
Running #7 Add non-null users.name column
  Pre-deploy:
    Phase 1: run_ddl:
    ✓ ALTER TABLE users ADD COLUMN name;
    Done!
  Post-deploy:
    STOPPING: There are still connected clients using schema revision 6:
      PID 1427, 1428, 1429 and 20 more
    Please deploy your new code, then run `$cmd up` again.
$ $cmd up # after deploy
Running #7 Add non-null users.name column
  Pre-deploy: already run
  Post-deploy:
    Phase 1: add_not_null_constraint(users.name):
    ✓ add constraint users.name IS NOT NULL NOT VALID
    ✓ VALIDATE CONSTRAINT
    ✓ SET NOT NULL
```

Note that `$cmd up` also works in development to downgrade and re-run a migration that you've edited.

```bash
$ $cmd up
Database is on the same revision (#7), but the revision was edited after
running. Would you like to revert to #6 and then run the new #7? (Y/n)> Y
OK!
Rolling back #7 Add non-null users.name column
  Post-deploy:
    Phase 1: add_not_null_constraint(users.name):
    ✓ SET NOT NULL
  Pre-deploy:
    Phase 1: run_ddl:
    ✓ ALTER TABLE users DROP COLUMN name;
Running #7 Add non-null users.name column
  Pre-deploy: ...
```

The downgrade uses the *original* source code of the migration (as stored in the database), so you don't need to worry about downgrading before you edit.

## Migration format

Each migration is divided into a pre-deploy and a post-deploy component. Each component is a series of *recipes*. A recipe is a reversible, idempotent series of steps that accomplishes a particular schema change.

Migrations are specified in YAML files that look something like this:

```yaml
revision: 8
message: Break addresses into separate table
pre_deploy:
- run_ddl:
    up: |
      CREATE TABLE addresses (
        addr_id INT PRIMARY KEY,
        text TEXT NOT NULL
      );
      ALTER TABLE users ADD COLUMN addr_id INT;
    down: |
      ALTER TABLE users DROP COLUMN addr_id;
      DROP TABLE addresses;
- add_foreign_key_constraint:
    column: users.addr_id
    references: addresses.addr_id
post_deploy:
- update_rows:
    table: users
    updates: |
      SELECT
        u_id,
        (INSERT INTO addresses VALUES addr_text RETURNING addr_id) as addr_id
      FROM users
      WHERE addr_id IS NULL
- add_check_constraint:
    table: users
    check: addr_id IS NOT NULL
```

## Recipes

The following are example configurations for each type of migration recipe.

```yaml
run_ddl:
  # General-purpose recipe for basic CREATE TABLE etc.
  # Runs the supplied upgrade/downgrade script in a transaction.
  up: CREATE TABLE addresses (...);
  down: DROP TABLE addresses;
add_index:
  # add an index on the given table (using CREATE INDEX CONCURRENTLY)
  table: users
  unique: true
  name: email_unique
  expr: email
add_check_constraint:
  # add a constraint to the given table, in 2 steps:
  # ALTER TABLE ... ADD ... NOT VALID
  # VALIDATE CONSTRAINT
  table: users
  expr: email LIKE %@%
  name: email_contains_atsign
add_foreign_key_constraint:
  # like check_constraint but for foreign keys
  column: users.addr_id
  references: addresses.addr_id
add_not_null:
  # like check_constraint but for NOT NULL
  # https://dba.stackexchange.com/a/52531 suggests a fast way to do this?
  column: table.column
update_rows:
  # does a bulk update of the given table. Commits the update in batches so
  # that no row is locked for a long time.
  table: users
  updates: |
    -- a SELECT statement that returns (primary_key, column_to_update)
    -- will be stuck into a temp table, then rows will be updated in bulk
    SELECT
      u_id,
      (INSERT INTO addresses VALUES addr_text RETURNING addr_id) as addr_id
    FROM users
    WHERE addr_id IS NULL
begin_rename:
  # Does the pre-deploy step of a column rename. Creates an updateable view in the
  # migration-specific schema with the new names.
  table: users
  renames:
    old_name: new_name
finish_rename:
  # Does the post-deploy step of a column rename.
  table: users
```

## Version control

Unlike Git commits, which form a directed acyclic graph, database migrations always form a single sequence. (Why? Because otherwise different developers might apply migrations in a different order. This is mostly fine, but can lead to very confusing results if the migrations touch the same columns!) This allows us to give migrations an auto-incrementing integer for their revision ID.

The only problem with the auto-incrementing integer is: what happens if two developers introduce a migration with the same ID in different branches?

For this use case we provide the `$cmd rebase` command. Run it after merging or rebasing a branch:

```
$ $cmd rebase main
Re-numbering migrations that aren't in the `main` branch...
  #9 -> #12 Make users.email unique, whoops!
 #10 -> #13 Some other cleanups
Resolving conflicts in migrations/init.sql... Done!
```
