Min viable "get something working"

- [x] test data + parsing into models
- [x] `$cmd up` works on empty db
- [x] `$cmd revision` generates DDL

Riskiest parts:

- [x] pair statement and reverse
- [x] fix ALTER TABLE output
- [x] QOL impvmts
  - [x] YAML formatting
  - [x] Fix names
  - [x] Speed up tests
- [x] Remove nulls in pydantic dicts
- [x] `$cmd revision` codegen works for:
  - [x] indexes
  - [x] constraints
- [x] impl renames
- [x] downgrading

Dev setup

- [x] mypy
- [ ] CI
- [x] black
- [x] DB constraints

Logic

- [x] insert migrations
- [x] init script
- [x] shim schema creation
- [x] downgrades + migration-in-db
- [x] figure out how connections table will work w/ pgbouncer
- [x] downgrades with no matching upgrade--split migration_audit table
- [x] correctly determine where to start if last op was a revert
- [x] downgrades that don't start from latest version
- [x] proof of concept of username approach w/ pgbouncer
- [ ] figure out how deploying multiple migrations at once should work
- [ ] progress reporting
- [ ] step idempotency tests
- [ ] safety checks + transaction timeouts
- [ ] rebase

Interface

- [ ] UI level safety checks
- [ ] init sets up scaffolding
- [ ] status command
- [ ] test command
- [ ] revision --amend
- [ ] up command progress display

Codegen

- [ ] impl `update_rows`
- [ ] emit correct sequence of steps for `ALTER TABLE ADD COLUMN`
- [ ] handle `default`?
- [ ] impl `NOT NULL` constraints
- [ ] impl `UNIQUE` constraints

