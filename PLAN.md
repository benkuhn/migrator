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
- [ ] downgrades with no matching upgrade?!
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

