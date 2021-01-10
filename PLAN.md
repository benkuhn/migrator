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
- [ ] downgrading
- [ ] impl `update_rows`

Other orthogonal stuff

- [ ] init script
- [ ] step idempotency
- [ ] safety checks
- [ ] rebase
