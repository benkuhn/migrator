Min viable "get something working"

- [x] test data + parsing into models
- [x] `$cmd up` works on empty db
- [x] `$cmd revision` generates DDL

Riskiest parts:

- [x] pair statement and reverse
- [x] fix ALTER TABLE output
- [ ] `$cmd revision` codegen works for:
  - [ ] indexes
  - [ ] constraints
- [ ] impl renames
- [ ] impl `update_rows`

Other orthogonal stuff

- [ ] init script
- [ ] step idempotency
- [ ] safety checks
- [ ] rebase
