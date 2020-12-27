Min viable "get something working"

- [ ] test data + parsing into models
- [ ] `$cmd up` works on empty db
- [ ] `$cmd revision` generates DDL

Riskiest parts:

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
