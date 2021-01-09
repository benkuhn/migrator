# Glossary

- **Revision:** A particular iteration of the database schema. Each revision is assigned
an incrementing integer.

- **Migration:** A set of instructions for how to move between two revisions. Each
migration has two series of **changes**:

  - a **pre-deploy** series that must be run before the associated code is deployed, and

  - a **post-deploy** series that must be run afterward.

- **Change:** An individual component of a migration, like "run this DDL" or "add this
index." Each change executes in one or more idempotent **phases**. Changes know how to
roll themselves back as well.

- **Phase:** An atomic component of a change; the unit at which we track the status of
migrations. Each phase is either:
 
  - **transactional**, meaning that the phase never leaves the database in a
  partially-changed state (including the migration status bookkeeping); or
  
  - **idempotent**, meaning that the phase can be safely re-run if we're not sure
  whether it has been completed or not (e.g. `CREATE INDEX CONCURRENTLY IF NOT EXISTS`).
