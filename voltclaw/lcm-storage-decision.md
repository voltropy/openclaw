# LCM Storage Backend Decision

## Status

**Decided** — Embedded PostgreSQL

## Context

The Long Context Management (LCM) subsystem requires a persistent storage backend for conversations, messages, summaries, and context-window state. The VoltCode reference implementation (`packages/voltcode/src/session/lcm/db.ts`) uses embedded PostgreSQL v17.7 on port 54329 with features including:

- `tsvector` full-text search with GIN indexes
- Advisory locks for compaction coordination
- `ENUM` types for role, summary kind, context item type, and message part type
- `JSONB` columns for tool input and metadata
- `GENERATED ALWAYS AS … STORED` columns for automatic tsvector maintenance
- Multi-tenant isolation via per-user schema (`search_path`)

## Evaluation Criteria

| Criterion              | Weight | Description                                                                                   |
| ---------------------- | ------ | --------------------------------------------------------------------------------------------- |
| Transactions & locking | High   | Compaction requires row-level locks and advisory locks to prevent concurrent compaction races |
| Full-text search       | High   | Semantic recall uses `ts_rank` over `tsvector` columns for relevance scoring                  |
| Concurrent writes      | Medium | Multiple sessions may write messages while compaction runs in the background                  |
| Operational complexity | Medium | Developer experience for local development and single-user deployment                         |
| Schema fidelity        | High   | Ability to reuse the reference schema without modification                                    |

## Option A: SQLite

**Pros:**

- Zero-config, single file, no daemon process
- Small binary footprint
- Well-supported in Node.js via `better-sqlite3`

**Cons:**

- No native `tsvector` or GIN indexes — would require FTS5 virtual tables with different query syntax, breaking schema compatibility
- No advisory locks — compaction coordination would need a custom locking table or file-based locks
- WAL mode still serializes writes; concurrent compaction + message inserts can cause `SQLITE_BUSY` under load
- No `ENUM` types — would require CHECK constraints and lose type safety
- No `GENERATED ALWAYS AS … STORED` for tsvector — would require triggers or application-level index maintenance
- Schema divergence from the reference implementation increases maintenance burden

## Option B: Embedded PostgreSQL

**Pros:**

- Full feature parity with the reference implementation — schema can be used as-is
- `tsvector`, advisory locks, ENUM types, JSONB, GENERATED columns all available
- Proven in production via VoltCode's embedded PostgreSQL deployment
- `embedded-postgres` npm package handles binary management and lifecycle
- Supports external PostgreSQL via connection string for production/team deployments

**Cons:**

- Heavier binary (~70MB compressed PostgreSQL distribution)
- Requires port management (default 54329, must avoid collisions)
- First startup incurs ~2-3 second initialization penalty

## Decision

**Embedded PostgreSQL** for OpenClaw LCM v1.

Rationale:

1. Schema compatibility with VoltCode eliminates translation risk and enables shared tooling
2. Advisory locks are the only clean solution for compaction coordination without race conditions
3. `tsvector` with GIN indexes provides battle-tested full-text search without reimplementing ranking logic
4. The `embedded-postgres` npm package reduces operational complexity to near-SQLite levels for local development

### Connection Strategy

- **Default (local):** Embedded PostgreSQL on port 54329, database `lcm`
- **Override:** Set `LCM_DATABASE_URL` to connect to any external PostgreSQL instance (e.g., RDS, Supabase, Neon)
- Connection pooling: max 10 connections, 20s idle timeout, 30min max lifetime
- `prepare: false` for RDS Proxy compatibility

## Migration Plan

1. Add `embedded-postgres` and `postgres` (connection library) as dependencies
2. On LCM initialization, start embedded PostgreSQL if no `LCM_DATABASE_URL` is set
3. Run idempotent migrations (CREATE TYPE IF NOT EXISTS, CREATE TABLE IF NOT EXISTS)
4. Schema lives in `src/plugins/lcm/db/schema.sql` for auditability
5. Migration runner in `src/plugins/lcm/db/migration.ts` handles enum creation and schema application
