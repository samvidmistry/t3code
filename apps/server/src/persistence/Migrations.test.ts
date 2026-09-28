import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { migrationManifest, runMigrations } from "./Migrations.ts";

const seedLegacyPiDatabase = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* runMigrations({ toMigrationInclusive: 47 });
  yield* sql`INSERT INTO effect_sql_migrations (migration_id, name, created_at)
    VALUES (48, 'PruneTransientTaskProgress', '2026-09-05 07:10:09')`;
  yield* sql`INSERT INTO projection_threads
    (thread_id, project_id, title, model_selection_json, linked_pull_request_json, created_at, updated_at)
    VALUES ('thread-pi', 'project-1', 'Keep my conversation',
      '{"instanceId":"pi","model":"anthropic/claude-sonnet-5","options":[{"id":"thinking","value":"high"}]}',
      '{"repository":"acme/repo","number":42,"url":"https://github.com/acme/repo/pull/42"}',
      '2026-09-05T00:00:00.000Z', '2026-09-05T00:00:00.000Z')`;
  yield* sql`INSERT INTO projection_thread_messages
    (message_id, thread_id, turn_id, role, text, is_streaming, created_at, updated_at)
    VALUES ('message-1', 'thread-pi', 'turn-1', 'assistant', 'Keep every byte π', 0,
      '2026-09-05T00:00:00.000Z', '2026-09-05T00:00:00.000Z')`;
  yield* sql`INSERT INTO projection_thread_activities
    (activity_id, thread_id, turn_id, tone, kind, summary, payload_json, created_at)
    VALUES ('activity-1', 'thread-pi', 'turn-1', 'info', 'task.progress', 'Still working',
      '{"detail":"Do not prune this"}', '2026-09-05T00:00:00.000Z')`;
  yield* sql`INSERT INTO orchestration_events
    (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
    VALUES ('event-1', 'thread', 'thread-pi', 1, 'thread.activity-appended',
      '2026-09-05T00:00:00.000Z', 'provider', '{"activity":{"kind":"task.progress"}}', '{}')`;
});

it.effect("upgrades the Pi fork without deleting history, model selections, or PR links", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedLegacyPiDatabase;
    const messages = yield* sql`SELECT message_id, thread_id, turn_id, role, text,
      is_streaming, attachments_json, created_at, updated_at FROM projection_thread_messages`;
    const activities = yield* sql`SELECT * FROM projection_thread_activities`;
    const events = yield* sql`SELECT * FROM orchestration_events`;
    const selection = yield* sql`SELECT model_selection_json FROM projection_threads`;
    const executed = yield* runMigrations();
    assert.deepStrictEqual(
      executed.map(([id]) => id),
      [49, 50, 51, 52, 53, 54],
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_events`, events);
    assert.deepStrictEqual(yield* sql`SELECT * FROM projection_thread_activities`, activities);
    assert.deepStrictEqual(
      yield* sql`SELECT model_selection_json FROM projection_threads`,
      selection,
    );
    assert.deepStrictEqual(
      yield* sql`SELECT message_id, thread_id, turn_id, role, text,
      is_streaming, attachments_json, created_at, updated_at FROM projection_thread_messages`,
      messages,
    );
    assert.deepStrictEqual(yield* sql`SELECT context_json FROM projection_thread_messages`, [
      { context_json: null },
    ]);
    assert.deepStrictEqual(yield* sql`SELECT branch_pull_request_json FROM projection_threads`, [
      { branch_pull_request_json: null },
    ]);
    assert.deepStrictEqual(
      yield* sql`SELECT repository, number FROM projection_thread_pull_requests`,
      [{ repository: "acme/repo", number: 42 }],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT name, created_at FROM effect_sql_migrations WHERE migration_id = 48`,
      [{ name: "ProjectionThreadBranchPullRequest", created_at: "2026-09-05 07:10:09" }],
    );
    assert.deepStrictEqual(yield* runMigrations(), []);
    assert.deepStrictEqual(yield* sql`PRAGMA quick_check`, [{ quick_check: "ok" }]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("leaves a normal main database on the standard migration path", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 48 });
    yield* sql`INSERT INTO projection_threads
      (thread_id, project_id, title, model_selection_json, branch_pull_request_json, created_at, updated_at)
      VALUES ('thread-main', 'project-1', 'Main', '{"instanceId":"codex","model":"gpt-5.6-sol"}',
        '{"number":123}', '2026-09-05T00:00:00.000Z', '2026-09-05T00:00:00.000Z')`;
    yield* runMigrations();
    assert.deepStrictEqual(yield* sql`SELECT branch_pull_request_json FROM projection_threads`, [
      { branch_pull_request_json: '{"number":123}' },
    ]);
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
      migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("repairs legacy metadata when the upstream column already exists", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedLegacyPiDatabase;
    yield* sql`ALTER TABLE projection_threads ADD COLUMN branch_pull_request_json TEXT`;
    yield* sql`UPDATE projection_threads SET branch_pull_request_json = '{"number":7}'`;
    yield* runMigrations();
    assert.deepStrictEqual(yield* sql`SELECT branch_pull_request_json FROM projection_threads`, [
      { branch_pull_request_json: '{"number":7}' },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("rolls back the collision repair atomically if recording it fails", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedLegacyPiDatabase;
    yield* sql`CREATE TRIGGER reject_migration_update BEFORE UPDATE ON effect_sql_migrations
      BEGIN SELECT RAISE(ABORT, 'simulated failure'); END`;
    const result = yield* runMigrations().pipe(Effect.result);
    assert.equal(result._tag, "Failure");
    const columns = yield* sql<{ name: string }>`PRAGMA table_info(projection_threads)`;
    assert.equal(
      columns.some(({ name }) => name === "branch_pull_request_json"),
      false,
    );
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = 48`,
      [{ name: "PruneTransientTaskProgress" }],
    );
    yield* sql`DROP TRIGGER reject_migration_update`;
    yield* runMigrations();
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("does not apply the repair when a caller explicitly stops before migration 48", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedLegacyPiDatabase;
    yield* runMigrations({ toMigrationInclusive: 47 });
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = 48`,
      [{ name: "PruneTransientTaskProgress" }],
    );
    const columns = yield* sql<{ name: string }>`PRAGMA table_info(projection_threads)`;
    assert.equal(
      columns.some(({ name }) => name === "branch_pull_request_json"),
      false,
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
