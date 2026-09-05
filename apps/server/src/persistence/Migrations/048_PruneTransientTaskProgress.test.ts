import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("048_PruneTransientTaskProgress", (it) => {
  it.effect("removes transient task progress from projections and the event log", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 47 });

      yield* sql`
        INSERT INTO projection_thread_activities (
          activity_id,
          thread_id,
          turn_id,
          tone,
          kind,
          summary,
          payload_json,
          sequence,
          created_at
        )
        VALUES
          (
            'activity-progress',
            'thread-1',
            'turn-1',
            'info',
            'task.progress',
            'Working',
            '{"taskId":"task-1","description":"Working"}',
            1,
            '2026-07-22T00:00:00.000Z'
          ),
          (
            'activity-completed',
            'thread-1',
            'turn-1',
            'success',
            'task.completed',
            'Finished',
            '{"taskId":"task-1","status":"completed"}',
            2,
            '2026-07-22T00:00:01.000Z'
          )
      `;

      yield* sql`
        INSERT INTO orchestration_events (
          event_id,
          aggregate_kind,
          stream_id,
          stream_version,
          event_type,
          occurred_at,
          command_id,
          causation_event_id,
          correlation_id,
          actor_kind,
          payload_json,
          metadata_json
        )
        VALUES
          (
            'event-progress',
            'thread',
            'thread-1',
            1,
            'thread.activity-appended',
            '2026-07-22T00:00:00.000Z',
            'command-1',
            NULL,
            NULL,
            'provider',
            '{"activity":{"kind":"task.progress"}}',
            '{}'
          ),
          (
            'event-completed',
            'thread',
            'thread-1',
            2,
            'thread.activity-appended',
            '2026-07-22T00:00:01.000Z',
            'command-2',
            NULL,
            NULL,
            'provider',
            '{"activity":{"kind":"task.completed"}}',
            '{}'
          )
      `;

      yield* runMigrations({ toMigrationInclusive: 48 });

      const activities = yield* sql<{ readonly kind: string }>`
        SELECT kind
        FROM projection_thread_activities
        ORDER BY activity_id ASC
      `;
      assert.deepStrictEqual(activities, [{ kind: "task.completed" }]);

      const events = yield* sql<{ readonly eventId: string }>`
        SELECT event_id AS "eventId"
        FROM orchestration_events
        ORDER BY event_id ASC
      `;
      assert.deepStrictEqual(events, [{ eventId: "event-completed" }]);
    }),
  );
});
