import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Task progress is a transient live-update signal. Retaining every intermediate
 * snapshot made the event log grow without bound and could exhaust memory while
 * replaying a thread. Keep durable lifecycle boundaries (task.started and
 * task.completed), but discard historical progress rows.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    DELETE FROM projection_thread_activities
    WHERE kind = 'task.progress'
  `;

  yield* sql`
    DELETE FROM orchestration_events
    WHERE event_type = 'thread.activity-appended'
      AND json_extract(payload_json, '$.activity.kind') = 'task.progress'
  `;
});
