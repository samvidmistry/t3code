// @effect-diagnostics-next-line nodeBuiltinImport:off -- The directory key must match the plugin's synchronous SHA-256 implementation.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The plugin keys run files with native resolved project paths.
import * as NodePath from "node:path";
import type { OrchestrationV2Subagent, OrchestrationV2SubagentWorkflow } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import type * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import { piRecordField as field, piRecordString as text } from "./rpc.ts";

export interface PiWorkflowSnapshot {
  readonly name: string | undefined;
  readonly runId: string | undefined;
  readonly status: "running" | "completed" | "failed" | "interrupted" | "paused" | undefined;
  readonly currentPhase: string | undefined;
  readonly workflow: OrchestrationV2SubagentWorkflow;
}

function preview(value: unknown, max = 200) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim().slice(0, max)
    : undefined;
}

function count(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? Math.round(value)
    : undefined;
}

export function normalizePiWorkflowSnapshot(value: unknown): PiWorkflowSnapshot | undefined {
  const rawAgents = field(value, "agents");
  const rawPhases = field(value, "phases");
  if (!Array.isArray(rawAgents) || !Array.isArray(rawPhases)) return;
  const phases = rawPhases.flatMap((phase) => preview(phase) ?? []).slice(0, 50);
  const agents: OrchestrationV2SubagentWorkflow["agents"][number][] = [];
  for (const raw of rawAgents) {
    const index = count(field(raw, "id"));
    const reportedState = text(raw, "status");
    // The wire has no skipped state: a skipped member is settled, not a failed execution.
    const state =
      reportedState === "queued"
        ? "queued"
        : reportedState === "running"
          ? "running"
          : reportedState === "done" || reportedState === "skipped"
            ? "completed"
            : reportedState === "error"
              ? "failed"
              : undefined;
    if (index === undefined || state === undefined) continue;
    const phase = preview(field(raw, "phase"));
    const phaseIndex = phase === undefined ? -1 : phases.indexOf(phase);
    const model = preview(field(raw, "model"));
    const error = preview(field(raw, "error"), 2_000);
    const result = error ?? preview(field(raw, "result"), 10_000);
    const tokens = count(field(field(raw, "tokenUsage"), "total")) ?? count(field(raw, "tokens"));
    agents.push({
      index,
      label: preview(field(raw, "label")) ?? `agent ${index}`,
      state,
      ...(phaseIndex < 0 ? {} : { phaseIndex, phaseTitle: phase }),
      ...(model === undefined ? {} : { model }),
      ...(tokens === undefined ? {} : { totalTokens: tokens }),
      ...(result === undefined ? {} : { result }),
    });
    if (agents.length >= 100) break;
  }
  const name = preview(field(value, "workflowName")) ?? preview(field(value, "name"));
  const runId = preview(field(value, "runId"));
  const tokens = count(field(field(value, "tokenUsage"), "total"));
  const rawStatus = text(value, "status");
  const status =
    rawStatus === "pending" || rawStatus === "running"
      ? "running"
      : rawStatus === "completed" || rawStatus === "failed" || rawStatus === "paused"
        ? rawStatus
        : rawStatus === "aborted"
          ? "interrupted"
          : undefined;
  return {
    name,
    runId,
    status,
    currentPhase: preview(field(value, "currentPhase")),
    workflow: {
      ...(name === undefined ? {} : { name }),
      ...(runId === undefined ? {} : { runHandles: { runId } }),
      phases: phases.map((title, index) => ({ index, title })),
      agents,
      ...(tokens === undefined ? {} : { totalTokens: tokens }),
    },
  };
}

export function piWorkflowProgressSummary(snapshot: PiWorkflowSnapshot) {
  const agents = snapshot.workflow.agents;
  const done = agents.filter(
    (agent) => agent.state === "completed" || agent.state === "failed",
  ).length;
  const running = agents.filter((agent) => agent.state === "running").length;
  const failed = agents.filter((agent) => agent.state === "failed").length;
  const summary =
    agents.length === 0
      ? snapshot.status === "running"
        ? "agents working; each appears when it finishes"
        : "starting"
      : `${done}/${agents.length} agents done${running > 0 ? `, ${running} running` : ""}${failed > 0 ? `, ${failed} failed` : ""}`;
  const phase = snapshot.currentPhase ?? snapshot.workflow.phases.at(-1)?.title;
  return phase === undefined ? summary : `${phase} · ${summary}`;
}

export function piWorkflowBackgroundRunId(toolName: string, result: unknown) {
  const details = field(result, "details");
  if (toolName === "workflow" && field(details, "background") === true)
    return preview(field(details, "runId"));
  if (
    toolName === "workflow_control" &&
    text(details, "action") === "resume" &&
    text(details, "result") === "resumed"
  ) {
    return preview(field(field(details, "run"), "runId"));
  }
  return undefined;
}

export function piWorkflowCallName(args: unknown) {
  return (
    preview(field(args, "name")) ??
    preview(/\bname\s*:\s*["'`]([^"'`\n]{1,200})["'`]/u.exec(text(args, "script") ?? "")?.[1])
  );
}

/** `home` is the home directory the Pi process sees; the plugin keys run files under it. */
export function piWorkflowRunsDirectory(cwd: string, home: string) {
  const projectPath = NodePath.resolve(cwd);
  const slug =
    NodePath.basename(projectPath)
      .toLowerCase()
      .replace(/[^a-z0-9._-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 48) || "project";
  const hash = NodeCrypto.createHash("sha256").update(projectPath).digest("hex").slice(0, 12);
  return NodePath.join(home, ".pi", "workflows", "projects", `${slug}-${hash}`, "runs");
}

export function isPiWorkflowRunId(runId: string) {
  return /^[A-Za-z0-9._-]{1,200}$/u.test(runId) && !runId.startsWith(".");
}

const RUN_LOG_FORMAT = "pi-workflow-run-v2";

const isPlainRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Commit boundary of a v2 run head: the log prefix that is safe to replay. */
export interface PiWorkflowRunLogHead {
  readonly bytes: number;
  readonly sequence: number;
  readonly generation: string;
  readonly runId: string | undefined;
}

/** Identifies a v2 head file's committed log prefix, or undefined for a legacy whole-state file. */
export function piWorkflowRunLogHead(record: unknown): PiWorkflowRunLogHead | undefined {
  if (field(record, "format") !== RUN_LOG_FORMAT) return undefined;
  const bytes = field(record, "bytes");
  const sequence = field(record, "sequence");
  const generation = text(record, "generation");
  if (
    !Number.isSafeInteger(bytes) ||
    !Number.isSafeInteger(sequence) ||
    typeof bytes !== "number" ||
    typeof sequence !== "number" ||
    generation === undefined
  )
    return undefined;
  return { bytes, sequence, generation, runId: text(field(record, "index"), "runId") };
}

/**
 * Rebuilds run state from the committed prefix of a v2 `<run>.json.events.jsonl` log, mirroring
 * the plugin's `applyDelta`. Returns undefined when the prefix does not match the head.
 */
export function replayPiWorkflowRunLog(
  head: PiWorkflowRunLogHead,
  committed: string,
): Record<string, unknown> | undefined {
  if (!committed.endsWith("\n")) return undefined;
  const state: Record<string, unknown> = {};
  let sequence = 0;
  for (const line of committed.slice(0, -1).split("\n")) {
    let entry: unknown;
    try {
      entry = JSON.parse(line);
    } catch {
      return undefined;
    }
    if (text(entry, "generation") !== head.generation || field(entry, "sequence") !== ++sequence)
      return undefined;
    const delta = field(entry, "delta");
    if (typeof field(delta, "settleAgentsAt") === "string" && Array.isArray(state.agents)) {
      // The plugin settles agents still in flight when a run pauses.
      state.agents = state.agents.map((agent) =>
        text(agent, "status") === "queued" || text(agent, "status") === "running"
          ? { ...(isPlainRecord(agent) ? agent : {}), status: "skipped" }
          : agent,
      );
    }
    const remove = field(delta, "remove");
    for (const key of Array.isArray(remove) ? remove : [])
      if (typeof key === "string") delete state[key];
    const set = field(delta, "set");
    if (isPlainRecord(set)) Object.assign(state, set);
    const objects = field(delta, "objects");
    for (const [key, patch] of Object.entries(isPlainRecord(objects) ? objects : {})) {
      const current = state[key];
      const value = isPlainRecord(current) ? current : {};
      const removed = field(patch, "remove");
      for (const name of Array.isArray(removed) ? removed : [])
        if (typeof name === "string") delete value[name];
      const patchSet = field(patch, "set");
      if (isPlainRecord(patchSet)) Object.assign(value, patchSet);
      state[key] = value;
    }
    const arrays = field(delta, "arrays");
    for (const [key, patch] of Object.entries(isPlainRecord(arrays) ? arrays : {})) {
      const length = field(patch, "length");
      if (typeof length !== "number" || !Number.isSafeInteger(length) || length < 0)
        return undefined;
      const current = state[key];
      const value: unknown[] = Array.isArray(current) ? current : [];
      value.length = length;
      const cells = field(patch, "set");
      for (const cell of Array.isArray(cells) ? cells : []) {
        const index: unknown = Array.isArray(cell) ? cell[0] : undefined;
        if (
          typeof index !== "number" ||
          !Number.isSafeInteger(index) ||
          index < 0 ||
          index >= length
        )
          return undefined;
        value[index] = cell[1];
      }
      state[key] = value;
    }
  }
  if (sequence !== head.sequence || (head.runId !== undefined && state.runId !== head.runId))
    return undefined;
  return state;
}

interface WorkflowRun<Context> {
  readonly context: Context;
  readonly toolCallId: string;
  readonly nativeTaskId: string;
  readonly startedAt: DateTime.Utc;
  name: string | undefined;
  runId: string | undefined;
  background: boolean;
  completed: boolean;
  snapshot: PiWorkflowSnapshot | undefined;
  fingerprint: string | undefined;
  /** Last polled v2 head commit, so unchanged runs skip re-reading their log. */
  logCommit: string | undefined;
}

type WorkflowStatus = Extract<
  OrchestrationV2Subagent["status"],
  "running" | "completed" | "failed" | "interrupted" | "waiting"
>;
const decodeRunFile = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const encodeFingerprint = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export const makePiWorkflowRuns = Effect.fnUntraced(function* <Context>(options: {
  readonly directory: string;
  readonly fileSystem: FileSystem.FileSystem;
  readonly scope: Scope.Scope;
  readonly publish: (
    run: WorkflowRun<Context>,
    snapshot: PiWorkflowSnapshot,
    status: WorkflowStatus,
    emittedAt: DateTime.Utc,
  ) => Effect.Effect<void>;
}) {
  const runs = new Map<string, WorkflowRun<Context>>();
  const byTool = new Map<string, WorkflowRun<Context>>();
  let polling: Fiber.Fiber<void> | null = null;

  const emitRun = Effect.fnUntraced(function* (
    run: WorkflowRun<Context>,
    snapshot: PiWorkflowSnapshot | undefined,
    terminal?: WorkflowStatus,
  ) {
    if (run.completed) return;
    if (snapshot !== undefined) run.snapshot = snapshot;
    if (snapshot?.name) run.name = snapshot.name;
    if (snapshot?.runId && isPiWorkflowRunId(snapshot.runId)) run.runId = snapshot.runId;
    const current = run.snapshot;
    const resolved: PiWorkflowSnapshot = {
      name: run.name,
      runId: run.runId,
      status: current?.status ?? "running",
      currentPhase: current?.currentPhase,
      workflow: {
        ...current?.workflow,
        phases: current?.workflow.phases ?? [],
        agents: current?.workflow.agents ?? [],
        ...(run.name === undefined ? {} : { name: run.name }),
        ...(run.runId === undefined ? {} : { runHandles: { runId: run.runId } }),
      },
    };
    const status =
      terminal ?? (resolved.status === "paused" ? "interrupted" : (resolved.status ?? "running"));
    const fingerprint = encodeFingerprint([status, resolved]);
    if (fingerprint === run.fingerprint) return;
    run.fingerprint = fingerprint;
    run.completed = (status !== "running" && status !== "waiting") || resolved.status === "paused";
    yield* options.publish(run, resolved, status, yield* DateTime.now);
    if (run.completed) {
      runs.delete(run.nativeTaskId);
      for (const [key, value] of byTool) if (value === run) byTool.delete(key);
    }
  });

  const readRunState = Effect.fnUntraced(function* (run: WorkflowRun<Context>, runId: string) {
    const path = NodePath.join(options.directory, `${runId}.json`);
    const record = yield* options.fileSystem
      .readFileString(path)
      .pipe(Effect.flatMap(decodeRunFile), Effect.option);
    if (record._tag === "None") return undefined;
    const head = piWorkflowRunLogHead(record.value);
    if (head === undefined) return record.value;
    const commit = `${head.generation}:${head.sequence}`;
    if (commit === run.logCommit) return undefined;
    const log = yield* options.fileSystem.readFile(`${path}.events.jsonl`).pipe(Effect.option);
    if (log._tag === "None" || log.value.byteLength < head.bytes) return undefined;
    const state = replayPiWorkflowRunLog(
      head,
      new TextDecoder().decode(log.value.subarray(0, head.bytes)),
    );
    if (state !== undefined) run.logCommit = commit;
    return state;
  });

  const poll = Effect.fnUntraced(function* (run: WorkflowRun<Context>) {
    if (run.completed || run.runId === undefined || !isPiWorkflowRunId(run.runId)) return;
    // The plugin rewrites in place; a torn or missing file is retried on the next tick.
    const state = yield* readRunState(run, run.runId);
    const snapshot = state === undefined ? undefined : normalizePiWorkflowSnapshot(state);
    if (snapshot !== undefined) yield* emitRun(run, snapshot);
  });

  const hasPendingBackgroundWork = () =>
    [...runs.values()].some((run) => run.background && !run.completed);
  const ensurePolling = Effect.fnUntraced(function* () {
    if (polling !== null || !hasPendingBackgroundWork()) return;
    polling = yield* Effect.gen(function* () {
      while (hasPendingBackgroundWork()) {
        yield* Effect.sleep("2 seconds");
        for (const run of runs.values()) if (run.background) yield* poll(run);
      }
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          polling = null;
        }),
      ),
      Effect.forkIn(options.scope),
    );
  });

  const finalize = Effect.fnUntraced(function* (
    context: Context | undefined,
    status: "failed" | "interrupted",
    includeBackground: boolean,
  ) {
    for (const run of runs.values()) {
      if (
        (context === undefined || run.context === context) &&
        (includeBackground || !run.background)
      )
        yield* emitRun(run, undefined, status);
    }
  });
  const stop = Effect.gen(function* () {
    if (polling !== null) yield* Fiber.interrupt(polling);
    yield* finalize(undefined, "interrupted", true);
  });
  yield* Effect.addFinalizer(() => stop);

  return {
    hasPendingBackgroundWork,
    stop,
    finalizeTurn: (context: Context, status: "failed" | "interrupted") =>
      finalize(context, status, false),
    trackTool: Effect.fnUntraced(function* (
      context: Context,
      toolName: string,
      toolCallId: string,
      args: unknown,
      result: unknown,
      phase: "start" | "update" | "end",
      isError: boolean,
    ) {
      if (toolName !== "workflow" && toolName !== "workflow_control") return;
      const details = field(result, "details");
      const snapshot =
        normalizePiWorkflowSnapshot(details) ?? normalizePiWorkflowSnapshot(field(details, "run"));
      const backgroundId = isError ? undefined : piWorkflowBackgroundRunId(toolName, result);
      const runId = backgroundId ?? snapshot?.runId ?? preview(field(args, "runId"));
      if (runId !== undefined && !isPiWorkflowRunId(runId)) return;
      let run =
        byTool.get(toolCallId) ??
        (runId === undefined
          ? undefined
          : [...runs.values()].find((candidate) => candidate.runId === runId));
      if (!run) {
        if (toolName === "workflow_control" && snapshot === undefined && backgroundId === undefined)
          return;
        run = {
          context,
          toolCallId,
          nativeTaskId: `${toolCallId}:workflow`,
          startedAt: yield* DateTime.now,
          name: piWorkflowCallName(args),
          runId,
          background: false,
          completed: false,
          snapshot: undefined,
          fingerprint: undefined,
          logCommit: undefined,
        };
        runs.set(run.nativeTaskId, run);
      }
      byTool.set(toolCallId, run);
      if (backgroundId !== undefined) {
        run.runId = backgroundId;
        run.background = true;
      }
      const terminal =
        phase === "end" && !run.background && toolName === "workflow"
          ? isError
            ? "failed"
            : "completed"
          : undefined;
      yield* emitRun(run, snapshot, terminal);
      if (run.background) {
        yield* poll(run);
        yield* ensurePolling();
      }
    }),
  };
});
