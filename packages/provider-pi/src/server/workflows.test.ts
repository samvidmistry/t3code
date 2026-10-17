import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";
import {
  isPiWorkflowRunId,
  makePiWorkflowRuns,
  normalizePiWorkflowSnapshot,
  piWorkflowBackgroundRunId,
  piWorkflowCallName,
  piWorkflowProgressSummary,
  piWorkflowRunsDirectory,
} from "./workflows.ts";

const runFile = (overrides: Record<string, unknown> = {}) => ({
  workflowName: "Review",
  runId: "review-123",
  status: "running",
  phases: ["Explore"],
  agents: [],
  ...overrides,
});

describe("Pi workflow snapshots", () => {
  it("normalizes persisted and streaming snapshots into workflow members", () => {
    const snapshot = normalizePiWorkflowSnapshot(
      runFile({
        agents: [
          {
            id: 1,
            label: "Reader",
            phase: "Explore",
            status: "running",
            model: "gpt",
            tokenUsage: { total: 123 },
          },
          { id: 2, label: "Skipped", status: "skipped" },
          { id: 3, label: "Failed", status: "error", error: "command failed" },
          { id: 4, status: "queued" },
          { id: 5, status: "unknown" },
          null,
        ],
        tokenUsage: { total: 456 },
      }),
    );
    expect(snapshot?.workflow).toEqual({
      name: "Review",
      runHandles: { runId: "review-123" },
      phases: [{ index: 0, title: "Explore" }],
      totalTokens: 456,
      agents: [
        {
          index: 1,
          label: "Reader",
          phaseIndex: 0,
          phaseTitle: "Explore",
          state: "running",
          model: "gpt",
          totalTokens: 123,
        },
        { index: 2, label: "Skipped", state: "completed" },
        { index: 3, label: "Failed", state: "failed", result: "command failed" },
        { index: 4, label: "agent 4", state: "queued" },
      ],
    });
    expect(normalizePiWorkflowSnapshot({ phases: [] })).toBeUndefined();
    expect(normalizePiWorkflowSnapshot({ agents: [] })).toBeUndefined();
  });

  it.each([
    ["aborted", "interrupted"],
    ["pending", "running"],
    ["paused", "paused"],
    ["failed", "failed"],
    ["completed", "completed"],
  ])("maps run status %s to %s", (status, expected) => {
    expect(normalizePiWorkflowSnapshot(runFile({ status }))?.status).toBe(expected);
  });

  it("explains live empty background runs and summarizes phase activity", () => {
    expect(piWorkflowProgressSummary(normalizePiWorkflowSnapshot(runFile())!)).toBe(
      "Explore · agents working; each appears when it finishes",
    );
    expect(
      piWorkflowProgressSummary(
        normalizePiWorkflowSnapshot(runFile({ status: undefined, phases: [] }))!,
      ),
    ).toBe("starting");
    expect(
      piWorkflowProgressSummary(
        normalizePiWorkflowSnapshot(
          runFile({
            currentPhase: "Critique",
            agents: [
              { id: 1, status: "done" },
              { id: 2, status: "error" },
              { id: 3, status: "running" },
            ],
          }),
        )!,
      ),
    ).toBe("Critique · 2/3 agents done, 1 running, 1 failed");
  });

  it("identifies background launches and resumes without accepting traversal paths", () => {
    expect(
      piWorkflowBackgroundRunId("workflow", { details: { background: true, runId: "run-1" } }),
    ).toBe("run-1");
    expect(
      piWorkflowBackgroundRunId("workflow_control", {
        details: { action: "resume", result: "resumed", run: { runId: "run-1" } },
      }),
    ).toBe("run-1");
    expect(piWorkflowCallName({ script: 'workflow({ name: "Review" })' })).toBe("Review");
    expect(piWorkflowRunsDirectory("/projects/Hello World", "/home/test")).toMatch(
      /^\/home\/test\/\.pi\/workflows\/projects\/hello-world-[a-f0-9]{12}\/runs$/,
    );
    for (const id of ["../escape", ".hidden", "/absolute", "bad/name", ""])
      expect(isPiWorkflowRunId(id)).toBe(false);
    expect(isPiWorkflowRunId("valid-run_123.json")).toBe(true);
  });

  it.effect("polls background run files until terminal, dedupes snapshots, and stops cleanly", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const scope = yield* Effect.scope;
      const published = yield* Queue.unbounded<{ status: string; progress: string }>();
      const reads = yield* Queue.unbounded<string>();
      let persisted = runFile();
      const controller = yield* makePiWorkflowRuns({
        directory: "/runs",
        scope,
        fileSystem: {
          ...fileSystem,
          readFileString: (path) =>
            Queue.offer(reads, path).pipe(Effect.as(JSON.stringify(persisted))),
        },
        publish: (_run, snapshot, status) =>
          Queue.offer(published, { status, progress: piWorkflowProgressSummary(snapshot) }).pipe(
            Effect.asVoid,
          ),
      });
      yield* controller.trackTool(
        "turn",
        "workflow",
        "tool",
        { name: "Review" },
        { details: { background: true, runId: "review-123" } },
        "end",
        false,
      );
      expect(yield* Queue.take(reads)).toBe("/runs/review-123.json");
      yield* Queue.take(published);
      expect((yield* Queue.take(published)).progress).toBe(
        "Explore · agents working; each appears when it finishes",
      );
      expect(controller.hasPendingBackgroundWork()).toBe(true);
      yield* controller.finalizeTurn("turn", "interrupted");
      expect(controller.hasPendingBackgroundWork()).toBe(true);
      yield* TestClock.adjust("2 seconds");
      yield* Queue.take(reads);
      expect(yield* Queue.size(published)).toBe(0);
      persisted = runFile({
        status: "completed",
        agents: [{ id: 1, label: "Reader", status: "done" }],
      });
      yield* TestClock.adjust("2 seconds");
      expect((yield* Queue.take(published)).status).toBe("completed");
      expect(controller.hasPendingBackgroundWork()).toBe(false);
      yield* controller.stop;
      expect(yield* Queue.size(published)).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("replays only the committed prefix of v2 run logs", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const scope = yield* Effect.scope;
      const published = yield* Queue.unbounded<{ status: string; progress: string }>();
      const entry = (sequence: number, delta: Record<string, unknown>) =>
        JSON.stringify({ generation: "g1", sequence, previous: "", delta }) + "\n";
      const committed =
        entry(1, {
          set: { runId: "review-123", workflowName: "Review", status: "running", phases: [] },
          remove: [],
          arrays: {
            phases: { length: 1, set: [[0, "Explore"]] },
            agents: { length: 1, set: [[0, { id: 1, label: "Reader", status: "running" }]] },
          },
        }) +
        entry(2, {
          set: { status: "completed" },
          remove: ["pendingDelivery"],
          arrays: { agents: { length: 1, set: [[0, { id: 1, label: "Reader", status: "done" }]] } },
        });
      // A trailing write the plugin has not committed to the head yet.
      const log = committed + entry(3, { set: { status: "failed" }, remove: [], arrays: {} });
      const head = {
        format: "pi-workflow-run-v2",
        generation: "g1",
        sequence: 2,
        bytes: new TextEncoder().encode(committed).byteLength,
        hash: "unused",
        index: { runId: "review-123" },
        summary: {},
        keys: [],
      };
      const controller = yield* makePiWorkflowRuns({
        directory: "/runs",
        scope,
        fileSystem: {
          ...fileSystem,
          readFileString: () => Effect.succeed(JSON.stringify(head)),
          readFile: (path) =>
            path === "/runs/review-123.json.events.jsonl"
              ? Effect.succeed(new TextEncoder().encode(log))
              : Effect.die(`unexpected read ${path}`),
        },
        publish: (_run, snapshot, status) =>
          Queue.offer(published, { status, progress: piWorkflowProgressSummary(snapshot) }).pipe(
            Effect.asVoid,
          ),
      });
      yield* controller.trackTool(
        "turn",
        "workflow",
        "tool",
        { name: "Review" },
        { details: { background: true, runId: "review-123" } },
        "end",
        false,
      );
      yield* Queue.take(published);
      expect(yield* Queue.take(published)).toEqual({
        status: "completed",
        progress: "Explore · 1/1 agents done",
      });
      expect(controller.hasPendingBackgroundWork()).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "tracks workflow_control resumes and settles paused runs without leaving live cards",
    () =>
      Effect.gen(function* () {
        const scope = yield* Effect.scope;
        const fileSystem = yield* FileSystem.FileSystem;
        const published = yield* Queue.unbounded<{ status: string; runId: string | undefined }>();
        const controller = yield* makePiWorkflowRuns({
          directory: "/runs",
          scope,
          fileSystem: {
            ...fileSystem,
            readFileString: () => Effect.succeed(JSON.stringify(runFile({ status: "paused" }))),
          },
          publish: (_run, snapshot, status) =>
            Queue.offer(published, { status, runId: snapshot.workflow.runHandles?.runId }).pipe(
              Effect.asVoid,
            ),
        });
        yield* controller.trackTool(
          "turn",
          "workflow_control",
          "resume",
          { action: "resume", runId: "review-123" },
          undefined,
          "start",
          false,
        );
        expect(yield* Queue.size(published)).toBe(0);
        yield* controller.trackTool(
          "turn",
          "workflow_control",
          "resume",
          { action: "resume", runId: "review-123" },
          {
            details: { action: "resume", result: "resumed", run: { runId: "review-123" } },
          },
          "end",
          false,
        );
        expect(yield* Queue.take(published)).toEqual({ status: "running", runId: "review-123" });
        expect(yield* Queue.take(published)).toEqual({
          status: "interrupted",
          runId: "review-123",
        });
        expect(controller.hasPendingBackgroundWork()).toBe(false);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("terminalizes live runs when the session stops", () =>
    Effect.gen(function* () {
      const scope = yield* Effect.scope;
      const fileSystem = yield* FileSystem.FileSystem;
      const published = yield* Queue.unbounded<string>();
      const controller = yield* makePiWorkflowRuns({
        directory: "/runs",
        scope,
        fileSystem,
        publish: (_run, _snapshot, status) => Queue.offer(published, status).pipe(Effect.asVoid),
      });
      yield* controller.trackTool(
        "turn",
        "workflow",
        "tool",
        { name: "Review" },
        undefined,
        "start",
        false,
      );
      expect(yield* Queue.take(published)).toBe("running");
      yield* controller.stop;
      expect(yield* Queue.take(published)).toBe("interrupted");
      yield* controller.stop;
      expect(yield* Queue.size(published)).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
