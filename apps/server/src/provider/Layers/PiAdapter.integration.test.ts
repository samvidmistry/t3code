import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  ApprovalRequestId,
  PiSettings,
  ProviderDriverKind,
  ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import { make as makeBackgroundLiveness } from "../../orchestration/ThreadBackgroundLiveness.ts";
import { runtimeEventToActivities } from "../../orchestration/Layers/ProviderRuntimeIngestion.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import { makePiAdapter } from "./PiAdapter.ts";
import type {
  AgentSessionEvent,
  PiRpcTransport,
  PiStdoutMessage,
  RpcCommand,
  RpcExtensionUIRequest,
  RpcExtensionUIResponse,
  RpcResponse,
} from "./PiRpcClient.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);
const decodeRuntimeEvent = Schema.decodeUnknownEffect(ProviderRuntimeEvent);
const PI = ProviderDriverKind.make("pi");

const HarnessLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-pi-adapter-integration-",
}).pipe(Layer.provideMerge(NodeServices.layer));

interface FakePiTransport {
  readonly transport: PiRpcTransport;
  readonly commands: Array<RpcCommand>;
  readonly extensionResponses: Array<RpcExtensionUIResponse>;
  readonly pushEvent: (event: AgentSessionEvent) => Effect.Effect<void>;
  readonly pushExtensionUI: (request: RpcExtensionUIRequest) => Effect.Effect<void>;
  readonly setResponse: (commandType: string, response: RpcResponse) => void;
  readonly wasKilled: () => boolean;
}

const asResponse = (value: unknown): RpcResponse => value as RpcResponse;

const makeFakePiRpcTransport = Effect.gen(function* () {
  const messages = yield* Queue.unbounded<PiStdoutMessage>();
  const commands: Array<RpcCommand> = [];
  const extensionResponses: Array<RpcExtensionUIResponse> = [];
  const responses = new Map<string, RpcResponse>();
  let killed = false;
  responses.set(
    "get_state",
    asResponse({
      type: "response",
      id: "x",
      command: "get_state",
      success: true,
      data: {
        sessionFile: "/tmp/pi-session.json",
        model: { contextWindow: 200_000 },
        autoCompactionEnabled: true,
      },
    }),
  );
  responses.set(
    "get_commands",
    asResponse({
      type: "response",
      id: "x",
      command: "get_commands",
      success: true,
      data: { commands: [{ name: "t3-approval-gate", source: "extension" }] },
    }),
  );

  const transport: PiRpcTransport = {
    writeCommand: (command) =>
      Effect.sync(() => {
        commands.push(command);
      }),
    writeExtensionResponse: (response) =>
      Effect.sync(() => {
        extensionResponses.push(response);
      }),
    request: (command) => Effect.succeed(responses.get((command as { type: string }).type)),
    messages,
    kill: Effect.sync(() => {
      killed = true;
    }),
  };

  return {
    transport,
    wasKilled: () => killed,
    commands,
    extensionResponses,
    pushEvent: (event) => Queue.offer(messages, { _tag: "event", event }).pipe(Effect.asVoid),
    pushExtensionUI: (request) =>
      Queue.offer(messages, { _tag: "extension-ui", request }).pipe(Effect.asVoid),
    setResponse: (commandType, response) => {
      responses.set(commandType, response);
    },
  } satisfies FakePiTransport;
});

const makePiAdapterForTest = (settings: PiSettings) =>
  Effect.gen(function* () {
    const fake = yield* makeFakePiRpcTransport;
    const adapter = yield* makePiAdapter(settings, {
      makeTransport: () => Effect.succeed(fake.transport),
    });
    return { adapter, fake } as const;
  });

const collectEvents = (
  adapter: PiAdapterShape,
  threadId: ThreadId,
  isTerminal: (event: ProviderRuntimeEvent) => boolean,
) =>
  Effect.gen(function* () {
    const store = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
    const fiber = yield* adapter.streamEvents.pipe(
      Stream.filter((event) => event.threadId === threadId),
      Stream.takeUntil(isTerminal),
      Stream.runForEach((event) => Ref.update(store, (events) => [...events, event])),
      Effect.forkChild,
    );
    return { store, fiber } as const;
  });

const enabledSettings = (overrides: Record<string, unknown> = {}) =>
  decodePiSettings({ enabled: true, ...overrides });

const bgToolEnd = (toolName: string, details: unknown, toolCallId = "bg-launch") =>
  ({
    type: "tool_execution_end",
    toolName,
    toolCallId,
    result: { content: [], details },
    isError: false,
  }) as AgentSessionEvent;

const bgResult = (status = "done", jobId = 1) =>
  ({
    type: "message_end",
    message: {
      role: "custom",
      customType: "bg-result",
      content: "Background output",
      display: true,
      timestamp: 0,
      details: {
        jobId,
        command: "claude --print review",
        description: "Review changes",
        status,
        durationMs: 100,
        stdout: "large output",
      },
    },
  }) as AgentSessionEvent;

it.layer(HarnessLayer)("PiAdapter integration", (it) => {
  it.effect("keeps bg live after the turn and settles from an idle custom completion", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-bg-idle");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "task.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "start background review", attachments: [] });
      yield* fake.pushEvent(
        bgToolEnd("bg", {
          jobId: 1,
          command: "claude --print review",
          description: "Review changes",
        }),
      );
      yield* fake.pushEvent({ type: "agent_end", messages: [], willRetry: false });
      yield* fake.pushEvent(bgResult());
      yield* Fiber.join(collected.fiber);
      const events = yield* Ref.get(collected.store);
      const tasks = events.filter((event) => event.type.startsWith("task."));
      expect(tasks.map((event) => event.type)).toEqual([
        "task.started",
        "task.progress",
        "task.completed",
      ]);
      const start = events.find((event) => event.type === "task.started")!;
      const end = events.find((event) => event.type === "task.completed")!;
      expect(end.turnId).toBe(start.turnId);
      expect(end.payload).toMatchObject({
        taskId: start.payload.taskId,
        taskType: "shell",
        title: "Review changes",
        status: "completed",
        toolUseId: "bg-launch",
        usage: { durationMs: 100 },
      });
      expect(events.indexOf(end)).toBeGreaterThan(
        events.findIndex((event) => event.type === "turn.completed"),
      );
      const liveness = makeBackgroundLiveness();
      for (const event of events) {
        yield* decodeRuntimeEvent(event);
        if (
          event.type === "task.started" ||
          event.type === "task.progress" ||
          event.type === "task.completed"
        ) {
          expect(event.raw).toBeUndefined();
          expect(runtimeEventToActivities(event)[0]?.payload).toMatchObject({
            agentKind: "background",
            taskType: "shell",
            title: "Review changes",
          });
          liveness.recordTaskLiveness({
            threadId,
            taskId: event.payload.taskId,
            taskType: event.payload.taskType,
            status: "status" in event.payload ? event.payload.status : undefined,
            kind:
              event.type === "task.started"
                ? "started"
                : event.type === "task.progress"
                  ? "progress"
                  : "completed",
          });
        }
        if (event.type === "turn.completed")
          expect(liveness.getThreadBackgroundLiveness(threadId)).toBe("monitoring");
      }
      expect(liveness.getThreadBackgroundLiveness(threadId)).toBeNull();
    }),
  );

  it.effect("reconciles bg_status without resurrecting settled or historical jobs", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-bg-status");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "check jobs", attachments: [] });
      const job = { id: 1, command: "tests", description: "Run tests" };
      yield* fake.pushEvent(
        bgToolEnd("bg_status", {
          jobs: [{ ...job, status: "running" }, { ...job, id: 8, status: "done" }, null],
        }),
      );
      yield* fake.pushEvent(bgToolEnd("bg_status", { jobs: [{ ...job, status: "running" }] }));
      yield* fake.pushEvent(bgToolEnd("bg_status", { jobs: [{ ...job, status: "killed" }] }));
      yield* fake.pushEvent(bgResult("killed"));
      yield* fake.pushEvent(bgToolEnd("bg_status", { jobs: [{ ...job, status: "running" }] }));
      yield* fake.pushEvent({ type: "agent_end", messages: [], willRetry: false });
      yield* Fiber.join(collected.fiber);
      const events = yield* Ref.get(collected.store);
      expect(events.filter((event) => event.type === "task.started")).toHaveLength(1);
      expect(events.filter((event) => event.type === "task.progress")).toHaveLength(1);
      const completions = events.filter((event) => event.type === "task.completed");
      expect(completions).toHaveLength(1);
      expect(completions[0]?.payload).toMatchObject({
        status: "stopped",
        title: "Run tests",
        taskType: "shell",
      });
    }),
  );

  it.effect.each(["running", "launching"] as const)(
    "stops the owning Pi process for %s bg jobs, keeping its resume cursor",
    (state) =>
      Effect.gen(function* () {
        const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
        const threadId = ThreadId.make("pi-bg-stop");
        const ready = yield* Deferred.make<void>();
        const store = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
        const fiber = yield* adapter.streamEvents.pipe(
          Stream.filter((event) => event.threadId === threadId),
          Stream.takeUntil((event) => event.type === "session.exited"),
          Stream.runForEach((event) =>
            Ref.update(store, (events) => [...events, event]).pipe(
              Effect.andThen(
                event.type === (state === "launching" ? "item.started" : "turn.completed")
                  ? Deferred.succeed(ready, undefined)
                  : Effect.void,
              ),
            ),
          ),
          Effect.forkChild,
        );
        const session = yield* adapter.startSession({
          threadId,
          provider: PI,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        yield* adapter.sendTurn({ threadId, input: "run jobs", attachments: [] });
        yield* fake.pushEvent({
          type: "tool_execution_start",
          toolName: "bg",
          toolCallId: "bg-launch",
          args: { command: "tests" },
        });
        if (state === "running") {
          yield* fake.pushEvent(
            bgToolEnd("bg", { jobId: 1, command: "tests", description: "Run tests" }),
          );
          yield* fake.pushEvent({ type: "agent_end", messages: [], willRetry: false });
        }
        yield* Deferred.await(ready);
        expect(fake.wasKilled()).toBe(false);
        yield* adapter.interruptTurn(threadId);
        yield* Fiber.join(fiber);
        expect(fake.wasKilled()).toBe(true);
        expect(yield* adapter.hasSession(threadId)).toBe(false);
        expect(session.resumeCursor).toEqual({ sessionFile: "/tmp/pi-session.json" });
        const events = yield* Ref.get(store);
        expect(
          events
            .filter((event) => event.type === "task.completed")
            .map((event) => event.payload.status),
        ).toEqual(state === "running" ? ["stopped"] : []);
        expect(fake.commands.some((command) => command.type === "abort")).toBe(false);
      }),
  );

  it.effect("retains the launching turn when bg finishes during a later turn", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-bg-later-turn");
      const first = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "run tests", attachments: [] });
      yield* fake.pushEvent(
        bgToolEnd("bg", { jobId: 1, command: "tests", description: "Run tests" }),
      );
      yield* fake.pushEvent({ type: "agent_end", messages: [], willRetry: false });
      yield* Fiber.join(first.fiber);
      const start = (yield* Ref.get(first.store)).find((event) => event.type === "task.started")!;
      const second = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.sendTurn({ threadId, input: "continue working", attachments: [] });
      yield* fake.pushEvent(bgResult());
      yield* fake.pushEvent({ type: "agent_end", messages: [], willRetry: false });
      yield* Fiber.join(second.fiber);
      const events = yield* Ref.get(second.store);
      const completion = events.find((event) => event.type === "task.completed")!;
      expect(completion.payload.taskId).toBe(start.payload.taskId);
      expect(completion.turnId).toBe(start.turnId);
      expect(completion.turnId).not.toBe(
        events.find((event) => event.type === "turn.completed")?.turnId,
      );
    }),
  );

  it.effect("settles jobs on rollback and gives reused extension IDs new task identities", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-bg-rollback");
      const first = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "run tests", attachments: [] });
      const launch = bgToolEnd("bg", { jobId: 1, command: "tests", description: "Run tests" });
      yield* fake.pushEvent(launch);
      yield* fake.pushEvent({ type: "agent_end", messages: [], willRetry: false });
      yield* Fiber.join(first.fiber);
      const start = (yield* Ref.get(first.store)).find((event) => event.type === "task.started")!;
      fake.setResponse(
        "get_fork_messages",
        asResponse({
          type: "response",
          command: "get_fork_messages",
          success: true,
          data: { messages: [{ entryId: "user-1", text: "run tests" }] },
        }),
      );
      fake.setResponse(
        "new_session",
        asResponse({
          type: "response",
          command: "new_session",
          success: true,
          data: { cancelled: false },
        }),
      );
      const second = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.rollbackThread(threadId, 1);
      yield* adapter.sendTurn({ threadId, input: "new job", attachments: [] });
      yield* fake.pushEvent(launch);
      yield* fake.pushEvent({ type: "agent_end", messages: [], willRetry: false });
      yield* Fiber.join(second.fiber);
      const events = yield* Ref.get(second.store);
      expect(events.find((event) => event.type === "task.completed")?.payload).toMatchObject({
        taskId: start.payload.taskId,
        status: "stopped",
      });
      const restarted = events.find((event) => event.type === "task.started")!;
      expect(restarted.payload.taskId).not.toBe(start.payload.taskId);
    }),
  );

  it.effect("handles completion racing ahead of the launch result without restarting the job", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-bg-race");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "run quick job", attachments: [] });
      yield* fake.pushEvent(bgResult("failed"));
      yield* fake.pushEvent(
        bgToolEnd("bg", { jobId: 1, command: "tests", description: "Run tests" }),
      );
      yield* fake.pushEvent(bgResult("failed"));
      yield* fake.pushEvent({ type: "agent_end", messages: [], willRetry: false });
      yield* Fiber.join(collected.fiber);
      const events = yield* Ref.get(collected.store);
      expect(events.filter((event) => event.type === "task.started")).toHaveLength(1);
      expect(events.filter((event) => event.type === "task.progress")).toHaveLength(0);
      expect(
        events
          .filter((event) => event.type === "task.completed")
          .map((event) => event.payload.status),
      ).toEqual(["failed"]);
      yield* adapter.interruptTurn(threadId);
      expect(fake.wasKilled()).toBe(false);
      expect(fake.commands.at(-1)?.type).toBe("abort");
    }),
  );
  it.effect(
    "streams progress for same-length text, tool call, tool result, and usage-only changes",
    () =>
      Effect.gen(function* () {
        const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
        const threadId = ThreadId.make("pi-int-subagent-stream");
        const collected = yield* collectEvents(
          adapter,
          threadId,
          (event) => event.type === "turn.completed",
        );
        yield* adapter.startSession({
          threadId,
          provider: PI,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        yield* adapter.sendTurn({ threadId, input: "delegate", attachments: [] });
        yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
        yield* fake.pushEvent({
          type: "tool_execution_start",
          toolCallId: "sa-6",
          toolName: "subagent",
          args: { agent: "worker", task: "do the thing" },
        } as AgentSessionEvent);

        const child = (
          messages: ReadonlyArray<Record<string, unknown>>,
          usage?: Record<string, number>,
        ) => subagentResult({ exitCode: -1, messages, ...(usage ? { usage } : {}) });
        const text = (t: string) => ({ role: "assistant", content: [{ type: "text", text: t }] });
        const toolCall = {
          role: "assistant",
          content: [{ type: "toolCall", id: "call-1", name: "read", arguments: {} }],
        };
        const toolResult = {
          role: "toolResult",
          toolCallId: "call-1",
          toolName: "read",
          content: [{ type: "text", text: "contents" }],
          isError: false,
        };

        // 1: text "aaaa"
        yield* fake.pushEvent(subagentUpdate("sa-6", "single", [child([text("aaaa")])]));
        // 2: same-length but different text "bbbb" (length-only cursor would miss this)
        yield* fake.pushEvent(subagentUpdate("sa-6", "single", [child([text("bbbb")])]));
        // 3: an assistant tool call
        yield* fake.pushEvent(subagentUpdate("sa-6", "single", [child([text("bbbb"), toolCall])]));
        // 4: the corresponding tool result
        yield* fake.pushEvent(
          subagentUpdate("sa-6", "single", [child([text("bbbb"), toolCall, toolResult])]),
        );
        // 5: usage-only change (token streaming), same transcript
        yield* fake.pushEvent(
          subagentUpdate("sa-6", "single", [
            child([text("bbbb"), toolCall, toolResult], {
              input: 100,
              output: 21,
              cacheRead: 0,
              cacheWrite: 0,
              cost: 0,
              contextTokens: 121,
              turns: 1,
            }),
          ]),
        );
        // 6: exact duplicate of #5 -> deduped, no new progress
        yield* fake.pushEvent(
          subagentUpdate("sa-6", "single", [
            child([text("bbbb"), toolCall, toolResult], {
              input: 100,
              output: 21,
              cacheRead: 0,
              cacheWrite: 0,
              cost: 0,
              contextTokens: 121,
              turns: 1,
            }),
          ]),
        );
        yield* fake.pushEvent(subagentEnd("sa-6", "single", [subagentResult()]));
        yield* fake.pushEvent({
          type: "agent_end",
          messages: [],
          willRetry: false,
        } as AgentSessionEvent);

        const events = yield* Fiber.join(collected.fiber).pipe(
          Effect.flatMap(() => Ref.get(collected.store)),
        );
        const progress = events.filter((e) => e.type === "task.progress");
        const summaries = progress.flatMap((e) =>
          e.type === "task.progress" && e.payload.summary ? [e.payload.summary] : [],
        );
        expect(summaries).toEqual(["aaaa", "bbbb", "Using read", "Ran read", "Ran read"]);
        expect(progress.every((event) => event.payload.description === "do the thing")).toBe(true);
        // Pi's raw subagent snapshots contain cumulative child transcripts, so
        // canonical task events must remain compact and transcript-free.
        const taskEvents = events.filter((event) => event.type.startsWith("task."));
        expect(taskEvents.some((event) => "raw" in event)).toBe(false);
        // the tool-call progress carries the resolved lastToolName
        const usingRead = progress.find(
          (e) => e.type === "task.progress" && e.payload.summary === "Using read",
        );
        if (usingRead?.type === "task.progress") {
          expect(usingRead.payload.lastToolName).toBe("read");
        }
        expect(events.filter((e) => e.type === "task.completed")).toHaveLength(1);
      }),
  );

  it.effect("defers task.started for untouched queued parallel placeholders", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-subagent-placeholder");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "delegate two", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_start",
        toolCallId: "sa-7",
        toolName: "subagent",
        args: {},
      } as AgentSessionEvent);
      // child 0 has begun (real activity); child 1 is still a queued placeholder
      yield* fake.pushEvent(
        subagentUpdate("sa-7", "parallel", [
          subagentResult({
            agent: "a",
            task: "task a",
            exitCode: -1,
            messages: [{ role: "assistant", content: [{ type: "text", text: "a working" }] }],
          }),
          { agent: "b", task: "task b", exitCode: -1, messages: [], usage: {} },
        ]),
      );

      yield* fake.pushEvent(
        subagentEnd("sa-7", "parallel", [
          subagentResult({ agent: "a", task: "task a", exitCode: 0 }),
          subagentResult({ agent: "b", task: "task b", exitCode: 0 }),
        ]),
      );
      yield* fake.pushEvent({
        type: "agent_end",
        messages: [],
        willRetry: false,
      } as AgentSessionEvent);

      const events = yield* Fiber.join(collected.fiber).pipe(
        Effect.flatMap(() => Ref.get(collected.store)),
      );
      const started = events.filter((e) => e.type === "task.started");
      const completed = events.filter((e) => e.type === "task.completed");
      // both children eventually start + complete...
      expect(started).toHaveLength(2);
      expect(completed).toHaveLength(2);

      const idx = (type: string, taskId: string) =>
        events.findIndex(
          (e) =>
            (e.type === "task.started" || e.type === "task.completed") &&
            e.type === type &&
            e.payload.taskId === taskId,
        );
      const child0Completed = idx("task.completed", "pi-subagent:sa-7:0");
      const child1Started = idx("task.started", "pi-subagent:sa-7:1");
      const child1Completed = idx("task.completed", "pi-subagent:sa-7:1");
      // child 0 (observable) starts + emits progress during the update
      expect(idx("task.started", "pi-subagent:sa-7:0")).toBeGreaterThanOrEqual(0);
      // the deferred placeholder does not start until the final snapshot: if it had
      // NOT been deferred it would have started during the first (update) event,
      // i.e. before child 0 completed. Here it starts only at final completion.
      expect(child1Started).toBeGreaterThan(child0Completed);
      expect(child1Started).toBeLessThan(child1Completed);
    }),
  );
  it.effect("starts a session, streams assistant text, and completes the turn", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-basic");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );

      const session = yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      expect(session.provider).toBe("pi");
      expect(session.status).toBe("ready");
      expect(session.resumeCursor).toEqual({ sessionFile: "/tmp/pi-session.json" });

      const turn = yield* adapter.sendTurn({ threadId, input: "hello", attachments: [] });
      expect(turn.turnId).toBeDefined();
      expect(fake.commands.some((c) => c.type === "prompt")).toBe(true);

      yield* fake.pushEvent({ type: "agent_start" } as AgentSessionEvent);
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", delta: "hi" },
      } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "turn_end",
        message: {
          role: "assistant",
          usage: {
            input: 1_000,
            output: 100,
            cacheRead: 4_000,
            cacheWrite: 0,
            totalTokens: 5_100,
          },
        },
        toolResults: [],
      } as unknown as AgentSessionEvent);
      yield* fake.pushEvent({ type: "agent_end" } as AgentSessionEvent);

      const events = yield* Fiber.join(collected.fiber).pipe(
        Effect.flatMap(() => Ref.get(collected.store)),
      );
      const types = events.map((event) => event.type);
      expect(types).toContain("session.started");
      expect(types).toContain("turn.started");

      const delta = events.find((event) => event.type === "content.delta");
      expect(delta).toBeDefined();
      if (delta && delta.type === "content.delta") {
        expect(delta.payload.streamKind).toBe("assistant_text");
        expect(delta.payload.delta).toBe("hi");
        expect(delta.raw?.source).toBe("pi.rpc.event");
      }
      const usage = events.find((event) => event.type === "thread.token-usage.updated");
      expect(usage).toBeDefined();
      if (usage?.type === "thread.token-usage.updated") {
        expect(usage.payload.usage).toMatchObject({
          usedTokens: 5_100,
          maxTokens: 200_000,
          inputTokens: 1_000,
          cachedInputTokens: 4_000,
          outputTokens: 100,
          compactsAutomatically: true,
        });
        expect(usage.raw?.source).toBe("pi.rpc.event");
      }
      const completed = events.find((event) => event.type === "turn.completed");
      if (completed && completed.type === "turn.completed") {
        expect(completed.payload.state).toBe("completed");
      }

      yield* adapter.stopSession(threadId);
      expect(yield* adapter.hasSession(threadId)).toBe(false);
    }),
  );

  it.effect("completes each assistant response at message_end", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-message-boundaries");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );

      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "inspect then answer", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", delta: "I'll inspect first." },
      } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "message_end",
        message: { role: "assistant" },
      } as unknown as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_start",
        toolCallId: "read-1",
        toolName: "read",
        args: { path: "src/app.ts" },
      } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_end",
        toolCallId: "read-1",
        toolName: "read",
        result: "contents",
        isError: false,
      } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", delta: "Inspection complete." },
      } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "message_end",
        message: { role: "assistant" },
      } as unknown as AgentSessionEvent);
      yield* fake.pushEvent({ type: "agent_end" } as AgentSessionEvent);

      const events = yield* Fiber.join(collected.fiber).pipe(
        Effect.flatMap(() => Ref.get(collected.store)),
      );
      const assistantDeltas = events.filter(
        (event) => event.type === "content.delta" && event.payload.streamKind === "assistant_text",
      );
      const assistantCompletions = events.filter(
        (event) =>
          event.type === "item.completed" && event.payload.itemType === "assistant_message",
      );

      expect(assistantDeltas).toHaveLength(2);
      expect(assistantCompletions).toHaveLength(2);
      expect(assistantDeltas[0]?.itemId).toBeDefined();
      expect(assistantDeltas[1]?.itemId).toBeDefined();
      expect(assistantDeltas[0]?.itemId).not.toBe(assistantDeltas[1]?.itemId);
      expect(assistantCompletions.map((event) => event.itemId)).toEqual(
        assistantDeltas.map((event) => event.itemId),
      );

      const firstCompletionIndex = events.indexOf(assistantCompletions[0]!);
      const toolStartIndex = events.findIndex((event) => event.type === "item.started");
      const secondDeltaIndex = events.indexOf(assistantDeltas[1]!);
      expect(firstCompletionIndex).toBeLessThan(toolStartIndex);
      expect(toolStartIndex).toBeLessThan(secondDeltaIndex);

      // Expanded work-log rows read output from `data.item.result`.
      const toolCompletion = events.find(
        (event) =>
          event.type === "item.completed" && event.payload.itemType === "dynamic_tool_call",
      );
      expect(toolCompletion?.payload).toMatchObject({
        data: { item: { toolName: "read", input: { path: "src/app.ts" }, result: "contents" } },
      });
    }),
  );

  it.effect("maps thinking_delta to a reasoning_text content delta", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-reasoning");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "think", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "message_update",
        assistantMessageEvent: { type: "thinking_delta", delta: "why" },
      } as AgentSessionEvent);
      yield* fake.pushEvent({ type: "agent_end" } as AgentSessionEvent);

      const events = yield* Fiber.join(collected.fiber).pipe(
        Effect.flatMap(() => Ref.get(collected.store)),
      );
      const reasoning = events.find(
        (event) => event.type === "content.delta" && event.payload.streamKind === "reasoning_text",
      );
      expect(reasoning).toBeDefined();
    }),
  );

  it.effect(
    "does not finalize the turn on agent_end willRetry; completes on the terminal end",
    () =>
      Effect.gen(function* () {
        const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
        const threadId = ThreadId.make("pi-int-retry");
        const collected = yield* collectEvents(
          adapter,
          threadId,
          (event) => event.type === "turn.completed",
        );
        yield* adapter.startSession({
          threadId,
          provider: PI,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        yield* adapter.sendTurn({ threadId, input: "retry please", attachments: [] });
        yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
        yield* fake.pushEvent({
          type: "agent_end",
          messages: [],
          willRetry: true,
        } as AgentSessionEvent);
        yield* fake.pushEvent({
          type: "agent_end",
          messages: [],
          willRetry: false,
        } as AgentSessionEvent);

        const events = yield* Fiber.join(collected.fiber).pipe(
          Effect.flatMap(() => Ref.get(collected.store)),
        );
        const completions = events.filter((event) => event.type === "turn.completed");
        expect(completions).toHaveLength(1);
        const completed = completions[0];
        if (completed && completed.type === "turn.completed") {
          expect(completed.payload.state).toBe("completed");
        }

        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect("maps a tool execution lifecycle to item events", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-tool");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "run ls", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_start",
        toolCallId: "t1",
        toolName: "bash",
        args: { command: "ls" },
      } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_end",
        toolCallId: "t1",
        toolName: "bash",
        result: "file.txt",
        isError: false,
      } as AgentSessionEvent);
      yield* fake.pushEvent({ type: "agent_end" } as AgentSessionEvent);

      const events = yield* Fiber.join(collected.fiber).pipe(
        Effect.flatMap(() => Ref.get(collected.store)),
      );
      const started = events.find((event) => event.type === "item.started");
      const completed = events.find((event) => event.type === "item.completed");
      expect(started).toBeDefined();
      expect(completed).toBeDefined();
      if (started && started.type === "item.started") {
        expect(started.payload.itemType).toBe("command_execution");
        expect(started.payload.status).toBe("inProgress");
        expect(started.payload.data).toEqual({
          item: { toolName: "bash", input: { command: "ls" } },
        });
      }
      if (completed && completed.type === "item.completed") {
        expect(completed.payload.data).toEqual({
          item: { toolName: "bash", input: { command: "ls" }, result: "file.txt" },
        });
      }
    }),
  );

  it.effect("maps think calls to per-call reasoning deltas, not tool rows", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-think");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "reason it out", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_start",
        toolCallId: "think-1",
        toolName: "think",
        args: { thoughts: "First consider the merge base." },
      } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_end",
        toolCallId: "think-1",
        toolName: "think",
        result: "ok",
        isError: false,
      } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_start",
        toolCallId: "think-2",
        toolName: "think",
        args: { thoughts: "Now verify the conflict list." },
      } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_end",
        toolCallId: "think-2",
        toolName: "think",
        result: "ok",
        isError: false,
      } as AgentSessionEvent);
      yield* fake.pushEvent({ type: "agent_end" } as AgentSessionEvent);

      const events = yield* Fiber.join(collected.fiber).pipe(
        Effect.flatMap(() => Ref.get(collected.store)),
      );

      // No tool rows: think is reflection, not an action.
      expect(events.filter((event) => event.type === "item.started")).toHaveLength(0);
      expect(events.filter((event) => event.type === "item.completed")).toHaveLength(0);

      const reasoning = events.filter(
        (event) => event.type === "content.delta" && event.payload.streamKind === "reasoning_text",
      );
      expect(
        reasoning.map((event) => event.type === "content.delta" && event.payload.delta),
      ).toEqual(["First consider the merge base.", "Now verify the conflict list."]);
      // Distinct itemIds keep each reflection its own row; a shared key would
      // coalesce them into one growing blob in ingestion.
      expect(new Set(reasoning.map((event) => event.itemId)).size).toBe(2);
    }),
  );

  it.effect("bridges a confirm request to an approval round-trip", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-approval");
      const store = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const opened = yield* Deferred.make<ApprovalRequestId>();
      const resolved = yield* Deferred.make<void>();
      const fiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            yield* Ref.update(store, (events) => [...events, event]);
            if (event.type === "request.opened" && event.requestId !== undefined) {
              yield* Deferred.succeed(opened, ApprovalRequestId.make(String(event.requestId))).pipe(
                Effect.ignore,
              );
            }
            if (event.type === "request.resolved") {
              yield* Deferred.succeed(resolved, undefined).pipe(Effect.ignore);
            }
          }),
        ),
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "edit file", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushExtensionUI({
        type: "extension_ui_request",
        id: "ui-1",
        method: "confirm",
        title: "bash",
        message: "ls -la",
      } as RpcExtensionUIRequest);

      const requestId = yield* Deferred.await(opened);
      yield* adapter.respondToRequest(threadId, requestId, "accept");
      yield* Deferred.await(resolved);
      yield* Fiber.interrupt(fiber);

      const events = yield* Ref.get(store);
      const requestOpened = events.find((event) => event.type === "request.opened");
      expect(requestOpened).toBeDefined();
      if (requestOpened && requestOpened.type === "request.opened") {
        expect(requestOpened.raw?.source).toBe("pi.rpc.extension-ui");
      }
      expect(fake.extensionResponses).toContainEqual({
        type: "extension_ui_response",
        id: "ui-1",
        confirmed: true,
      });
    }),
  );

  it.effect("bridges a select request to a user-input round-trip", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-userinput");
      const store = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const opened = yield* Deferred.make<ApprovalRequestId>();
      const resolved = yield* Deferred.make<void>();
      const fiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            yield* Ref.update(store, (events) => [...events, event]);
            if (event.type === "user-input.requested" && event.requestId !== undefined) {
              yield* Deferred.succeed(opened, ApprovalRequestId.make(String(event.requestId))).pipe(
                Effect.ignore,
              );
            }
            if (event.type === "user-input.resolved") {
              yield* Deferred.succeed(resolved, undefined).pipe(Effect.ignore);
            }
          }),
        ),
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "pick one", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushExtensionUI({
        type: "extension_ui_request",
        id: "ui-2",
        method: "select",
        title: "Choose an option",
        options: ["Option A", "Option B"],
      } as RpcExtensionUIRequest);

      const requestId = yield* Deferred.await(opened);
      const events0 = yield* Ref.get(store);
      const requested = events0.find((event) => event.type === "user-input.requested");
      expect(requested).toBeDefined();
      if (requested && requested.type === "user-input.requested") {
        const questionId = requested.payload.questions[0]?.id;
        expect(questionId).toBeDefined();
        yield* adapter.respondToUserInput(threadId, requestId, {
          [String(questionId)]: "Option A",
        });
      }
      yield* Deferred.await(resolved);
      yield* Fiber.interrupt(fiber);

      expect(
        fake.extensionResponses.some(
          (response) => "value" in response && response.value === "Option A",
        ),
      ).toBe(true);
    }),
  );

  it.effect("fails closed when the approval gate does not load", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      fake.setResponse(
        "get_commands",
        asResponse({
          type: "response",
          id: "x",
          command: "get_commands",
          success: true,
          data: { commands: [] },
        }),
      );
      const threadId = ThreadId.make("pi-int-failclosed");
      const result = yield* adapter
        .startSession({
          threadId,
          provider: PI,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        })
        .pipe(Effect.result);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(String(result.failure.message)).toMatch(/approval gate|ungated/i);
      }
    }),
  );

  it.effect("rejects startSession when the provider does not match", () =>
    Effect.gen(function* () {
      const { adapter } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-mismatch");
      const result = yield* adapter
        .startSession({
          threadId,
          provider: ProviderDriverKind.make("codex"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        })
        .pipe(Effect.result);
      expect(Result.isFailure(result)).toBe(true);
    }),
  );

  it.effect("steers a running turn instead of opening a second turn", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-steer");
      const store = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const fiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.runForEach((event) => Ref.update(store, (events) => [...events, event])),
        Effect.forkChild,
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const first = yield* adapter.sendTurn({ threadId, input: "first", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      const second = yield* adapter.sendTurn({ threadId, input: "steer me", attachments: [] });
      expect(second.turnId).toBe(first.turnId);

      yield* fake.pushEvent({ type: "agent_end" } as AgentSessionEvent);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(fiber);

      const events = yield* Ref.get(store);
      const turnStarts = events.filter((event) => event.type === "turn.started");
      expect(turnStarts.length).toBe(1);
      expect(fake.commands.some((command) => command.type === "steer")).toBe(true);
    }),
  );

  const subagentResult = (overrides: Record<string, unknown> = {}) => ({
    agent: "worker",
    agentSource: "user",
    task: "do the thing",
    exitCode: 0,
    messages: [{ role: "assistant", content: [{ type: "text", text: "done" }] }],
    stderr: "",
    usage: {
      input: 100,
      output: 20,
      cacheRead: 0,
      cacheWrite: 0,
      cost: 0,
      contextTokens: 120,
      turns: 1,
    },
    ...overrides,
  });

  const subagentUpdate = (
    toolCallId: string,
    mode: "single" | "parallel" | "chain",
    results: ReadonlyArray<Record<string, unknown>>,
  ) =>
    ({
      type: "tool_execution_update",
      toolCallId,
      toolName: "subagent",
      args: {},
      partialResult: {
        content: [{ type: "text", text: "(running...)" }],
        details: { mode, agentScope: "user", projectAgentsDir: null, results },
      },
    }) as unknown as AgentSessionEvent;

  const subagentEnd = (
    toolCallId: string,
    mode: "single" | "parallel" | "chain",
    results: ReadonlyArray<Record<string, unknown>>,
    isError = false,
  ) =>
    ({
      type: "tool_execution_end",
      toolCallId,
      toolName: "subagent",
      isError,
      result: {
        content: [{ type: "text", text: "final" }],
        details: { mode, agentScope: "user", projectAgentsDir: null, results },
      },
    }) as unknown as AgentSessionEvent;

  it.effect("maps a single subagent run to task.started/progress/completed", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-subagent-single");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "delegate", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_start",
        toolCallId: "sa-1",
        toolName: "subagent",
        args: { agent: "worker", task: "do the thing" },
      } as AgentSessionEvent);
      // streaming update: partial assistant text
      yield* fake.pushEvent(
        subagentUpdate("sa-1", "single", [
          subagentResult({
            messages: [{ role: "assistant", content: [{ type: "text", text: "working" }] }],
          }),
        ]),
      );
      // duplicate cumulative update: no new progress
      yield* fake.pushEvent(
        subagentUpdate("sa-1", "single", [
          subagentResult({
            messages: [{ role: "assistant", content: [{ type: "text", text: "working" }] }],
          }),
        ]),
      );
      yield* fake.pushEvent(subagentEnd("sa-1", "single", [subagentResult()]));
      yield* fake.pushEvent({
        type: "agent_end",
        messages: [],
        willRetry: false,
      } as AgentSessionEvent);

      const events = yield* Fiber.join(collected.fiber).pipe(
        Effect.flatMap(() => Ref.get(collected.store)),
      );
      const started = events.filter((e) => e.type === "task.started");
      const progress = events.filter((e) => e.type === "task.progress");
      const completed = events.filter((e) => e.type === "task.completed");
      expect(started).toHaveLength(1);
      // one progress from "working"; the duplicate snapshot is deduped
      expect(progress).toHaveLength(1);
      expect(completed).toHaveLength(1);
      if (started[0]?.type === "task.started") {
        expect(started[0].payload.taskId).toBe("pi-subagent:sa-1:0");
        expect(started[0].payload).toMatchObject({
          taskType: "subagent",
          title: "do the thing",
          role: "worker",
          toolUseId: "sa-1",
        });
      }
      if (completed[0]?.type === "task.completed") {
        expect(completed[0].payload.status).toBe("completed");
      }

      // task.completed must precede the parent item.completed
      const parentCompletedIndex = events.findIndex(
        (e) => e.type === "item.completed" && e.itemId === "sa-1",
      );
      const taskCompletedIndex = events.findIndex((e) => e.type === "task.completed");
      expect(taskCompletedIndex).toBeGreaterThanOrEqual(0);
      expect(taskCompletedIndex).toBeLessThan(parentCompletedIndex);
    }),
  );

  it.effect("maps parallel subagent runs to stable per-child task ids and statuses", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-subagent-parallel");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "delegate two", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_start",
        toolCallId: "sa-2",
        toolName: "subagent",
        args: {},
      } as AgentSessionEvent);
      // first update: both placeholders running
      yield* fake.pushEvent(
        subagentUpdate("sa-2", "parallel", [
          { agent: "a", task: "task a", exitCode: -1, messages: [], usage: {} },
          { agent: "b", task: "task b", exitCode: -1, messages: [], usage: {} },
        ]),
      );
      yield* fake.pushEvent(
        subagentEnd("sa-2", "parallel", [
          subagentResult({ agent: "a", task: "task a", exitCode: 0 }),
          subagentResult({ agent: "b", task: "task b", exitCode: 1, stopReason: "error" }),
        ]),
      );
      yield* fake.pushEvent({
        type: "agent_end",
        messages: [],
        willRetry: false,
      } as AgentSessionEvent);

      const events = yield* Fiber.join(collected.fiber).pipe(
        Effect.flatMap(() => Ref.get(collected.store)),
      );
      const started = events.filter((e) => e.type === "task.started");
      const completed = events.filter((e) => e.type === "task.completed");
      expect(started).toHaveLength(2);
      expect(completed).toHaveLength(2);
      const startedIds = started.flatMap((e) =>
        e.type === "task.started" ? [e.payload.taskId] : [],
      );
      expect(startedIds).toEqual(["pi-subagent:sa-2:0", "pi-subagent:sa-2:1"]);
      const statuses = completed.flatMap((e) =>
        e.type === "task.completed" ? [e.payload.status] : [],
      );
      expect(statuses).toEqual(["completed", "failed"]);
    }),
  );

  it.effect("maps chain subagent steps and an aborted child to stopped", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-subagent-chain");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "chain", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_start",
        toolCallId: "sa-3",
        toolName: "subagent",
        args: {},
      } as AgentSessionEvent);
      yield* fake.pushEvent(
        subagentUpdate("sa-3", "chain", [
          subagentResult({ agent: "scout", task: "scout", step: 1 }),
        ]),
      );
      yield* fake.pushEvent(
        subagentEnd(
          "sa-3",
          "chain",
          [
            subagentResult({ agent: "scout", task: "scout", step: 1, exitCode: 0 }),
            subagentResult({
              agent: "worker",
              task: "impl",
              step: 2,
              exitCode: 1,
              stopReason: "aborted",
            }),
          ],
          true,
        ),
      );
      yield* fake.pushEvent({
        type: "agent_end",
        messages: [],
        willRetry: false,
      } as AgentSessionEvent);

      const events = yield* Fiber.join(collected.fiber).pipe(
        Effect.flatMap(() => Ref.get(collected.store)),
      );
      const completed = events.filter((e) => e.type === "task.completed");
      expect(completed).toHaveLength(2);
      const byId = new Map(
        completed.flatMap((e) =>
          e.type === "task.completed"
            ? [[String(e.payload.taskId), e.payload.status] as const]
            : [],
        ),
      );
      expect(byId.get("pi-subagent:sa-3:0")).toBe("completed");
      expect(byId.get("pi-subagent:sa-3:1")).toBe("stopped");
    }),
  );

  it.effect("ignores malformed subagent payloads (no task events)", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-subagent-malformed");
      const collected = yield* collectEvents(
        adapter,
        threadId,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "delegate", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_start",
        toolCallId: "sa-4",
        toolName: "subagent",
        args: {},
      } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_update",
        toolCallId: "sa-4",
        toolName: "subagent",
        args: {},
        partialResult: { content: [{ type: "text", text: "garbled" }] },
      } as unknown as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "agent_end",
        messages: [],
        willRetry: false,
      } as AgentSessionEvent);

      const events = yield* Fiber.join(collected.fiber).pipe(
        Effect.flatMap(() => Ref.get(collected.store)),
      );
      expect(events.some((e) => e.type.startsWith("task."))).toBe(false);
      // the unrecognized payload still surfaces its text (not [object Object])
      const delta = events.find((e) => e.type === "content.delta" && e.itemId === "sa-4");
      expect(delta).toBeDefined();
      if (delta?.type === "content.delta") {
        expect(delta.payload.delta).toContain("garbled");
      }
    }),
  );

  it.effect("finalizes outstanding subagent tasks as stopped on interruption", () =>
    Effect.gen(function* () {
      const { adapter, fake } = yield* makePiAdapterForTest(enabledSettings());
      const threadId = ThreadId.make("pi-int-subagent-interrupt");
      const store = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const fiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.runForEach((event) => Ref.update(store, (events) => [...events, event])),
        Effect.forkChild,
      );
      yield* adapter.startSession({
        threadId,
        provider: PI,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "delegate", attachments: [] });
      yield* fake.pushEvent({ type: "turn_start" } as AgentSessionEvent);
      yield* fake.pushEvent({
        type: "tool_execution_start",
        toolCallId: "sa-5",
        toolName: "subagent",
        args: {},
      } as AgentSessionEvent);
      yield* fake.pushEvent(
        subagentUpdate("sa-5", "single", [
          subagentResult({
            messages: [{ role: "assistant", content: [{ type: "text", text: "in progress" }] }],
          }),
        ]),
      );
      yield* Effect.yieldNow;
      yield* adapter.interruptTurn(threadId);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(fiber);

      const events = yield* Ref.get(store);
      const started = events.filter((e) => e.type === "task.started");
      const completed = events.filter((e) => e.type === "task.completed");
      expect(started).toHaveLength(1);
      expect(completed).toHaveLength(1);
      if (completed[0]?.type === "task.completed") {
        expect(completed[0].payload.status).toBe("stopped");
        expect(completed[0].payload.taskId).toBe("pi-subagent:sa-5:0");
      }
    }),
  );
});
