/** `ProviderAdapterShape` for the Pi coding agent (per-thread `pi --mode rpc` sessions). */
import * as NodeURL from "node:url";

import {
  ApprovalRequestId,
  type CanonicalItemType,
  type CanonicalRequestType,
  EventId,
  type ModelSelection,
  type PiSettings,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  RuntimeRequestId,
  RuntimeTaskId,
  type RuntimeTaskUsage,
  ThreadId,
  type ThreadTokenUsageSnapshot,
  TurnId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerConfig } from "../../config.ts";
import { resolveAttachmentPath } from "../../attachmentStore.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import {
  piBackgroundCompletion,
  piBackgroundToolJobs,
  type PiBackgroundJob,
} from "./PiBackgroundTasks.ts";
import {
  normalizePiSubagentResult,
  subagentChildCompletionSummary,
  subagentChildFingerprint,
  subagentChildHasActivity,
  subagentChildProgressDescription,
  subagentChildTaskId,
  subagentChildTerminalStatus,
  type NormalizedSubagentSnapshot,
  type NormalizedSubagentUsage,
} from "./PiSubagentSnapshot.ts";
import {
  type AgentSessionEvent,
  buildPiTurnCommand,
  extractAssistantTextDelta,
  extractForkMessages,
  extractPiContextConfig,
  extractReasoningTextDelta,
  extractSessionFile,
  makePiRpcTransport,
  type MakePiRpcTransportOptions,
  piForkSucceeded,
  piImageContentFromBytes,
  type PiImageContent,
  piResponseHasCommand,
  piResponseSucceeded,
  planPiModelSwitch,
  resolveForkTargetEntryId,
  resolvePiThinkingLevel,
  type PiRpcTransport,
  type PiStdoutMessage,
  type PiThinkingLevel,
  type RpcExtensionUIRequest,
  type RpcExtensionUIResponse,
} from "./PiRpcClient.ts";

const PROVIDER = ProviderDriverKind.make("pi");

const PI_STATE_TIMEOUT_MS = 5_000;
const PI_COMMANDS_TIMEOUT_MS = 5_000;
const PI_MESSAGES_TIMEOUT_MS = 5_000;
// fork/new_session rebinds to a new session file — give it more headroom
const PI_FORK_TIMEOUT_MS = 15_000;
const PI_MODEL_OPTIONS_TIMEOUT_MS = 5_000;

// keep in sync with SENTINEL_COMMAND in t3-approvals.ts
const PI_APPROVAL_SENTINEL_COMMAND = "t3-approval-gate";

// like Claude/Cursor: full-access runs ungated; approval-required and
// auto-accept-edits gate via the bundled extension (Pi has no native per-tool approval).
// `auto` has no AI reviewer here either, so it gates with auto-accept-edits semantics
// rather than falling through to ungated execution.
function approvalGateForRuntimeMode(
  runtimeMode: ProviderSession["runtimeMode"],
): { readonly gate: false } | { readonly gate: true; readonly mode: string } {
  if (runtimeMode === "approval-required" || runtimeMode === "auto-accept-edits") {
    return { gate: true, mode: runtimeMode };
  }
  if (runtimeMode === "auto") {
    return { gate: true, mode: "auto-accept-edits" };
  }
  return { gate: false };
}

// dev resolves ../assets (running from src); the vp-pack build copies the asset
// next to the bundle, so prod resolves ./assets
const APPROVAL_EXTENSION_CANDIDATES: ReadonlyArray<string> = (() => {
  const resolve = (relative: string): string | undefined => {
    try {
      return NodeURL.fileURLToPath(new URL(relative, import.meta.url));
    } catch {
      return undefined;
    }
  };
  return [resolve("../assets/pi/t3-approvals.ts"), resolve("./assets/pi/t3-approvals.ts")].filter(
    (value): value is string => value !== undefined,
  );
})();

interface PiToolItem {
  readonly id: RuntimeItemId;
  readonly type: CanonicalItemType;
  readonly toolName: string;
  args: unknown;
}

// Per child subagent task tracked against its parent `subagent` tool call.
interface SubagentChildTaskState {
  readonly taskId: RuntimeTaskId;
  readonly title: string;
  readonly role: string;
  readonly toolUseId: string;
  started: boolean;
  completed: boolean;
  // compact fingerprint of the last emitted snapshot (no transcript retained)
  progressFingerprint: string | undefined;
}

interface BackgroundTaskState {
  readonly taskId: RuntimeTaskId;
  readonly title: string;
  readonly turnId: TurnId | undefined;
  readonly toolUseId: string | undefined;
  completed: boolean;
}

interface PiTurnState {
  readonly turnId: TurnId;
  readonly startedAt: string;
  readonly items: Array<PiToolItem>;
  activeAssistantItemId: RuntimeItemId | undefined;
  activeAssistantHasText: boolean;
}

interface PendingApproval {
  readonly piId: string;
  readonly requestType: CanonicalRequestType;
  readonly sessionApprovalKey: string;
}

interface NumberedOption {
  readonly index: number;
  readonly label: string;
}

type PendingNumberedOption = string | NumberedOption;

interface PendingUserInput {
  readonly piId: string;
  readonly questionId: string;
  readonly method: "select" | "input" | "editor";
  readonly numberedOptions?: ReadonlyArray<PendingNumberedOption>;
}

interface PiSessionContext {
  session: ProviderSession;
  readonly sessionScope: Scope.Closeable;
  readonly transport: PiRpcTransport;
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly pendingUserInputs: Map<ApprovalRequestId, PendingUserInput>;
  readonly sessionApprovals: Set<string>;
  turnState: PiTurnState | undefined;
  readonly turns: Array<{ id: TurnId; items: Array<PiToolItem> }>;
  // parent `subagent` toolCallId -> child index -> child task state
  readonly subagentTasks: Map<string, Map<number, SubagentChildTaskState>>;
  // bg jobs outlive turns; retain settled identities to dedupe bg_status snapshots.
  readonly backgroundTasks: Map<number, BackgroundTaskState>;
  readonly backgroundLaunches: Set<string>;
  // `think` toolCallIds routed to reasoning; their update/end events emit no tool row
  readonly thinkItemIds: Set<string>;
  stopped: boolean;
  // slug the pi process is running; used to issue set_model only on change
  currentModel: string | undefined;
  currentContextWindow: number | undefined;
  compactsAutomatically: boolean | undefined;
  appliedThinkingLevel: PiThinkingLevel | undefined;
}

function hasBackgroundWork(context: PiSessionContext): boolean {
  return (
    context.backgroundLaunches.size > 0 ||
    [...context.backgroundTasks.values()].some((task) => !task.completed)
  );
}

// ---------------------------------------------------------------------------
// Pure classification helpers
// ---------------------------------------------------------------------------

export function classifyPiToolItemType(toolName: string): CanonicalItemType {
  // whole-token match (split camelCase/separators) so "recommend" isn't read as "command"
  const tokens = new Set(
    toolName
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .replace(/[._/-]/g, " ")
      .toLowerCase()
      .split(/\s+/)
      .filter((token) => token.length > 0),
  );
  const has = (...words: ReadonlyArray<string>): boolean => words.some((word) => tokens.has(word));

  if (has("mcp")) return "mcp_tool_call";
  if (has("agent", "subagent", "task", "skill")) return "collab_agent_tool_call";
  if (has("bash", "shell", "command", "terminal", "exec")) return "command_execution";
  if (has("edit", "write", "patch", "apply", "file")) return "file_change";
  if (has("search", "web")) return "web_search";
  if (has("image")) return "image_view";
  return "dynamic_tool_call";
}

export function classifyPiApprovalRequestType(toolHint: string): CanonicalRequestType {
  const item = classifyPiToolItemType(toolHint);
  switch (item) {
    case "command_execution":
      return "command_execution_approval";
    case "file_change":
      return "file_change_approval";
    default:
      // a Pi confirm is a binary gate, not structured input; tool_user_input
      // would be dropped by the runtime-ingestion + web approval pipeline
      return "dynamic_tool_call";
  }
}

function finiteNonNegativeInteger(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? Math.round(value)
    : undefined;
}

export function normalizePiTokenUsage(
  event: AgentSessionEvent,
  options: {
    readonly contextWindow?: number;
    readonly compactsAutomatically?: boolean;
  } = {},
): ThreadTokenUsageSnapshot | undefined {
  if (event.type !== "turn_end" || event.message.role !== "assistant") return undefined;

  const usage = event.message.usage;
  const inputTokens = finiteNonNegativeInteger(usage.input) ?? 0;
  const outputTokens = finiteNonNegativeInteger(usage.output) ?? 0;
  const cacheReadTokens = finiteNonNegativeInteger(usage.cacheRead) ?? 0;
  const cacheWriteTokens = finiteNonNegativeInteger(usage.cacheWrite) ?? 0;
  const cachedInputTokens = cacheReadTokens + cacheWriteTokens;
  const reportedTotal = finiteNonNegativeInteger(usage.totalTokens) ?? 0;
  // Mirrors Pi's calculateContextTokens: prefer the provider total, then sum
  // the components for providers that leave totalTokens at zero.
  const usedTokens =
    reportedTotal > 0
      ? reportedTotal
      : inputTokens + outputTokens + cacheReadTokens + cacheWriteTokens;
  if (usedTokens <= 0) return undefined;

  const reasoningOutputTokens = finiteNonNegativeInteger(usage.reasoning);
  return {
    usedTokens,
    lastUsedTokens: usedTokens,
    ...(options.contextWindow !== undefined ? { maxTokens: options.contextWindow } : {}),
    ...(inputTokens > 0 ? { inputTokens, lastInputTokens: inputTokens } : {}),
    ...(cachedInputTokens > 0
      ? { cachedInputTokens, lastCachedInputTokens: cachedInputTokens }
      : {}),
    ...(outputTokens > 0 ? { outputTokens, lastOutputTokens: outputTokens } : {}),
    ...(reasoningOutputTokens !== undefined
      ? { reasoningOutputTokens, lastReasoningOutputTokens: reasoningOutputTokens }
      : {}),
    ...(options.compactsAutomatically !== undefined
      ? { compactsAutomatically: options.compactsAutomatically }
      : {}),
  };
}

// Extract human-readable text from a tool `partialResult`, which may be a raw
// string or a structured `AgentToolResult` ({ content: [{ type: "text", text }] }).
// Falls back to compact JSON so structured payloads never render as "[object Object]".
function normalizePiSubagentUsage(
  usage: NormalizedSubagentUsage | undefined,
): RuntimeTaskUsage | undefined {
  if (!usage) return undefined;
  const inputTokens = finiteNonNegativeInteger(usage.input);
  const outputTokens = finiteNonNegativeInteger(usage.output);
  const cacheReadTokens = finiteNonNegativeInteger(usage.cacheRead);
  const cacheWriteTokens = finiteNonNegativeInteger(usage.cacheWrite);
  const cachedInputTokens = (cacheReadTokens ?? 0) + (cacheWriteTokens ?? 0);
  const totalTokens =
    (inputTokens ?? 0) + (outputTokens ?? 0) + (cacheReadTokens ?? 0) + (cacheWriteTokens ?? 0);
  if (totalTokens <= 0) return undefined;
  return {
    totalTokens,
    ...(inputTokens !== undefined ? { inputTokens } : {}),
    ...(cachedInputTokens > 0 ? { cachedInputTokens } : {}),
    ...(outputTokens !== undefined ? { outputTokens } : {}),
  };
}

/**
 * Pi's bundled `think` tool is reflection, not an action: it has no output to
 * expand and cannot fail. Rendering it as a tool row buries the prose behind a
 * generic label, so it maps to the canonical reasoning stream instead.
 *
 * Keyed on the tool call id (never the assistant item) so each call renders as
 * its own row — ingestion coalesces deltas per item, and a shared key would
 * merge every reflection in the turn into one growing blob.
 */
export function extractPiThinkText(toolName: string, args: unknown): string | undefined {
  if (toolName !== "think") return undefined;
  if (!args || typeof args !== "object") return undefined;
  const input = args as Record<string, unknown>;
  for (const key of ["thoughts", "thought"]) {
    const value = input[key];
    if (typeof value === "string" && value.trim().length > 0) return value.trim();
  }
  return undefined;
}

export function extractPiPartialResultText(partial: unknown): string | undefined {
  if (partial === undefined || partial === null) return undefined;
  if (typeof partial === "string") return partial;
  if (typeof partial === "number" || typeof partial === "boolean") return String(partial);
  if (Array.isArray(partial)) {
    const joined = partial
      .map((entry) => extractPiPartialResultText(entry))
      .filter((value): value is string => value !== undefined && value.length > 0)
      .join("");
    return joined.length > 0 ? joined : undefined;
  }
  if (typeof partial !== "object") return undefined;
  const record = partial as Record<string, unknown>;
  const content = record["content"];
  if (Array.isArray(content)) {
    let text = "";
    for (const rawPart of content) {
      if (rawPart && typeof rawPart === "object") {
        const part = rawPart as Record<string, unknown>;
        if (part["type"] === "text" && typeof part["text"] === "string") {
          text += part["text"];
        }
      }
    }
    if (text.length > 0) return text;
  }
  if (typeof record["text"] === "string" && record["text"].length > 0) return record["text"];
  if (typeof record["output"] === "string" && record["output"].length > 0) return record["output"];
  try {
    const serialized = JSON.stringify(partial);
    return serialized && serialized !== "{}" ? serialized : undefined;
  } catch {
    return undefined;
  }
}

/**
 * The work-log row renders this preview *instead of* the tool name, so a
 * summary has to read as a human-legible phrase on its own. Keys are probed
 * from most to least specific; anything we cannot describe returns undefined
 * so the row falls back to the tool name rather than dumping raw JSON.
 */
export function summarizePiToolArgs(args: unknown): string | undefined {
  if (!args || typeof args !== "object") return undefined;
  const input = args as Record<string, unknown>;
  const firstString = (...keys: ReadonlyArray<string>): string | undefined => {
    for (const key of keys) {
      const value = input[key];
      if (typeof value === "string" && value.trim().length > 0) return value.trim().slice(0, 400);
    }
    return undefined;
  };
  return (
    firstString("command", "cmd") ??
    firstString("file_path", "path", "filePath") ??
    // `think` and note-style tools carry their whole payload in a prose field
    firstString("thoughts", "thought", "prompt", "task", "text", "content") ??
    firstString("pattern", "query", "description", "url")
  );
}

// Pi encodes an RPC multi-select as `Title\n1. A\n2. B`
export function parseNumberedList(
  text: string,
): { title: string; items: ReadonlyArray<NumberedOption> } | null {
  const lines = text.split("\n");
  const items: NumberedOption[] = [];
  for (const line of lines.slice(1)) {
    const match = /^(\d+)\.\s+(.+)$/.exec(line.trim());
    if (match?.[1] && match[2]) items.push({ index: Number(match[1]), label: match[2] });
  }
  return items.length >= 2 ? { title: lines[0] ?? text, items } : null;
}

export function isPiApprovalConfirmed(decision: ProviderApprovalDecision): boolean {
  return decision === "accept" || decision === "acceptForSession";
}

export function buildPiApprovalResponse(
  piId: string,
  decision: ProviderApprovalDecision,
): RpcExtensionUIResponse {
  return { type: "extension_ui_response", id: piId, confirmed: isPiApprovalConfirmed(decision) };
}

// numbered-list multi-select maps labels back to Pi's 1-based, comma-joined indices
export function buildPiUserInputResponse(
  pending: {
    readonly piId: string;
    readonly questionId: string;
    readonly method: "select" | "input" | "editor";
    readonly numberedOptions?: ReadonlyArray<PendingNumberedOption>;
  },
  answers: ProviderUserInputAnswers,
): RpcExtensionUIResponse {
  const answer = answers[pending.questionId];
  const numberedOptions = pending.numberedOptions;
  if (pending.method === "input" && numberedOptions) {
    const selected: ReadonlyArray<string> = Array.isArray(answer)
      ? answer.map(String)
      : typeof answer === "string" && answer.length > 0
        ? [answer]
        : [];
    const indices = selected
      .map((label) => {
        const optionIndex = numberedOptions.findIndex((entry) =>
          typeof entry === "string" ? entry === label : entry.label === label,
        );
        if (optionIndex < 0) return null;
        const option = numberedOptions[optionIndex];
        if (option === undefined) return null;
        return String(typeof option === "string" ? optionIndex + 1 : option.index);
      })
      .filter((value): value is string => value !== null);
    return { type: "extension_ui_response", id: pending.piId, value: indices.join(",") };
  }
  const value = typeof answer === "string" ? answer : "";
  return { type: "extension_ui_response", id: pending.piId, value };
}

function toMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error && cause.message.length > 0) return cause.message;
  return fallback;
}

function readPiResumeState(resumeCursor: unknown): { sessionFile: string } | undefined {
  if (!resumeCursor || typeof resumeCursor !== "object") return undefined;
  const cursor = resumeCursor as Record<string, unknown>;
  return typeof cursor["sessionFile"] === "string" && cursor["sessionFile"].trim().length > 0
    ? { sessionFile: cursor["sessionFile"].trim() }
    : undefined;
}

export interface PiAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  // transport override for tests; defaults to the real `pi --mode rpc` spawn
  readonly makeTransport?: (
    options: MakePiRpcTransportOptions,
  ) => Effect.Effect<
    PiRpcTransport,
    PlatformError.PlatformError,
    Scope.Scope | ChildProcessSpawner.ChildProcessSpawner
  >;
}

export const makePiAdapter = Effect.fn("makePiAdapter")(function* (
  piSettings: PiSettings,
  options?: PiAdapterLiveOptions,
) {
  const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
  const serverConfig = yield* ServerConfig;
  const crypto = yield* Crypto.Crypto;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const baseEnvironment = options?.environment ?? process.env;

  let approvalExtensionPath: string | undefined;
  for (const candidate of APPROVAL_EXTENSION_CANDIDATES) {
    const exists = yield* fileSystem.exists(candidate).pipe(Effect.orElseSucceed(() => false));
    if (exists) {
      approvalExtensionPath = candidate;
      break;
    }
  }
  const approvalExtensionAvailable = approvalExtensionPath !== undefined;

  const sessions = new Map<ThreadId, PiSessionContext>();
  const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const nextUuid = crypto.randomUUIDv4.pipe(Effect.orDie);
  const nextEventId = Effect.map(nextUuid, (id) => EventId.make(id));
  const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

  const offerRuntimeEvent = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
    Queue.offer(runtimeEventQueue, event).pipe(Effect.asVoid);

  const makeTurnState = (turnId: TurnId, startedAt: string): PiTurnState => ({
    turnId,
    startedAt,
    items: [],
    activeAssistantItemId: undefined,
    activeAssistantHasText: false,
  });

  const rawEvent = (
    source: "pi.rpc.event" | "pi.rpc.extension-ui",
    method: string,
    payload: unknown,
  ) => ({ raw: { source, method, payload } }) as const;

  // Emit canonical task.* events for the child agents of a `subagent` tool call.
  // On a non-final snapshot we only emit task.started / task.progress; final
  // completion status comes from the terminal tool result (`final: true`).
  // Do not attach `rawEvent`: Pi's cumulative snapshot carries full child
  // transcripts, which would otherwise be persisted once per progress update.
  const emitSubagentTaskEvents = (
    context: PiSessionContext,
    event: AgentSessionEvent & { readonly toolCallId: string; readonly toolName: string },
    snapshot: NormalizedSubagentSnapshot,
    final: boolean,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const turnState = context.turnState;
      if (!turnState) return;
      const toolCallId = event.toolCallId;
      let parent = context.subagentTasks.get(toolCallId);
      if (!parent) {
        parent = new Map();
        context.subagentTasks.set(toolCallId, parent);
      }

      for (const child of snapshot.children) {
        let state = parent.get(child.index);
        if (!state) {
          state = {
            taskId: RuntimeTaskId.make(subagentChildTaskId(toolCallId, child)),
            title: child.task.trim() || child.agent,
            role: child.agent,
            toolUseId: toolCallId,
            started: false,
            completed: false,
            progressFingerprint: undefined,
          };
          parent.set(child.index, state);
        }

        if (!state.started) {
          // Defer task.started for a not-yet-dispatched queued placeholder
          // (parallel `exitCode === -1`, empty transcript, no usage). Start on
          // its first observable activity, or immediately before completion.
          if (!final && !subagentChildHasActivity(child)) continue;
          state.started = true;
          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            ...stamp,
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: context.session.threadId,
            turnId: turnState.turnId,
            type: "task.started",
            payload: {
              taskId: state.taskId,
              description: state.title,
              taskType: "subagent",
              title: state.title,
              role: state.role,
              toolUseId: state.toolUseId,
            },
          });
        }

        if (final) {
          if (state.completed) continue;
          state.completed = true;
          const summary = subagentChildCompletionSummary(child);
          const typedUsage = normalizePiSubagentUsage(child.usage);
          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            ...stamp,
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: context.session.threadId,
            turnId: turnState.turnId,
            type: "task.completed",
            payload: {
              taskId: state.taskId,
              status: subagentChildTerminalStatus(child),
              ...(summary ? { summary } : {}),
              ...(child.usage ? { usage: child.usage } : {}),
              ...(typedUsage ? { typedUsage } : {}),
              taskType: "subagent",
              title: state.title,
              role: state.role,
              toolUseId: state.toolUseId,
            },
          });
          continue;
        }

        const fingerprint = subagentChildFingerprint(child);
        if (fingerprint === state.progressFingerprint) continue;
        state.progressFingerprint = fingerprint;
        const summary = subagentChildProgressDescription(child);
        if (!summary) continue;
        const typedUsage = normalizePiSubagentUsage(child.usage);
        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          ...stamp,
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          turnId: turnState.turnId,
          type: "task.progress",
          payload: {
            taskId: state.taskId,
            description: state.title,
            summary,
            ...(child.lastToolName ? { lastToolName: child.lastToolName } : {}),
            ...(child.usage ? { usage: child.usage } : {}),
            ...(typedUsage ? { typedUsage } : {}),
            taskType: "subagent",
            title: state.title,
            role: state.role,
            toolUseId: state.toolUseId,
          },
        });
      }

      if (final) context.subagentTasks.delete(toolCallId);
    });

  // Finalize any still-open child subagent tasks (e.g. on interruption / a
  // failed or incomplete turn) and clear the tracker.
  const finalizeSubagentTasks = (
    context: PiSessionContext,
    status: "completed" | "failed" | "stopped",
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      for (const [, parent] of context.subagentTasks) {
        for (const [, state] of parent) {
          if (state.completed) continue;
          state.completed = true;
          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            ...stamp,
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: context.session.threadId,
            ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
            type: "task.completed",
            payload: {
              taskId: state.taskId,
              status,
              taskType: "subagent",
              title: state.title,
              role: state.role,
              toolUseId: state.toolUseId,
            },
          });
        }
      }
      context.subagentTasks.clear();
    });

  const emitBackgroundTask = Effect.fn("PiAdapter.emitBackgroundTask")(function* (
    context: PiSessionContext,
    job: PiBackgroundJob,
    toolUseId?: string,
    fromStatusSnapshot = false,
  ) {
    let task = context.backgroundTasks.get(job.jobId);
    // bg_status can include completed jobs restored from an older Pi process.
    if (!task && fromStatusSnapshot && job.status !== "running") return;
    if (!task) {
      task = {
        taskId: RuntimeTaskId.make(`pi-bg:${yield* nextUuid}:${job.jobId}`),
        title: job.title,
        turnId: context.turnState?.turnId,
        toolUseId,
        completed: false,
      };
      context.backgroundTasks.set(job.jobId, task);
      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        ...stamp,
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        ...(task.turnId ? { turnId: task.turnId } : {}),
        type: "task.started",
        payload: {
          taskId: task.taskId,
          taskType: "shell",
          title: task.title,
          description: task.title,
          ...(task.toolUseId ? { toolUseId: task.toolUseId } : {}),
        },
      });
      if (job.status === "running") {
        const progressStamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          ...progressStamp,
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          ...(task.turnId ? { turnId: task.turnId } : {}),
          type: "task.progress",
          payload: {
            taskId: task.taskId,
            taskType: "shell",
            title: task.title,
            description: task.title,
            summary: `Background job #${job.jobId} running: ${task.title}`,
            status: "running",
            ...(task.toolUseId ? { toolUseId: task.toolUseId } : {}),
          },
        });
      }
    }
    if (task.completed || job.status === "running") return;
    task.completed = true;
    const stamp = yield* makeEventStamp();
    // Do not persist raw custom messages/snapshots: they contain cumulative output.
    yield* offerRuntimeEvent({
      ...stamp,
      provider: PROVIDER,
      providerInstanceId: boundInstanceId,
      threadId: context.session.threadId,
      ...(task.turnId ? { turnId: task.turnId } : {}),
      type: "task.completed",
      payload: {
        taskId: task.taskId,
        taskType: "shell",
        title: task.title,
        status: job.status,
        summary: `Background job #${job.jobId} ${job.status}: ${task.title}`,
        ...(task.toolUseId ? { toolUseId: task.toolUseId } : {}),
        ...(job.durationMs !== undefined ? { usage: { durationMs: job.durationMs } } : {}),
      },
    });
  });

  const finalizeBackgroundTasks = Effect.fn("PiAdapter.finalizeBackgroundTasks")(function* (
    context: PiSessionContext,
    status: "stopped" | "failed",
  ) {
    for (const [jobId, task] of context.backgroundTasks) {
      if (!task.completed) yield* emitBackgroundTask(context, { jobId, title: task.title, status });
    }
    context.backgroundTasks.clear();
    context.backgroundLaunches.clear();
  });

  const completeTurn = (
    context: PiSessionContext,
    state: "completed" | "failed" | "interrupted" | "cancelled",
    errorMessage?: string,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const turnState = context.turnState;
      if (!turnState) return;
      yield* finalizeSubagentTasks(
        context,
        state === "completed" ? "completed" : state === "failed" ? "failed" : "stopped",
      );
      context.turnState = undefined;
      context.thinkItemIds.clear();
      context.turns.push({ id: turnState.turnId, items: [...turnState.items] });

      const updatedAt = yield* nowIso;
      const { activeTurnId: _activeTurnId, ...readySession } = context.session;
      context.session = { ...readySession, status: "ready", updatedAt };

      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "turn.completed",
        ...stamp,
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        turnId: turnState.turnId,
        payload: {
          state,
          ...(errorMessage ? { errorMessage } : {}),
        },
      });
    });

  const openTurn = (context: PiSessionContext): Effect.Effect<TurnId> =>
    Effect.gen(function* () {
      const turnId = TurnId.make(yield* nextUuid);
      const startedAt = yield* nowIso;
      context.turnState = makeTurnState(turnId, startedAt);
      context.session = {
        ...context.session,
        status: "running",
        activeTurnId: turnId,
        updatedAt: startedAt,
      };
      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        ...stamp,
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        turnId,
        type: "turn.started",
        payload: context.currentModel ? { model: context.currentModel } : {},
      });
      return turnId;
    });

  const handlePiEvent = (
    context: PiSessionContext,
    event: AgentSessionEvent,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const stamp = yield* makeEventStamp();
      const base = {
        ...stamp,
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        ...rawEvent("pi.rpc.event", event.type, event),
      };

      switch (event.type) {
        case "agent_start": {
          yield* offerRuntimeEvent({
            ...base,
            type: "session.state.changed",
            payload: { state: "running" },
          });
          return;
        }

        case "turn_start": {
          if (!context.turnState) {
            yield* openTurn(context);
          }
          return;
        }

        case "message_update": {
          const turnState = context.turnState;
          if (!turnState) return;
          const text = extractAssistantTextDelta(event);
          const reasoning = text === null ? extractReasoningTextDelta(event) : null;
          if (text === null && reasoning === null) return;

          const itemId =
            turnState.activeAssistantItemId ??
            RuntimeItemId.make(`pi-assistant-${yield* nextUuid}`);
          turnState.activeAssistantItemId = itemId;

          if (text !== null) {
            turnState.activeAssistantHasText = true;
            yield* offerRuntimeEvent({
              ...base,
              turnId: turnState.turnId,
              itemId,
              type: "content.delta",
              payload: { streamKind: "assistant_text", delta: text },
            });
            return;
          }

          if (reasoning !== null) {
            yield* offerRuntimeEvent({
              ...base,
              turnId: turnState.turnId,
              itemId,
              type: "content.delta",
              payload: { streamKind: "reasoning_text", delta: reasoning },
            });
          }
          return;
        }

        case "message_end": {
          const job = piBackgroundCompletion(event.message);
          if (job) {
            yield* emitBackgroundTask(context, job);
            return;
          }
          const turnState = context.turnState;
          if (!turnState || event.message.role !== "assistant") return;

          const itemId = turnState.activeAssistantItemId;
          const hasText = turnState.activeAssistantHasText;
          turnState.activeAssistantItemId = undefined;
          turnState.activeAssistantHasText = false;
          if (!itemId || !hasText) return;

          yield* offerRuntimeEvent({
            ...base,
            turnId: turnState.turnId,
            itemId,
            type: "item.completed",
            payload: {
              itemType: "assistant_message",
              status: "completed",
              title: "Assistant message",
            },
          });
          return;
        }

        case "tool_execution_start": {
          if (!context.turnState) return;
          if (event.toolName === "bg") context.backgroundLaunches.add(event.toolCallId);
          const itemId = RuntimeItemId.make(event.toolCallId);

          // `think` renders as reasoning, not a tool row. The whole payload
          // arrives at once, so this single delta is the complete text.
          const thinkText = extractPiThinkText(event.toolName, event.args);
          if (thinkText !== undefined) {
            context.thinkItemIds.add(event.toolCallId);
            yield* offerRuntimeEvent({
              ...base,
              turnId: context.turnState.turnId,
              itemId,
              type: "content.delta",
              payload: { streamKind: "reasoning_text", delta: thinkText },
            });
            return;
          }

          const itemType = classifyPiToolItemType(event.toolName);
          const detail = summarizePiToolArgs(event.args);
          const argsObj =
            event.args && typeof event.args === "object"
              ? (event.args as Record<string, unknown>)
              : undefined;
          context.turnState.items.push({
            id: itemId,
            type: itemType,
            toolName: event.toolName,
            args: event.args,
          });
          yield* offerRuntimeEvent({
            ...base,
            turnId: context.turnState.turnId,
            itemId,
            type: "item.started",
            payload: {
              itemType,
              status: "inProgress",
              title: event.toolName,
              ...(detail ? { detail } : {}),
              ...(argsObj ? { data: { item: { toolName: event.toolName, input: argsObj } } } : {}),
            },
          });
          return;
        }

        case "tool_execution_update": {
          if (!context.turnState) return;
          if (context.thinkItemIds.has(event.toolCallId)) return;
          const partial = (event as { partialResult?: unknown }).partialResult;
          if (partial === undefined) return;
          const itemId = RuntimeItemId.make(event.toolCallId);

          // The bundled `subagent` extension streams structured child snapshots;
          // map recognized snapshots to canonical task.* events instead of text.
          if (event.toolName === "subagent") {
            const snapshot = normalizePiSubagentResult(partial);
            if (snapshot) {
              yield* emitSubagentTaskEvents(
                context,
                event as AgentSessionEvent & { toolCallId: string; toolName: string },
                snapshot,
                false,
              );
              return;
            }
          }

          const itemType = classifyPiToolItemType(event.toolName);
          const delta = extractPiPartialResultText(partial);
          if (delta === undefined || delta.length === 0) return;
          yield* offerRuntimeEvent({
            ...base,
            turnId: context.turnState.turnId,
            itemId,
            type: "content.delta",
            payload: {
              streamKind:
                itemType === "command_execution" ? "command_output" : "file_change_output",
              delta,
            },
          });
          return;
        }

        case "tool_execution_end": {
          context.backgroundLaunches.delete(event.toolCallId);
          for (const job of piBackgroundToolJobs(event.toolName, event.result, event.isError)) {
            yield* emitBackgroundTask(
              context,
              job,
              event.toolName === "bg" ? event.toolCallId : undefined,
              event.toolName === "bg_status",
            );
          }
          if (!context.turnState) return;
          // Already rendered as reasoning at start; `think` has no output to settle.
          if (context.thinkItemIds.delete(event.toolCallId)) return;
          const itemId = RuntimeItemId.make(event.toolCallId);
          const itemType = classifyPiToolItemType(event.toolName);

          // Emit child subagent task completions (from the terminal result
          // details) BEFORE the parent item.completed so consumers see task
          // lifecycle close out under the still-open parent tool call.
          if (event.toolName === "subagent") {
            const snapshot = normalizePiSubagentResult((event as { result?: unknown }).result);
            if (snapshot) {
              yield* emitSubagentTaskEvents(
                context,
                event as AgentSessionEvent & { toolCallId: string; toolName: string },
                snapshot,
                true,
              );
            }
          }

          const storedItem = context.turnState.items.find((item) => item.id === itemId);
          const detail = summarizePiToolArgs(storedItem?.args);
          const argsObj =
            storedItem?.args && typeof storedItem.args === "object"
              ? (storedItem.args as Record<string, unknown>)
              : undefined;
          // Expanded work-log rows read tool output from `data.item.result`;
          // without it a settled Pi tool call expands to its input and nothing else.
          const result = extractPiPartialResultText((event as { result?: unknown }).result);
          yield* offerRuntimeEvent({
            ...base,
            turnId: context.turnState.turnId,
            itemId,
            type: "item.completed",
            payload: {
              itemType,
              title: event.toolName,
              status: event.isError ? "failed" : "completed",
              ...(detail ? { detail } : {}),
              ...(argsObj || result !== undefined
                ? {
                    data: {
                      item: {
                        toolName: event.toolName,
                        ...(argsObj ? { input: argsObj } : {}),
                        ...(result !== undefined ? { result } : {}),
                      },
                    },
                  }
                : {}),
            },
          });
          return;
        }

        case "turn_end": {
          const usage = normalizePiTokenUsage(event, {
            ...(context.currentContextWindow !== undefined
              ? { contextWindow: context.currentContextWindow }
              : {}),
            ...(context.compactsAutomatically !== undefined
              ? { compactsAutomatically: context.compactsAutomatically }
              : {}),
          });
          if (usage) {
            yield* offerRuntimeEvent({
              ...base,
              ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
              type: "thread.token-usage.updated",
              payload: { usage },
            });
          }
          // agent_end drives completion (Pi can run many internal turns per prompt).
          return;
        }

        case "agent_end": {
          // willRetry means pi will auto-retry (another agent_start/end cycle) —
          // finalize only on the terminal end, since a retry isn't a user interrupt
          if (event.willRetry) return;
          if (context.turnState) {
            yield* completeTurn(context, "completed");
          }
          return;
        }

        case "compaction_start": {
          yield* offerRuntimeEvent({
            ...base,
            type: "session.state.changed",
            payload: { state: "waiting", reason: "compaction" },
          });
          return;
        }

        case "compaction_end": {
          yield* offerRuntimeEvent({
            ...base,
            type: "thread.state.changed",
            payload: { state: "compacted" },
          });
          return;
        }

        default:
          return;
      }
    });

  const handleExtensionUIRequest = (
    context: PiSessionContext,
    request: RpcExtensionUIRequest,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      // fire-and-forget UI side-effects — Pi does not await a response
      if (
        request.method === "notify" ||
        request.method === "setStatus" ||
        request.method === "setWidget" ||
        request.method === "setTitle" ||
        request.method === "set_editor_text"
      ) {
        return;
      }

      const stamp = yield* makeEventStamp();
      const requestId = ApprovalRequestId.make(yield* nextUuid);
      const runtimeRequestId = RuntimeRequestId.make(requestId);
      const turnId = context.turnState?.turnId;

      if (request.method === "confirm") {
        const requestType = classifyPiApprovalRequestType(request.title);
        const detail =
          request.message.length > 0 ? `${request.title}\n${request.message}` : request.title;
        const sessionApprovalKey = `${requestType}:${detail}`;
        if (context.sessionApprovals.has(sessionApprovalKey)) {
          yield* context.transport.writeExtensionResponse({
            type: "extension_ui_response",
            id: request.id,
            confirmed: true,
          });
          return;
        }
        context.pendingApprovals.set(requestId, {
          piId: request.id,
          requestType,
          sessionApprovalKey,
        });
        yield* offerRuntimeEvent({
          ...stamp,
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          ...(turnId ? { turnId } : {}),
          requestId: runtimeRequestId,
          type: "request.opened",
          payload: { requestType, detail: detail.slice(0, 2000), args: request },
          ...rawEvent("pi.rpc.extension-ui", request.method, request),
        });
        return;
      }

      const questionId = String(requestId);
      let question: UserInputQuestion;
      let numberedOptions: ReadonlyArray<PendingNumberedOption> | undefined;

      if (request.method === "select") {
        question = {
          id: questionId,
          header: request.title.slice(0, 12) || "Select",
          question: request.title,
          options: request.options.map((label) => ({ label, description: label })),
          multiSelect: false,
        };
      } else {
        const title = "title" in request ? request.title : "";
        const parsed = request.method === "input" ? parseNumberedList(title) : null;
        if (parsed) {
          numberedOptions = parsed.items;
          question = {
            id: questionId,
            header: parsed.title.slice(0, 12) || "Select",
            question: parsed.title,
            options: parsed.items.map((item) => ({ label: item.label, description: item.label })),
            multiSelect: true,
          };
        } else {
          question = {
            id: questionId,
            header: title.slice(0, 12) || "Input",
            question: title || "Input",
            options: [],
            multiSelect: false,
          };
        }
      }

      context.pendingUserInputs.set(requestId, {
        piId: request.id,
        questionId,
        method: request.method,
        ...(numberedOptions ? { numberedOptions } : {}),
      });

      yield* offerRuntimeEvent({
        ...stamp,
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        ...(turnId ? { turnId } : {}),
        requestId: runtimeRequestId,
        type: "user-input.requested",
        payload: { questions: [question] },
        ...rawEvent("pi.rpc.extension-ui", request.method, request),
      });
    });

  const handleMessage = (
    context: PiSessionContext,
    message: PiStdoutMessage,
  ): Effect.Effect<void> => {
    switch (message._tag) {
      case "event":
        return handlePiEvent(context, message.event);
      case "extension-ui":
        return handleExtensionUIRequest(context, message.request);
      case "response":
        return Effect.void;
    }
  };

  // settle+clear pending extension-UI requests so Pi is never left blocked
  const cancelPendingExtensionRequests = (context: PiSessionContext): Effect.Effect<void> =>
    Effect.gen(function* () {
      for (const [requestId, pending] of context.pendingApprovals) {
        yield* Effect.ignore(
          context.transport.writeExtensionResponse({
            type: "extension_ui_response",
            id: pending.piId,
            confirmed: false,
          }),
        );
        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          ...stamp,
          type: "request.resolved",
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
          requestId: RuntimeRequestId.make(requestId),
          payload: { requestType: pending.requestType, decision: "decline" },
        });
      }
      context.pendingApprovals.clear();
      for (const [requestId, pending] of context.pendingUserInputs) {
        yield* Effect.ignore(
          context.transport.writeExtensionResponse({
            type: "extension_ui_response",
            id: pending.piId,
            cancelled: true,
          }),
        );
        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          ...stamp,
          type: "user-input.resolved",
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
          requestId: RuntimeRequestId.make(requestId),
          payload: { answers: {} },
        });
      }
      context.pendingUserInputs.clear();
    });

  const stopSessionInternal = (
    context: PiSessionContext,
    opts?: {
      readonly emitExitEvent?: boolean;
      readonly exitKind?: "graceful" | "error";
      readonly reason?: string;
    },
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      if (context.stopped) return;
      context.stopped = true;

      if (context.turnState) {
        yield* completeTurn(context, "interrupted", "Session stopped.");
      }

      yield* cancelPendingExtensionRequests(context);

      if (context.notificationFiber) yield* Fiber.interrupt(context.notificationFiber);

      // Pi's graceful process shutdown runs extension cleanup, including bg's
      // detached process groups. An RPC abort alone only stops the LLM turn.
      if (hasBackgroundWork(context)) {
        yield* context.transport.kill;
      }
      yield* Effect.ignore(Scope.close(context.sessionScope, Exit.void));
      yield* finalizeBackgroundTasks(context, opts?.exitKind === "error" ? "failed" : "stopped");

      const updatedAt = yield* nowIso;
      const { activeTurnId: _activeTurnId, ...closedSession } = context.session;
      context.session = { ...closedSession, status: "closed", updatedAt };
      sessions.delete(context.session.threadId);

      if (opts?.emitExitEvent !== false) {
        const exitKind = opts?.exitKind ?? "graceful";
        const reason =
          opts?.reason ??
          (exitKind === "error" ? "Pi process exited unexpectedly." : "Session stopped");
        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          ...stamp,
          type: "session.exited",
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          payload: {
            reason,
            exitKind,
            ...(exitKind === "error" ? { recoverable: false } : {}),
          },
        });
      }
    });

  const requireSession = (
    threadId: ThreadId,
  ): Effect.Effect<PiSessionContext, ProviderAdapterError> => {
    const context = sessions.get(threadId);
    if (!context) {
      return Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
    }
    if (context.stopped || context.session.status === "closed") {
      return Effect.fail(new ProviderAdapterSessionClosedError({ provider: PROVIDER, threadId }));
    }
    return Effect.succeed(context);
  };

  // resolve attachments before mutating turn state so a bad one fails cleanly
  const resolvePromptImages = (
    attachments: ProviderSendTurnInput["attachments"],
  ): Effect.Effect<ReadonlyArray<PiImageContent>, ProviderAdapterError> =>
    Effect.forEach(attachments ?? [], (attachment) =>
      Effect.gen(function* () {
        const attachmentPath = resolveAttachmentPath({
          attachmentsDir: serverConfig.attachmentsDir,
          attachment,
        });
        if (!attachmentPath) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "prompt",
            detail: `Invalid attachment id '${attachment.id}'.`,
          });
        }
        const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "prompt",
                detail: `Failed to read attachment '${attachment.id}'.`,
                cause,
              }),
          ),
        );
        return piImageContentFromBytes({ mimeType: attachment.mimeType, bytes });
      }),
    );

  // switch only on change; fail closed (prompt not sent) on a bad slug or rejection
  const maybeSwitchPiModel = (
    context: PiSessionContext,
    requestedModel: string | undefined,
  ): Effect.Effect<void, ProviderAdapterError> =>
    Effect.gen(function* () {
      const plan = planPiModelSwitch(context.currentModel, requestedModel);
      if (plan.kind === "noop") return;
      if (plan.kind === "invalid") {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `Invalid Pi model slug '${plan.slug}'; expected 'provider/id'.`,
        });
      }
      const response = yield* context.transport.request(
        { type: "set_model", provider: plan.provider, modelId: plan.modelId },
        `pi-set-model-${yield* nextUuid}`,
        PI_MODEL_OPTIONS_TIMEOUT_MS,
      );
      if (!piResponseSucceeded(response, "set_model")) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "set_model",
          detail: `Pi rejected model switch to '${plan.slug}'.`,
        });
      }
      context.currentModel = plan.slug;
      const contextConfig = extractPiContextConfig(response);
      context.currentContextWindow = contextConfig.contextWindow;
      context.session = { ...context.session, model: plan.slug };
      // a model switch can reset the thinking level — force re-apply next turn
      context.appliedThinkingLevel = undefined;
    });

  const applyThinkingLevel = (
    context: PiSessionContext,
    modelSelection: ModelSelection | null | undefined,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const level = resolvePiThinkingLevel(modelSelection);
      if (level === undefined || level === context.appliedThinkingLevel) return;
      const response = yield* context.transport.request(
        { type: "set_thinking_level", level },
        `pi-set-thinking-${yield* nextUuid}`,
        PI_MODEL_OPTIONS_TIMEOUT_MS,
      );
      if (piResponseSucceeded(response, "set_thinking_level")) {
        context.appliedThinkingLevel = level;
      } else {
        yield* Effect.logWarning("pi.thinking.set-failed", {
          threadId: context.session.threadId,
          level,
        });
      }
    });

  const startSession: PiAdapterShape["startSession"] = Effect.fn("startSession")(function* (input) {
    if (input.provider !== undefined && input.provider !== PROVIDER) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "startSession",
        issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
      });
    }

    const existing = sessions.get(input.threadId);
    if (existing) {
      yield* stopSessionInternal(existing, { emitExitEvent: false }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("pi.session.replace.stop-failed", { threadId: input.threadId, cause }),
        ),
      );
    }

    const startedAt = yield* nowIso;
    const threadId = input.threadId;
    const modelSelection =
      input.modelSelection !== undefined && input.modelSelection.instanceId === boundInstanceId
        ? input.modelSelection
        : undefined;
    const resumeState = readPiResumeState(input.resumeCursor);
    const cwd = input.cwd ?? serverConfig.cwd;
    const thinkingLevel = resolvePiThinkingLevel(modelSelection);

    const spawnArgs: string[] = ["--mode", "rpc"];
    if (resumeState) spawnArgs.push("--session", resumeState.sessionFile);
    if (modelSelection?.model) spawnArgs.push("--model", modelSelection.model);
    if (thinkingLevel) spawnArgs.push("--thinking", thinkingLevel);

    // gate driven by runtimeMode; if required, must be provably active or we fail closed
    const approvalGate = approvalGateForRuntimeMode(input.runtimeMode);
    let processEnv = baseEnvironment;
    let verifyApprovalGate = false;
    if (approvalGate.gate) {
      if (!approvalExtensionAvailable || !approvalExtensionPath) {
        return yield* new ProviderAdapterProcessError({
          provider: PROVIDER,
          threadId,
          detail:
            "Tool approval is required for this runtime mode but the bundled approval gate is unavailable; refusing to start an ungated Pi session.",
        });
      }
      spawnArgs.push("--extension", approvalExtensionPath);
      processEnv = { ...baseEnvironment, T3_PI_APPROVAL_MODE: approvalGate.mode };
      verifyApprovalGate = true;
    }

    const sessionScope = yield* Scope.make();

    const makeTransport = options?.makeTransport ?? makePiRpcTransport;
    const transport = yield* makeTransport({
      binaryPath: piSettings.binaryPath || "pi",
      args: spawnArgs,
      cwd,
      env: processEnv,
      onExit: Effect.suspend(() => {
        const live = sessions.get(threadId);
        if (live && !live.stopped && live.session.status !== "closed") {
          return stopSessionInternal(live, { emitExitEvent: true, exitKind: "error" });
        }
        return Effect.void;
      }),
    }).pipe(
      Effect.provideService(Scope.Scope, sessionScope),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.mapError(
        (cause) =>
          new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId,
            detail: toMessage(cause, "Failed to start Pi RPC process."),
            cause,
          }),
      ),
      Effect.onError(() => Effect.ignore(Scope.close(sessionScope, Exit.void))),
    );

    const session: ProviderSession = {
      threadId,
      provider: PROVIDER,
      providerInstanceId: boundInstanceId,
      status: "ready",
      runtimeMode: input.runtimeMode,
      ...(input.cwd ? { cwd: input.cwd } : {}),
      ...(modelSelection?.model ? { model: modelSelection.model } : {}),
      createdAt: startedAt,
      updatedAt: startedAt,
    };

    const context: PiSessionContext = {
      session,
      sessionScope,
      transport,
      notificationFiber: undefined,
      pendingApprovals: new Map(),
      pendingUserInputs: new Map(),
      sessionApprovals: new Set(),
      turnState: undefined,
      turns: [],
      subagentTasks: new Map(),
      backgroundTasks: new Map(),
      backgroundLaunches: new Set(),
      thinkItemIds: new Set(),
      stopped: false,
      currentModel: modelSelection?.model,
      currentContextWindow: undefined,
      compactsAutomatically: undefined,
      appliedThinkingLevel: thinkingLevel,
    };
    sessions.set(threadId, context);

    const notificationFiber = yield* Stream.fromQueue(transport.messages).pipe(
      Stream.mapEffect((message) => handleMessage(context, message)),
      Stream.runDrain,
      Effect.catchCause((cause) =>
        Effect.logError("Failed to process Pi runtime message.", { cause }),
      ),
      Effect.forkIn(sessionScope),
    );
    context.notificationFiber = notificationFiber;

    const stateResponse = yield* transport.request(
      { type: "get_state" },
      `pi-get-state-${yield* nextUuid}`,
      PI_STATE_TIMEOUT_MS,
    );
    const sessionFile = extractSessionFile(stateResponse);
    const contextConfig = extractPiContextConfig(stateResponse);
    context.currentContextWindow = contextConfig.contextWindow;
    context.compactsAutomatically = contextConfig.compactsAutomatically;
    if (sessionFile !== undefined) {
      context.session = { ...context.session, resumeCursor: { sessionFile } };
    }

    // fail closed unless the gate extension registered its sentinel command
    if (verifyApprovalGate) {
      const commandsResponse = yield* transport.request(
        { type: "get_commands" },
        `pi-get-commands-${yield* nextUuid}`,
        PI_COMMANDS_TIMEOUT_MS,
      );
      if (!piResponseHasCommand(commandsResponse, PI_APPROVAL_SENTINEL_COMMAND)) {
        yield* stopSessionInternal(context, { emitExitEvent: false });
        return yield* new ProviderAdapterProcessError({
          provider: PROVIDER,
          threadId,
          detail:
            "Tool approval is enabled but the approval gate failed to load; refusing to run an ungated Pi session.",
        });
      }
    }

    const startedStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      ...startedStamp,
      type: "session.started",
      provider: PROVIDER,
      providerInstanceId: boundInstanceId,
      threadId,
      payload: {},
    });

    // session file is the provider-native thread id — publish for provider_thread_id parity
    if (sessionFile !== undefined) {
      const threadStartedStamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        ...threadStartedStamp,
        type: "thread.started",
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId,
        payload: { providerThreadId: sessionFile },
      });
    }

    const configuredStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      ...configuredStamp,
      type: "session.configured",
      provider: PROVIDER,
      providerInstanceId: boundInstanceId,
      threadId,
      payload: {
        config: {
          ...(modelSelection?.model ? { model: modelSelection.model } : {}),
          ...(input.cwd ? { cwd: input.cwd } : {}),
        },
      },
    });

    const readyStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      ...readyStamp,
      type: "session.state.changed",
      provider: PROVIDER,
      providerInstanceId: boundInstanceId,
      threadId,
      payload: { state: "ready" },
    });

    return { ...context.session };
  });

  const sendTurn: PiAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
    const context = yield* requireSession(input.threadId);

    const requestedModel =
      input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection.model : undefined;
    const promptText = typeof input.input === "string" ? input.input : "";
    // resolve before mutating turn state so a bad attachment fails cleanly
    const images = yield* resolvePromptImages(input.attachments);

    if (promptText.length === 0 && images.length === 0) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "sendTurn",
        issue: "Pi turns require non-empty text or at least one attachment.",
      });
    }

    // a message mid-turn steers the running turn; otherwise it opens a fresh one
    const isMidTurn = context.turnState !== undefined;

    // only on a fresh turn — changing options mid-stream would race the active turn
    if (!isMidTurn) {
      yield* maybeSwitchPiModel(context, requestedModel);
      yield* applyThinkingLevel(context, input.modelSelection);
    }

    if (!context.turnState) {
      const turnId = TurnId.make(yield* nextUuid);
      const startedAt = yield* nowIso;
      context.turnState = makeTurnState(turnId, startedAt);
      context.session = {
        ...context.session,
        status: "running",
        activeTurnId: turnId,
        updatedAt: startedAt,
      };
      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        ...stamp,
        type: "turn.started",
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        turnId,
        payload: context.currentModel ? { model: context.currentModel } : {},
      });
    }

    const turnId = context.turnState.turnId;

    yield* context.transport
      .writeCommand(buildPiTurnCommand({ isMidTurn, message: promptText, images }))
      .pipe(
        Effect.catchCause((cause) =>
          completeTurn(context, "failed", "Failed to send message to Pi.").pipe(
            Effect.andThen(
              Effect.fail(
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "prompt",
                  detail: "Failed to send message to Pi.",
                  cause,
                }),
              ),
            ),
          ),
        ),
      );

    return {
      threadId: context.session.threadId,
      turnId,
      ...(context.session.resumeCursor !== undefined
        ? { resumeCursor: context.session.resumeCursor }
        : {}),
    };
  });

  const interruptTurn: PiAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
    function* (threadId) {
      const context = yield* requireSession(threadId);
      if (hasBackgroundWork(context)) {
        // Stop-everything must stop the process owning bg, not just its current
        // turn. The persisted resume cursor keeps the conversation resumable.
        yield* stopSessionInternal(context, { emitExitEvent: true });
        return;
      }
      yield* Effect.ignore(context.transport.writeCommand({ type: "abort" }));
      // settle bridged requests so Pi isn't left blocked (matches Cursor)
      yield* cancelPendingExtensionRequests(context);
      if (context.turnState) {
        yield* completeTurn(context, "interrupted", "Turn interrupted.");
      }
    },
  );

  const respondToRequest: PiAdapterShape["respondToRequest"] = Effect.fn("respondToRequest")(
    function* (threadId, requestId, decision: ProviderApprovalDecision) {
      const context = yield* requireSession(threadId);
      const pending = context.pendingApprovals.get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToRequest",
          detail: `Unknown pending approval request: ${requestId}.`,
        });
      }
      context.pendingApprovals.delete(requestId);

      const response: RpcExtensionUIResponse = buildPiApprovalResponse(pending.piId, decision);
      yield* context.transport.writeExtensionResponse(response);
      if (decision === "acceptForSession") {
        context.sessionApprovals.add(pending.sessionApprovalKey);
      }

      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        ...stamp,
        type: "request.resolved",
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
        requestId: RuntimeRequestId.make(requestId),
        payload: { requestType: pending.requestType, decision },
      });
    },
  );

  const respondToUserInput: PiAdapterShape["respondToUserInput"] = Effect.fn("respondToUserInput")(
    function* (threadId, requestId, answers: ProviderUserInputAnswers) {
      const context = yield* requireSession(threadId);
      const pending = context.pendingUserInputs.get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToUserInput",
          detail: `Unknown pending user-input request: ${requestId}.`,
        });
      }
      context.pendingUserInputs.delete(requestId);

      const response: RpcExtensionUIResponse = buildPiUserInputResponse(pending, answers);

      yield* context.transport.writeExtensionResponse(response);

      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        ...stamp,
        type: "user-input.resolved",
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: context.session.threadId,
        ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
        requestId: RuntimeRequestId.make(requestId),
        payload: { answers },
      });
    },
  );

  const readThread: PiAdapterShape["readThread"] = Effect.fn("readThread")(function* (threadId) {
    const context = yield* requireSession(threadId);
    return {
      threadId,
      turns: context.turns.map((turn) => ({ id: turn.id, items: [...turn.items] })),
    };
  });

  const rollbackThread: PiAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
    function* (threadId, numTurns) {
      const context = yield* requireSession(threadId);

      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "numTurns must be an integer >= 1.",
        });
      }

      // forking mid-stream is undefined — abort/finalize any live turn first
      if (context.turnState) {
        yield* Effect.ignore(context.transport.writeCommand({ type: "abort" }));
        yield* cancelPendingExtensionRequests(context);
        yield* completeTurn(context, "interrupted", "Turn interrupted for rollback.");
      }

      const forkResponse = yield* context.transport.request(
        { type: "get_fork_messages" },
        `pi-fork-messages-${yield* nextUuid}`,
        PI_MESSAGES_TIMEOUT_MS,
      );
      if (!piResponseSucceeded(forkResponse, "get_fork_messages")) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "get_fork_messages",
          detail: "Pi did not return forkable messages for rollback.",
        });
      }
      const userMessages = extractForkMessages(forkResponse);
      const target = resolveForkTargetEntryId(userMessages, numTurns);

      if (target === null) {
        // no known Pi history to fork against; just trim the local skeleton
        context.turns.splice(Math.max(0, context.turns.length - numTurns));
        return yield* readThread(threadId);
      }

      // fork branches before the target message; new_session resets past the first
      const rollbackResponse =
        target.kind === "fork"
          ? yield* context.transport.request(
              { type: "fork", entryId: target.entryId },
              `pi-fork-${yield* nextUuid}`,
              PI_FORK_TIMEOUT_MS,
            )
          : yield* context.transport.request(
              { type: "new_session" },
              `pi-new-session-${yield* nextUuid}`,
              PI_FORK_TIMEOUT_MS,
            );
      if (!piForkSucceeded(rollbackResponse)) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: target.kind === "fork" ? "fork" : "new_session",
          detail: "Pi rejected or cancelled the rollback.",
        });
      }

      // A successful Pi fork/reset shuts down the old extension runtime. Its
      // job IDs may restart at 1, so close the old tasks before accepting new ones.
      yield* finalizeBackgroundTasks(context, "stopped");

      // CRITICAL: fork/new_session rebinds to a new session file — refresh the
      // resume cursor or a later reconnect resumes the stale pre-rollback branch
      const stateResponse = yield* context.transport.request(
        { type: "get_state" },
        `pi-get-state-${yield* nextUuid}`,
        PI_STATE_TIMEOUT_MS,
      );
      const sessionFile = extractSessionFile(stateResponse);
      const contextConfig = extractPiContextConfig(stateResponse);
      context.currentContextWindow = contextConfig.contextWindow;
      context.compactsAutomatically = contextConfig.compactsAutomatically;
      const updatedAt = yield* nowIso;
      context.session = {
        ...context.session,
        status: "ready",
        updatedAt,
        resumeCursor: sessionFile !== undefined ? { sessionFile } : undefined,
      };

      if (sessionFile !== undefined) {
        const threadStartedStamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          ...threadStartedStamp,
          type: "thread.started",
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId,
          payload: { providerThreadId: sessionFile },
        });
      }

      context.turns.splice(Math.max(0, context.turns.length - numTurns));

      return yield* readThread(threadId);
    },
  );

  const stopSession: PiAdapterShape["stopSession"] = Effect.fn("stopSession")(function* (threadId) {
    const context = yield* requireSession(threadId);
    yield* stopSessionInternal(context, { emitExitEvent: true });
  });

  const listSessions: PiAdapterShape["listSessions"] = () =>
    Effect.sync(() => Array.from(sessions.values(), ({ session }) => ({ ...session })));

  const hasSession: PiAdapterShape["hasSession"] = (threadId) =>
    Effect.sync(() => {
      const context = sessions.get(threadId);
      return context !== undefined && !context.stopped;
    });

  const stopAll: PiAdapterShape["stopAll"] = () =>
    Effect.forEach(
      sessions,
      ([, context]) => stopSessionInternal(context, { emitExitEvent: true }),
      {
        discard: true,
      },
    );

  yield* Effect.addFinalizer(() =>
    Effect.forEach(
      sessions,
      ([, context]) => stopSessionInternal(context, { emitExitEvent: false }),
      {
        discard: true,
      },
    ).pipe(Effect.tap(() => Queue.shutdown(runtimeEventQueue))),
  );

  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session" as const },
    startSession,
    sendTurn,
    interruptTurn,
    readThread,
    rollbackThread,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    stopAll,
    get streamEvents() {
      return Stream.fromQueue(runtimeEventQueue);
    },
  } satisfies PiAdapterShape;
});
