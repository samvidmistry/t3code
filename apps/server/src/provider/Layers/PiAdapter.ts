/** `ProviderAdapterShape` for the Pi coding agent (per-thread `pi --mode rpc` sessions). */
import * as NodeURL from "node:url";
import * as Path from "effect/Path";

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
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/unstable/process";

import { HostProcessPlatform } from "@t3tools/shared/hostProcess";

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
  PI_CODEMODE_TOOL,
  piCodemodeCode,
  piCodemodeDetail,
  piCodemodeOutput,
  piCodemodeScript,
} from "./PiCodemode.ts";
import {
  isPiWorkflowRunId,
  normalizePiWorkflowSnapshot,
  piWorkflowBackgroundRunId,
  piWorkflowCallName,
  piWorkflowProgressSummary,
  piWorkflowRunsDirectory,
  type PiWorkflowSnapshot,
} from "./PiWorkflowRuns.ts";
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
  extractPiContextConfig,
  extractReasoningTextDelta,
  extractSessionFile,
  makePiRpcTransport,
  type MakePiRpcTransportOptions,
  piImageContentFromBytes,
  type PiImageContent,
  piResponseHasCommand,
  piResponseData,
  piResponseSucceeded,
  planPiModelSwitch,
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

// A dynamic-workflows run shown as a T3 workflow task with one member per agent.
interface WorkflowRunState {
  readonly taskId: RuntimeTaskId;
  readonly toolUseId: string;
  readonly turnId: TurnId | undefined;
  /** Background runs are followed through their run file after the tool returns. */
  readonly background: boolean;
  runId: string | undefined;
  name: string | undefined;
  started: boolean;
  completed: boolean;
  progressFingerprint: string | undefined;
  readonly memberFingerprints: Map<number, string>;
}

const decodeWorkflowRunFile = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

// Background runs are persisted after each finished agent; poll at a similar pace.
const WORKFLOW_POLL_INTERVAL = "2 seconds";

interface PiTurnState {
  readonly turnId: TurnId;
  readonly startedAt: string;
  readonly items: Array<PiToolItem>;
  activeAssistantItemId: RuntimeItemId | undefined;
  activeAssistantHasText: boolean;
  terminalState?: "completed" | "failed" | "interrupted";
  errorMessage?: string | undefined;
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
  // Workflow runs by tool call id (foreground) or `run:<runId>` (background).
  readonly workflowRuns: Map<string, WorkflowRunState>;
  readonly workflowRunsDirectory: string;
  workflowPolling: boolean;
  // One entry per T3 turn, oldest first: when T3 sent the turn's opening
  // prompt, or null for turns Pi started on its own. Rewind uses these to find
  // the Pi user message that opened a turn; steers never add an entry.
  turnStarts: Array<number | null>;
}

/** A tool call another tool made, such as a codemode script's `tools.read(...)`. */
function isNestedToolEvent(event: object): boolean {
  const parent = (event as { parentToolCallId?: unknown }).parentToolCallId;
  return typeof parent === "string" && parent.length > 0;
}

/** Text of a background workflow result the dynamic-workflows plugin delivered. */
function piWorkflowResultMessage(message: unknown): string | undefined {
  if (message === null || typeof message !== "object") return undefined;
  const record = message as Record<string, unknown>;
  if (record["role"] !== "custom" || record["customType"] !== "workflow-result") return undefined;
  const content = record["content"];
  if (typeof content === "string") return content.trim() || undefined;
  if (!Array.isArray(content)) return undefined;
  const text = content
    .flatMap((part) =>
      part !== null && typeof part === "object" && typeof part.text === "string" ? [part.text] : [],
    )
    .join("\n")
    .trim();
  return text || undefined;
}

function hasBackgroundWork(context: PiSessionContext): boolean {
  return (
    context.backgroundLaunches.size > 0 ||
    [...context.backgroundTasks.values()].some((task) => !task.completed) ||
    [...context.workflowRuns.values()].some((run) => run.background && !run.completed)
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

// Bounds the resume cursor. Rewinds further back than this are refused.
const MAX_TRACKED_PI_TURNS = 500;

function readPiResumeState(
  resumeCursor: unknown,
): { sessionFile: string; turnStarts: Array<number | null> } | undefined {
  if (!resumeCursor || typeof resumeCursor !== "object") return undefined;
  const cursor = resumeCursor as Record<string, unknown>;
  if (typeof cursor["sessionFile"] !== "string" || cursor["sessionFile"].trim().length === 0) {
    return undefined;
  }
  const turnStarts = cursor["turnStarts"];
  return {
    sessionFile: cursor["sessionFile"].trim(),
    // Cursors from before rewind support carry no boundaries; their turns
    // stay unreachable rather than being guessed.
    turnStarts:
      Array.isArray(turnStarts) &&
      turnStarts.every((value) => value === null || typeof value === "number")
        ? [...(turnStarts as Array<number | null>)]
        : [],
  };
}

function piEntryTimestampMs(entry: Record<string, unknown>): number | undefined {
  const message = entry["message"];
  const messageTimestamp =
    message !== null && typeof message === "object"
      ? (message as Record<string, unknown>)["timestamp"]
      : undefined;
  if (typeof messageTimestamp === "number" && Number.isFinite(messageTimestamp)) {
    return messageTimestamp;
  }
  const parsed = typeof entry["timestamp"] === "string" ? Date.parse(entry["timestamp"]) : NaN;
  return Number.isFinite(parsed) ? parsed : undefined;
}

/**
 * Finds the user message that opened a T3 turn: the first user entry on Pi's
 * active branch sent at or after `startedAt` and, when known, before the next
 * turn's prompt. Branch entries come from `get_entries`, which also returns
 * abandoned branches, so the path is walked from the leaf.
 */
export function findPiTurnStartEntryId(input: {
  readonly entries: ReadonlyArray<unknown>;
  readonly leafId: string | null;
  readonly startedAt: number;
  readonly nextStartedAt: number | undefined;
}): string | undefined {
  const byId = new Map<string, Record<string, unknown>>();
  for (const entry of input.entries) {
    if (entry === null || typeof entry !== "object") continue;
    const record = entry as Record<string, unknown>;
    if (typeof record["id"] === "string") byId.set(record["id"], record);
  }
  const path: Array<Record<string, unknown>> = [];
  const seen = new Set<string>();
  let cursor = input.leafId;
  while (cursor !== null && !seen.has(cursor)) {
    seen.add(cursor);
    const entry = byId.get(cursor);
    if (!entry) break;
    path.push(entry);
    cursor = typeof entry["parentId"] === "string" ? entry["parentId"] : null;
  }
  path.reverse();
  for (const entry of path) {
    if (entry["type"] !== "message") continue;
    const message = entry["message"];
    if (message === null || typeof message !== "object") continue;
    if ((message as Record<string, unknown>)["role"] !== "user") continue;
    const timestamp = piEntryTimestampMs(entry);
    if (timestamp === undefined || timestamp < input.startedAt) continue;
    if (input.nextStartedAt !== undefined && timestamp >= input.nextStartedAt) return undefined;
    return entry["id"] as string;
  }
  return undefined;
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
  const adapterScope = yield* Scope.Scope;
  const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
  const serverConfig = yield* ServerConfig;
  const crypto = yield* Crypto.Crypto;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const baseEnvironment = options?.environment ?? process.env;
  const hostPlatform = yield* HostProcessPlatform;

  let approvalExtensionPath: string | undefined;
  for (const candidate of APPROVAL_EXTENSION_CANDIDATES) {
    const exists = yield* fileSystem.exists(candidate).pipe(Effect.orElseSucceed(() => false));
    if (exists) {
      approvalExtensionPath = candidate;
      break;
    }
  }
  if (approvalExtensionPath !== undefined) {
    // Desktop assets may live in an Electron asar, which the external Pi CLI
    // cannot read. Materialize the bundled gate outside it for this driver.
    const directory = yield* fileSystem
      .makeTempDirectoryScoped({ prefix: "t3-pi-extension-" })
      .pipe(Effect.orDie);
    const source = yield* fileSystem.readFileString(approvalExtensionPath).pipe(Effect.orDie);
    approvalExtensionPath = path.join(directory, "t3-approvals.ts");
    yield* fileSystem.writeFileString(approvalExtensionPath, source).pipe(Effect.orDie);
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

  // Mirrors Claude's workflow tasks: a `local_workflow` coordinator carrying the
  // phases, plus a timeline-bypassing member row per agent that changed.
  const emitWorkflow = Effect.fn("PiAdapter.emitWorkflow")(function* (
    context: PiSessionContext,
    run: WorkflowRunState,
    snapshot: PiWorkflowSnapshot | undefined,
    terminal?: { readonly status: "completed" | "failed" | "stopped"; readonly summary?: string },
  ) {
    if (run.completed) return;
    if (snapshot?.name) run.name = snapshot.name;
    if (snapshot?.runId) run.runId = snapshot.runId;
    const title = run.name ?? "Workflow";
    const linkage = {
      taskType: "local_workflow",
      title,
      ...(run.name ? { workflowName: run.name } : {}),
      toolUseId: run.toolUseId,
      ...(run.runId ? { runHandles: { runId: run.runId } } : {}),
    };
    const offer = (event: Record<string, unknown>) =>
      Effect.flatMap(makeEventStamp(), (stamp) =>
        offerRuntimeEvent({
          ...stamp,
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: context.session.threadId,
          ...(run.turnId ? { turnId: run.turnId } : {}),
          ...event,
        } as ProviderRuntimeEvent),
      );

    if (!run.started) {
      run.started = true;
      yield* offer({
        type: "task.started",
        payload: { taskId: run.taskId, description: title, ...linkage },
      });
    }

    const typedUsage =
      snapshot?.tokens !== undefined ? { typedUsage: { totalTokens: snapshot.tokens } } : {};
    if (snapshot) {
      for (const agent of snapshot.agents) {
        const phaseIndex = agent.phase ? snapshot.phases.indexOf(agent.phase) : -1;
        const fingerprint = [
          agent.status,
          agent.label,
          agent.model ?? "",
          agent.error ?? "",
          agent.tokens ?? "",
          agent.phase ?? "",
        ].join("\u001f");
        if (run.memberFingerprints.get(agent.index) === fingerprint) continue;
        run.memberFingerprints.set(agent.index, fingerprint);
        yield* offer({
          type: "task.progress",
          payload: {
            taskId: RuntimeTaskId.make(`${run.taskId}:wf:${agent.index}`),
            description: agent.label,
            title: agent.label,
            status: agent.status,
            ...(agent.error ? { error: agent.error } : {}),
            ...(agent.model ? { model: agent.model } : {}),
            ...(agent.tokens !== undefined ? { typedUsage: { totalTokens: agent.tokens } } : {}),
            parentAgentId: run.taskId,
            agentIndex: agent.index,
            ...(phaseIndex >= 0 ? { phaseIndex, phaseTitle: agent.phase } : {}),
            timelineBypass: true,
          },
        });
      }
      const summary = piWorkflowProgressSummary(snapshot);
      const fingerprint = [title, summary, snapshot.phases.join("\u001f"), snapshot.tokens].join(
        "\u001e",
      );
      if (fingerprint !== run.progressFingerprint) {
        run.progressFingerprint = fingerprint;
        yield* offer({
          type: "task.progress",
          payload: {
            taskId: run.taskId,
            description: title,
            summary,
            ...(terminal ? {} : { status: "running" }),
            ...typedUsage,
            ...(snapshot.phases.length > 0
              ? {
                  phases: snapshot.phases.map((phaseTitle, index) => ({
                    index,
                    title: phaseTitle,
                  })),
                }
              : {}),
            ...linkage,
          },
        });
      }
    }

    if (!terminal) return;
    run.completed = true;
    const summary =
      terminal.summary ??
      `Workflow ${terminal.status}${snapshot ? `: ${piWorkflowProgressSummary(snapshot)}` : ""}`;
    yield* offer({
      type: "task.completed",
      payload: {
        taskId: run.taskId,
        status: terminal.status,
        summary,
        ...typedUsage,
        ...linkage,
      },
    });
    for (const [key, candidate] of context.workflowRuns) {
      if (candidate === run) context.workflowRuns.delete(key);
    }
  });

  const pollWorkflowRun = Effect.fn("PiAdapter.pollWorkflowRun")(function* (
    context: PiSessionContext,
    run: WorkflowRunState,
  ) {
    if (run.completed || run.runId === undefined || !isPiWorkflowRunId(run.runId)) return;
    const file = path.join(context.workflowRunsDirectory, `${run.runId}.json`);
    // The plugin rewrites the file in place; skip a torn or missing read and retry next tick.
    const parsed = yield* fileSystem
      .readFileString(file)
      .pipe(Effect.flatMap(decodeWorkflowRunFile), Effect.option);
    if (parsed._tag === "None") return;
    const snapshot = normalizePiWorkflowSnapshot(parsed.value);
    if (!snapshot) return;
    const status = snapshot.status;
    yield* emitWorkflow(
      context,
      run,
      snapshot,
      status === undefined || status === "running"
        ? undefined
        : status === "paused"
          ? {
              status: "stopped",
              summary: `Workflow paused: ${piWorkflowProgressSummary(snapshot)}`,
            }
          : { status },
    );
  });

  const pollWorkflowRuns = (context: PiSessionContext) =>
    Effect.forEach(
      [...context.workflowRuns.values()].filter((run) => run.background),
      (run) => pollWorkflowRun(context, run),
      { discard: true },
    );

  /** Follow background runs until none are live; lives in the Pi process's scope. */
  const ensureWorkflowPolling = (context: PiSessionContext) =>
    Effect.gen(function* () {
      if (context.workflowPolling || context.stopped) return;
      context.workflowPolling = true;
      const live = () =>
        !context.stopped &&
        [...context.workflowRuns.values()].some((run) => run.background && !run.completed);
      yield* Effect.gen(function* () {
        while (live()) {
          yield* Effect.sleep(WORKFLOW_POLL_INTERVAL);
          yield* pollWorkflowRuns(context);
        }
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            context.workflowPolling = false;
          }),
        ),
        Effect.forkIn(context.sessionScope),
      );
    });

  const newWorkflowRun = (
    context: PiSessionContext,
    input: { taskId: string; toolUseId: string; background: boolean; runId?: string },
  ): WorkflowRunState => ({
    taskId: RuntimeTaskId.make(input.taskId),
    toolUseId: input.toolUseId,
    turnId: context.turnState?.turnId,
    background: input.background,
    runId: input.runId,
    name: undefined,
    started: false,
    completed: false,
    progressFingerprint: undefined,
    memberFingerprints: new Map(),
  });

  const foregroundWorkflowRun = (context: PiSessionContext, toolCallId: string) => {
    let run = context.workflowRuns.get(toolCallId);
    if (!run) {
      run = newWorkflowRun(context, {
        taskId: `pi-workflow:${toolCallId}`,
        toolUseId: toolCallId,
        background: false,
      });
      context.workflowRuns.set(toolCallId, run);
    }
    return run;
  };

  /** Settle a foreground run from its result, or start following a background one. */
  const trackWorkflowToolResult = Effect.fn("PiAdapter.trackWorkflowToolResult")(function* (
    context: PiSessionContext,
    event: Extract<AgentSessionEvent, { type: "tool_execution_end" }>,
    args: unknown,
  ) {
    const result = (event as { result?: unknown }).result;
    const runId = event.isError ? undefined : piWorkflowBackgroundRunId(event.toolName, result);
    if (runId !== undefined) {
      const key = `run:${runId}`;
      let run = context.workflowRuns.get(key);
      if (!run) {
        run = newWorkflowRun(context, {
          taskId: `pi-workflow:${runId}`,
          toolUseId: event.toolCallId,
          background: true,
          runId,
        });
        run.name = piWorkflowCallName(args);
        context.workflowRuns.set(key, run);
      }
      yield* emitWorkflow(context, run, undefined);
      yield* pollWorkflowRun(context, run);
      yield* ensureWorkflowPolling(context);
      return;
    }
    if (event.toolName !== "workflow") return;
    const snapshot = normalizePiWorkflowSnapshot((result as { details?: unknown } | null)?.details);
    const run = context.workflowRuns.get(event.toolCallId);
    if (!snapshot && !run) return;
    const foreground = run ?? foregroundWorkflowRun(context, event.toolCallId);
    if (!foreground.name) foreground.name = piWorkflowCallName(args);
    yield* emitWorkflow(context, foreground, snapshot, {
      status: event.isError ? "failed" : "completed",
    });
  });

  const finalizeWorkflowRuns = Effect.fn("PiAdapter.finalizeWorkflowRuns")(function* (
    context: PiSessionContext,
    status: "failed" | "stopped",
    onlyForeground: boolean,
  ) {
    for (const run of context.workflowRuns.values()) {
      if (onlyForeground && run.background) continue;
      yield* emitWorkflow(context, run, undefined, { status });
    }
  });

  const completeTurn = (
    context: PiSessionContext,
    state: "completed" | "failed" | "interrupted" | "cancelled",
    errorMessage?: string,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const turnState = context.turnState;
      if (!turnState) return;
      // A foreground run cannot outlive its tool call; background runs continue.
      yield* finalizeWorkflowRuns(context, state === "failed" ? "failed" : "stopped", true);
      yield* finalizeSubagentTasks(
        context,
        state === "completed" ? "completed" : state === "failed" ? "failed" : "stopped",
      );
      context.turnState = undefined;
      context.thinkItemIds.clear();

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

  const setResumeCursor = (context: PiSessionContext, sessionFile: string) => {
    context.session = {
      ...context.session,
      resumeCursor: { sessionFile, turnStarts: [...context.turnStarts] },
    };
  };

  const openTurn = (
    context: PiSessionContext,
    promptedAt: number | null = null,
  ): Effect.Effect<TurnId> =>
    Effect.gen(function* () {
      const turnId = TurnId.make(yield* nextUuid);
      const startedAt = yield* nowIso;
      context.turnStarts.push(promptedAt);
      if (context.turnStarts.length > MAX_TRACKED_PI_TURNS) context.turnStarts.shift();
      const sessionFile = readPiResumeState(context.session.resumeCursor)?.sessionFile;
      if (sessionFile !== undefined) setResumeCursor(context, sessionFile);
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

  // A background run's result, delivered by the plugin into the turn it triggers.
  // Settle the run from its file now, then show the delivered text in that turn.
  const emitWorkflowResult = Effect.fn("PiAdapter.emitWorkflowResult")(function* (
    context: PiSessionContext,
    content: string,
  ) {
    for (const run of context.workflowRuns.values()) {
      if (run.background && run.runId !== undefined && content.includes(run.runId)) {
        yield* pollWorkflowRun(context, run);
      }
    }
    const turnState = context.turnState;
    if (!turnState) return;
    const itemId = RuntimeItemId.make(`pi-workflow-result-${yield* nextUuid}`);
    const detail = content.split("\n", 1)[0]?.trim().slice(0, 500);
    const item = {
      provider: PROVIDER,
      providerInstanceId: boundInstanceId,
      threadId: context.session.threadId,
      turnId: turnState.turnId,
      itemId,
    };
    yield* offerRuntimeEvent({
      ...(yield* makeEventStamp()),
      ...item,
      type: "item.started",
      payload: {
        itemType: "collab_agent_tool_call",
        status: "inProgress",
        title: "Workflow result",
        ...(detail ? { detail } : {}),
      },
    });
    yield* offerRuntimeEvent({
      ...(yield* makeEventStamp()),
      ...item,
      type: "item.completed",
      payload: {
        itemType: "collab_agent_tool_call",
        status: "completed",
        title: "Workflow result",
        ...(detail ? { detail } : {}),
        data: { rawOutput: content },
      },
    });
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
          const delivered = piWorkflowResultMessage(event.message);
          if (delivered !== undefined) {
            yield* emitWorkflowResult(context, delivered);
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
          // A codemode script's own calls are listed on the script's row.
          if (isNestedToolEvent(event)) return;
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
          const isScript = event.toolName === PI_CODEMODE_TOOL;
          const detail = isScript
            ? piCodemodeDetail(piCodemodeCode(event.args))
            : summarizePiToolArgs(event.args);
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
          const toolEvent = {
            ...base,
            turnId: context.turnState.turnId,
            itemId,
            payload: {
              itemType,
              status: "inProgress" as const,
              title: event.toolName,
              ...(detail ? { detail } : {}),
              ...(argsObj
                ? {
                    data: {
                      item: { toolName: event.toolName, input: argsObj },
                      ...(isScript ? { script: piCodemodeScript({ args: argsObj }) } : {}),
                    },
                  }
                : {}),
            },
          };
          yield* offerRuntimeEvent({ ...toolEvent, type: "item.started" });
          // Upstream uses starts as lifecycle boundaries, and updates as visible rows.
          yield* offerRuntimeEvent({
            ...toolEvent,
            ...(yield* makeEventStamp()),
            type: "item.updated",
          });
          return;
        }

        case "tool_execution_update": {
          if (!context.turnState) return;
          if (context.thinkItemIds.has(event.toolCallId)) return;
          const partial = (event as { partialResult?: unknown }).partialResult;
          if (partial === undefined) return;
          const itemId = RuntimeItemId.make(event.toolCallId);
          const nested = isNestedToolEvent(event);

          // Each call a script makes republishes the script's call list.
          if (event.toolName === PI_CODEMODE_TOOL && !nested) {
            const stored = context.turnState.items.find((item) => item.id === itemId);
            const args = stored?.args ?? event.args;
            yield* offerRuntimeEvent({
              ...base,
              turnId: context.turnState.turnId,
              itemId,
              type: "item.updated",
              payload: {
                itemType: classifyPiToolItemType(event.toolName),
                status: "inProgress",
                title: event.toolName,
                ...(piCodemodeDetail(piCodemodeCode(args))
                  ? { detail: piCodemodeDetail(piCodemodeCode(args)) }
                  : {}),
                data: {
                  item: { toolName: event.toolName },
                  script: piCodemodeScript({
                    args,
                    details: (partial as { details?: unknown } | null)?.details,
                  }),
                },
              },
            });
            return;
          }

          // Foreground workflow runs stream their whole snapshot on every update.
          if (event.toolName === "workflow") {
            const snapshot = normalizePiWorkflowSnapshot(
              (partial as { details?: unknown } | null)?.details,
            );
            if (snapshot) {
              yield* emitWorkflow(
                context,
                foregroundWorkflowRun(context, event.toolCallId),
                snapshot,
              );
              return;
            }
          }

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

          if (nested) return;
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

          if (isNestedToolEvent(event)) return;
          const storedItem = context.turnState.items.find((item) => item.id === itemId);
          if (event.toolName === "workflow" || event.toolName === "workflow_control") {
            yield* trackWorkflowToolResult(context, event, storedItem?.args);
          }
          const isScript = event.toolName === PI_CODEMODE_TOOL;
          const detail = isScript
            ? piCodemodeDetail(piCodemodeCode(storedItem?.args))
            : summarizePiToolArgs(storedItem?.args);
          const argsObj =
            storedItem?.args && typeof storedItem.args === "object"
              ? (storedItem.args as Record<string, unknown>)
              : undefined;
          // Use upstream's output projection shapes. Retain one copy of the
          // result; the shared projector produces the bounded client preview.
          const fullResult = extractPiPartialResultText((event as { result?: unknown }).result);
          const result = isScript ? piCodemodeOutput(fullResult) : fullResult;
          const script = isScript
            ? piCodemodeScript({
                args: storedItem?.args,
                details: ((event as { result?: unknown }).result as { details?: unknown } | null)
                  ?.details,
                ...(fullResult !== undefined ? { output: fullResult } : {}),
                isError: event.isError,
              })
            : undefined;
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
                      ...(result !== undefined && itemType !== "mcp_tool_call"
                        ? { rawOutput: result }
                        : {}),
                      item: {
                        toolName: event.toolName,
                        ...(argsObj ? { input: argsObj } : {}),
                        ...(result !== undefined && itemType === "mcp_tool_call" ? { result } : {}),
                      },
                      ...(script ? { script } : {}),
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
          if (context.turnState && event.message.role === "assistant") {
            context.turnState.terminalState =
              event.message.stopReason === "error"
                ? "failed"
                : event.message.stopReason === "aborted"
                  ? "interrupted"
                  : "completed";
            context.turnState.errorMessage = event.message.errorMessage;
          }
          return;
        }

        case "agent_settled": {
          // agent_end can precede compaction, retries, or queued continuations.
          // Only Pi's session-level settled boundary finishes a T3 turn.
          if (context.turnState) {
            yield* completeTurn(
              context,
              context.turnState.terminalState ?? "completed",
              context.turnState.errorMessage,
            );
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
          if (event.aborted || !event.result) {
            if (event.errorMessage)
              yield* offerRuntimeEvent({
                ...base,
                type: "runtime.warning",
                payload: { message: event.errorMessage },
              });
            return;
          }
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
      case "barrier":
        return Deferred.succeed(message.done, undefined).pipe(Effect.asVoid);
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
        yield* completeTurn(
          context,
          opts?.exitKind === "error" ? "failed" : "interrupted",
          opts?.exitKind === "error" ? "Pi process exited unexpectedly." : "Session stopped.",
        );
      }

      yield* cancelPendingExtensionRequests(context);

      if (context.notificationFiber) yield* Fiber.interrupt(context.notificationFiber);

      // Pi's graceful process shutdown runs extension cleanup, including bg's
      // detached process groups. An RPC abort alone only stops the LLM turn.
      yield* context.transport.kill;
      yield* Effect.ignore(Scope.close(context.sessionScope, Exit.void));
      yield* finalizeBackgroundTasks(context, opts?.exitKind === "error" ? "failed" : "stopped");
      // Runs live in the Pi process; the plugin marks them paused on its next start.
      yield* finalizeWorkflowRuns(
        context,
        opts?.exitKind === "error" ? "failed" : "stopped",
        false,
      );

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
    // ProviderService already adds on-disk references for files and folded pastes.
    // Only images belong in Pi's native image-content payload.
    Effect.forEach(
      (attachments ?? []).filter((attachment) => attachment.type === "image"),
      (attachment) =>
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
    if (input.resumeCursor != null && !resumeState) {
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "startSession",
        detail:
          "The saved Pi continuation is invalid. Refusing to replace its conversation with an empty session.",
      });
    }
    if (
      resumeState &&
      !(yield* fileSystem.exists(resumeState.sessionFile).pipe(Effect.orElseSucceed(() => false)))
    ) {
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "startSession",
        detail: `The saved Pi session is missing: ${resumeState.sessionFile}. Refusing to replace its conversation with an empty session.`,
      });
    }
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
      onExit: Effect.gen(function* () {
        const live = sessions.get(threadId);
        if (!live || live.stopped) return;
        // Drain the final messages before closing the session. Run outside the
        // process scope so its exit callback never interrupts itself.
        if (live.notificationFiber) yield* Fiber.join(live.notificationFiber);
        yield* stopSessionInternal(live, { emitExitEvent: true, exitKind: "error" });
      }).pipe(Effect.forkIn(adapterScope), Effect.asVoid),
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
      subagentTasks: new Map(),
      backgroundTasks: new Map(),
      backgroundLaunches: new Set(),
      thinkItemIds: new Set(),
      workflowRuns: new Map(),
      workflowRunsDirectory: piWorkflowRunsDirectory(cwd, processEnv, hostPlatform, path),
      workflowPolling: false,
      stopped: false,
      currentModel: modelSelection?.model,
      currentContextWindow: undefined,
      compactsAutomatically: undefined,
      appliedThinkingLevel: thinkingLevel,
      turnStarts: resumeState?.turnStarts ?? [],
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
    if (!piResponseSucceeded(stateResponse, "get_state")) {
      yield* stopSessionInternal(context, { emitExitEvent: false });
      return yield* new ProviderAdapterProcessError({
        provider: PROVIDER,
        threadId,
        detail: "Pi did not complete its startup handshake.",
      });
    }
    const sessionFile = extractSessionFile(stateResponse) ?? resumeState?.sessionFile;
    if (sessionFile === undefined) {
      yield* stopSessionInternal(context, { emitExitEvent: false });
      return yield* new ProviderAdapterProcessError({
        provider: PROVIDER,
        threadId,
        detail:
          "Pi did not provide a persistent session file. Refusing to start a conversation that cannot be resumed.",
      });
    }
    const contextConfig = extractPiContextConfig(stateResponse);
    context.currentContextWindow = contextConfig.contextWindow;
    context.compactsAutomatically = contextConfig.compactsAutomatically;
    setResumeCursor(context, sessionFile);

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
    if (input.interactionMode === "plan") {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "sendTurn",
        issue:
          "Pi has no native plan mode. Switch to normal mode and use tool approvals to control edits.",
      });
    }

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

    // Pi stamps the prompt's user message after this instant.
    const turnId = context.turnState
      ? context.turnState.turnId
      : yield* openTurn(context, yield* Clock.currentTimeMillis);

    const command = buildPiTurnCommand({ isMidTurn, message: promptText, images });
    const response = yield* context.transport.request(
      command,
      `pi-prompt-${yield* nextUuid}`,
      15_000,
    );
    if (!piResponseSucceeded(response, command.type)) {
      const detail =
        response?.success === false ? response.error : "Pi did not acknowledge the message.";
      if (!isMidTurn) yield* completeTurn(context, "failed", detail);
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: command.type,
        detail,
      });
    }
    // Extension commands can handle a prompt without starting an agent run.
    // The notification receipt prevents a fast response from being settled
    // before its already-queued text/tool events are ingested.
    if (!isMidTurn && promptText.startsWith("/")) {
      const state = yield* context.transport.request(
        { type: "get_state" },
        `pi-command-state-${yield* nextUuid}`,
        PI_STATE_TIMEOUT_MS,
      );
      yield* context.transport.flushEvents;
      const data = piResponseData(state);
      if (
        data?.isStreaming === false &&
        data.isCompacting === false &&
        data.pendingMessageCount === 0
      ) {
        yield* completeTurn(context, "completed");
      }
    }

    return {
      threadId: context.session.threadId,
      turnId,
      ...(context.session.resumeCursor !== undefined
        ? { resumeCursor: context.session.resumeCursor }
        : {}),
    };
  });

  const compactThread = Effect.fn("PiAdapter.compactThread")(function* (threadId: ThreadId) {
    const context = yield* requireSession(threadId);
    const response = yield* context.transport.request(
      { type: "compact" },
      `pi-compact-${yield* nextUuid}`,
      180_000,
    );
    if (!piResponseSucceeded(response, "compact")) {
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "compact",
        detail: response?.success === false ? response.error : "Pi compaction timed out.",
      });
    }
  });

  const interruptTurn: PiAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
    function* (threadId, turnId) {
      const context = yield* requireSession(threadId);
      if (turnId !== undefined && context.turnState?.turnId !== turnId) return;
      if (hasBackgroundWork(context)) {
        // Stop-everything must stop the process owning bg, not just its current
        // turn. The persisted resume cursor keeps the conversation resumable.
        yield* stopSessionInternal(context, { emitExitEvent: true });
        return;
      }
      yield* cancelPendingExtensionRequests(context);
      const response = yield* context.transport.request(
        { type: "abort" },
        `pi-abort-${yield* nextUuid}`,
        PI_STATE_TIMEOUT_MS,
      );
      if (!piResponseSucceeded(response, "abort")) {
        yield* stopSessionInternal(context, { emitExitEvent: true });
        return;
      }
      yield* context.transport.flushEvents;
      if (context.turnState) yield* completeTurn(context, "interrupted", "Turn interrupted.");
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
      // Durable history belongs to Pi's session file and T3's event store.
      turns: context.turnState
        ? [{ id: context.turnState.turnId, items: [...context.turnState.items] }]
        : [],
    };
  });

  // Pi forks before a native user message into a new session file, leaving
  // the original intact. Steers and extension prompts are user messages too,
  // so the turn's opening prompt is located by when T3 sent it.
  const rollbackThread: PiAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
    function* (threadId, numTurns) {
      const context = yield* requireSession(threadId);
      const fail = (detail: string) =>
        new ProviderAdapterRequestError({ provider: PROVIDER, method: "fork", detail });
      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "numTurns must be an integer >= 1.",
        });
      }
      if (context.turnState || hasBackgroundWork(context)) {
        return yield* fail("Wait for the running Pi turn and background tasks before rewinding.");
      }
      const retained = context.turnStarts.length - numTurns;
      const startedAt = retained >= 0 ? context.turnStarts[retained] : undefined;
      if (startedAt === undefined || startedAt === null) {
        return yield* fail(
          "Pi can only rewind to a message sent from T3 Code since rewind support was added. Start a new thread instead.",
        );
      }
      const nextStartedAt =
        context.turnStarts.slice(retained + 1).find((value): value is number => value !== null) ??
        undefined;

      const entriesResponse = yield* context.transport.request(
        { type: "get_entries" },
        `pi-get-entries-${yield* nextUuid}`,
        60_000,
      );
      const entriesData = piResponseData(entriesResponse);
      if (!piResponseSucceeded(entriesResponse, "get_entries") || !entriesData) {
        return yield* fail("Pi did not return its session history.");
      }
      const entryId = findPiTurnStartEntryId({
        entries: Array.isArray(entriesData["entries"]) ? entriesData["entries"] : [],
        leafId: typeof entriesData["leafId"] === "string" ? entriesData["leafId"] : null,
        startedAt,
        nextStartedAt,
      });
      if (entryId === undefined) {
        return yield* fail("The Pi message that started this turn is no longer in the session.");
      }

      const forkResponse = yield* context.transport.request(
        { type: "fork", entryId },
        `pi-fork-${yield* nextUuid}`,
        60_000,
      );
      if (!piResponseSucceeded(forkResponse, "fork")) {
        return yield* fail(
          forkResponse?.success === false ? forkResponse.error : "Pi did not fork the session.",
        );
      }
      if (piResponseData(forkResponse)?.["cancelled"] === true) {
        return yield* fail("A Pi extension cancelled the rewind.");
      }

      const stateResponse = yield* context.transport.request(
        { type: "get_state" },
        `pi-get-state-${yield* nextUuid}`,
        PI_STATE_TIMEOUT_MS,
      );
      const sessionFile = extractSessionFile(stateResponse);
      if (sessionFile === undefined) {
        // The process now runs an unknown session; the saved cursor still
        // points at the untouched original, so restart from that on next use.
        yield* stopSessionInternal(context, { emitExitEvent: true });
        return yield* fail("Pi did not report the rewound session file.");
      }
      context.turnStarts = context.turnStarts.slice(0, retained);
      // The forked runtime restores its own settings; reapply ours next turn.
      context.appliedThinkingLevel = undefined;
      setResumeCursor(context, sessionFile);
      context.session = { ...context.session, updatedAt: yield* nowIso };
      return { threadId, turns: [] };
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
    capabilities: {
      sessionModelSwitch: "in-session" as const,
    },
    startSession,
    sendTurn,
    compaction: { type: "native", start: compactThread },
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
