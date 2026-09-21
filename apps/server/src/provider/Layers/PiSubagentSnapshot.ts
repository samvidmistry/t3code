/**
 * Defensive normalizer for the bundled Pi `subagent` extension tool result shape.
 *
 * The Pi subagent extension (examples/extensions/subagent) streams structured
 * snapshots through `tool_execution_update.partialResult` and the final
 * `tool_execution_end.result`, both shaped like:
 *
 *   { content?: Array<{ type: "text"; text: string }>,
 *     details?: {
 *       mode: "single" | "parallel" | "chain",
 *       results: Array<SingleResult> } }
 *
 * where `SingleResult` carries per-child identity (agent/task/step), the child
 * transcript (`messages`), token usage, and terminal signals (exitCode /
 * stopReason / errorMessage).
 *
 * Everything here is defensive: malformed / unknown payloads normalize to
 * `undefined` (a no-op for the caller) rather than throwing.
 */

const MAX_DESCRIPTION_LENGTH = 2_000;

export type SubagentMode = "single" | "parallel" | "chain";

/** Terminal status for a child task, aligned with the canonical task event. */
export type SubagentChildStatus = "completed" | "failed" | "stopped";

export interface NormalizedSubagentUsage {
  readonly input?: number;
  readonly output?: number;
  readonly cacheRead?: number;
  readonly cacheWrite?: number;
  readonly cost?: number;
  readonly contextTokens?: number;
  readonly turns?: number;
}

/**
 * The latest observable event within a child's transcript. Modeled explicitly so
 * a streaming progress update can reflect the newest activity (a tool call or a
 * tool result), not just the last assistant text block.
 */
export type SubagentChildActivity =
  | { readonly kind: "text"; readonly text: string }
  | { readonly kind: "toolCall"; readonly toolName: string }
  | { readonly kind: "toolResult"; readonly toolName: string; readonly isError: boolean };

export interface NormalizedSubagentChild {
  /** Stable index within the parent tool call's `results` array. */
  readonly index: number;
  readonly agent: string;
  readonly task: string;
  readonly step: number | undefined;
  /** Final assistant text for the child (may be empty while running). */
  readonly assistantText: string;
  readonly lastToolName: string | undefined;
  readonly toolCallCount: number;
  readonly toolResultCount: number;
  /** Newest observable transcript activity, or `undefined` when none yet. */
  readonly latestActivity: SubagentChildActivity | undefined;
  readonly usage: NormalizedSubagentUsage | undefined;
  readonly exitCode: number | undefined;
  readonly stopReason: string | undefined;
  readonly errorMessage: string | undefined;
  /** True for a not-yet-dispatched parallel placeholder (`exitCode === -1`). */
  readonly running: boolean;
}

export interface NormalizedSubagentSnapshot {
  readonly mode: SubagentMode;
  readonly children: ReadonlyArray<NormalizedSubagentChild>;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function asFiniteNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function asNonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function normalizeMode(value: unknown): SubagentMode | undefined {
  return value === "single" || value === "parallel" || value === "chain" ? value : undefined;
}

function normalizeUsage(value: unknown): NormalizedSubagentUsage | undefined {
  const usage = asRecord(value);
  if (!usage) return undefined;
  const input = asFiniteNumber(usage["input"]);
  const output = asFiniteNumber(usage["output"]);
  const cacheRead = asFiniteNumber(usage["cacheRead"]);
  const cacheWrite = asFiniteNumber(usage["cacheWrite"]);
  const cost = asFiniteNumber(usage["cost"]);
  const contextTokens = asFiniteNumber(usage["contextTokens"]);
  const turns = asFiniteNumber(usage["turns"]);
  const entries: NormalizedSubagentUsage = {
    ...(input !== undefined ? { input } : {}),
    ...(output !== undefined ? { output } : {}),
    ...(cacheRead !== undefined ? { cacheRead } : {}),
    ...(cacheWrite !== undefined ? { cacheWrite } : {}),
    ...(cost !== undefined ? { cost } : {}),
    ...(contextTokens !== undefined ? { contextTokens } : {}),
    ...(turns !== undefined ? { turns } : {}),
  };
  return Object.keys(entries).length > 0 ? entries : undefined;
}

/**
 * Walk a child's messages, extracting the final assistant text plus assistant
 * tool-call and `toolResult` activity. The child transcript can interleave
 * assistant messages (text + `toolCall` parts) with `toolResult` messages
 * (`role: "toolResult"`), because the bundled extension emits an update after
 * both `message_end` and `tool_result_end`.
 *
 * No transcript is retained: only the counts, the last tool name, the final
 * assistant text, and the single newest observable activity are returned.
 */
function summarizeChildMessages(messages: unknown): {
  readonly assistantText: string;
  readonly lastToolName: string | undefined;
  readonly toolCallCount: number;
  readonly toolResultCount: number;
  readonly latestActivity: SubagentChildActivity | undefined;
} {
  if (!Array.isArray(messages)) {
    return {
      assistantText: "",
      lastToolName: undefined,
      toolCallCount: 0,
      toolResultCount: 0,
      latestActivity: undefined,
    };
  }

  let lastToolName: string | undefined;
  let toolCallCount = 0;
  let toolResultCount = 0;
  let latestActivity: SubagentChildActivity | undefined;
  // Maps an assistant tool call id to its tool name, so a later `toolResult`
  // that omits `toolName` can still be attributed to the right tool.
  const toolNamesByCallId = new Map<string, string>();
  const assistantTexts: Array<string> = [];

  for (const rawMessage of messages) {
    const message = asRecord(rawMessage);
    if (!message) continue;
    const role = message["role"];

    if (role === "assistant") {
      const content = message["content"];
      if (!Array.isArray(content)) continue;
      let messageText = "";
      for (const rawPart of content) {
        const part = asRecord(rawPart);
        if (!part) continue;
        if (part["type"] === "text" && typeof part["text"] === "string") {
          messageText += part["text"];
          latestActivity = { kind: "text", text: messageText };
        } else if (part["type"] === "toolCall") {
          toolCallCount += 1;
          const name = asNonEmptyString(part["name"]);
          if (name) {
            lastToolName = name;
            const callId = asNonEmptyString(part["id"]);
            if (callId) toolNamesByCallId.set(callId, name);
            latestActivity = { kind: "toolCall", toolName: name };
          }
        }
      }
      if (messageText.length > 0) assistantTexts.push(messageText);
      continue;
    }

    if (role === "toolResult") {
      toolResultCount += 1;
      const callId = asNonEmptyString(message["toolCallId"]);
      const resolvedName =
        asNonEmptyString(message["toolName"]) ??
        (callId ? toolNamesByCallId.get(callId) : undefined) ??
        lastToolName;
      const isError = message["isError"] === true;
      latestActivity = { kind: "toolResult", toolName: resolvedName ?? "tool", isError };
    }
  }

  // The extension shows the last assistant text block as the child's output.
  const assistantText =
    assistantTexts.length > 0 ? (assistantTexts[assistantTexts.length - 1] ?? "") : "";
  return { assistantText, lastToolName, toolCallCount, toolResultCount, latestActivity };
}

function normalizeChild(value: unknown, index: number): NormalizedSubagentChild | undefined {
  const result = asRecord(value);
  if (!result) return undefined;

  const agent = asNonEmptyString(result["agent"]);
  const task = asNonEmptyString(result["task"]);
  // A child without agent+task identity is not something we can track meaningfully.
  if (!agent && !task) return undefined;

  const { assistantText, lastToolName, toolCallCount, toolResultCount, latestActivity } =
    summarizeChildMessages(result["messages"]);
  const exitCode = asFiniteNumber(result["exitCode"]);

  return {
    index,
    agent: agent ?? `agent-${index + 1}`,
    task: task ?? "",
    step: asFiniteNumber(result["step"]),
    assistantText,
    lastToolName,
    toolCallCount,
    toolResultCount,
    latestActivity,
    usage: normalizeUsage(result["usage"]),
    exitCode,
    stopReason: asNonEmptyString(result["stopReason"]),
    errorMessage: asNonEmptyString(result["errorMessage"]),
    running: exitCode === -1,
  };
}

/**
 * Normalize a Pi subagent `partialResult` / `result` payload into a stable,
 * transcript-free snapshot. Returns `undefined` for anything that is not a
 * recognizable subagent result (a no-op for the caller).
 */
export function normalizePiSubagentResult(
  payload: unknown,
): NormalizedSubagentSnapshot | undefined {
  const record = asRecord(payload);
  if (!record) return undefined;
  const details = asRecord(record["details"]);
  if (!details) return undefined;
  const mode = normalizeMode(details["mode"]);
  if (!mode) return undefined;
  const rawResults = details["results"];
  if (!Array.isArray(rawResults)) return undefined;

  const children: Array<NormalizedSubagentChild> = [];
  for (let index = 0; index < rawResults.length; index += 1) {
    const child = normalizeChild(rawResults[index], index);
    if (child) children.push(child);
  }
  // A valid (mode + results) envelope with no usable children is still a
  // recognized snapshot — return it so the caller can no-op cleanly rather
  // than misclassify the payload as generic text.
  return { mode, children };
}

/** Stable, globally-unique task id for a child within its parent tool call. */
export function subagentChildTaskId(toolCallId: string, child: NormalizedSubagentChild): string {
  return `pi-subagent:${toolCallId}:${child.index}`;
}

/**
 * Bounded FNV-1a (32-bit) hash rendered as hex. Used to fold arbitrarily long
 * observable text into a fixed-width token so the dedupe fingerprint never
 * retains (or grows with) the child transcript.
 */
function hashString(value: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    // FNV prime multiply, kept in 32-bit space via Math.imul.
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16);
}

function activityKey(activity: SubagentChildActivity | undefined): string {
  if (!activity) return "";
  if (activity.kind === "text") return `t:${hashString(activity.text)}`;
  if (activity.kind === "toolCall") return `c:${activity.toolName}`;
  return `r:${activity.toolName}:${activity.isError ? 1 : 0}`;
}

function usageKey(usage: NormalizedSubagentUsage | undefined): string {
  if (!usage) return "";
  return [
    usage.input ?? "",
    usage.output ?? "",
    usage.cacheRead ?? "",
    usage.cacheWrite ?? "",
    usage.cost ?? "",
    usage.contextTokens ?? "",
    usage.turns ?? "",
  ].join(",");
}

/**
 * Compact, bounded fingerprint used to dedupe cumulative snapshots without
 * retaining the child transcript. It folds in every observable dimension —
 * assistant text content (hashed, so same-length-but-changed text still
 * differs), tool-call / tool-result activity, token usage, and terminal
 * signals — so a progress event is emitted whenever any of them changes.
 */
export function subagentChildFingerprint(child: NormalizedSubagentChild): string {
  return [
    hashString(child.assistantText),
    child.toolCallCount,
    child.toolResultCount,
    child.lastToolName ?? "",
    activityKey(child.latestActivity),
    usageKey(child.usage),
    child.running ? 1 : 0,
    child.exitCode ?? "",
    child.stopReason ?? "",
    child.errorMessage ?? "",
  ].join("|");
}

/**
 * True when a child has produced anything observable yet (text, tool activity,
 * usage, or a real terminal exit code). A queued parallel placeholder
 * (`exitCode === -1`, empty transcript, no usage) is not yet observable, so the
 * caller should defer its `task.started` until it actually begins.
 */
export function subagentChildHasActivity(child: NormalizedSubagentChild): boolean {
  return (
    child.assistantText.length > 0 ||
    child.latestActivity !== undefined ||
    child.toolCallCount > 0 ||
    child.toolResultCount > 0 ||
    child.usage !== undefined ||
    (child.exitCode !== undefined && child.exitCode !== -1)
  );
}

/** Map a child's terminal signals onto the canonical task-completed status. */
export function subagentChildTerminalStatus(child: NormalizedSubagentChild): SubagentChildStatus {
  if (child.stopReason === "aborted") return "stopped";
  if (child.stopReason === "error") return "failed";
  if (child.exitCode !== undefined && child.exitCode !== 0) return "failed";
  return "completed";
}

function truncate(text: string): string {
  return text.length <= MAX_DESCRIPTION_LENGTH
    ? text
    : `${text.slice(0, MAX_DESCRIPTION_LENGTH - 1)}…`;
}

/**
 * Human-readable progress description reflecting the child's **latest**
 * observable event (assistant text, a tool call, or a tool result) — so a later
 * tool-call/result update does not display stale earlier assistant text.
 * Returns `undefined` when there is nothing observable yet (the caller then
 * skips the progress event, since the progress payload requires a non-empty
 * description).
 */
export function subagentChildProgressDescription(
  child: NormalizedSubagentChild,
): string | undefined {
  const activity = child.latestActivity;
  if (activity) {
    if (activity.kind === "text") {
      const text = activity.text.trim();
      if (text.length > 0) return truncate(text);
    } else if (activity.kind === "toolCall") {
      return `Using ${activity.toolName}`;
    } else {
      return activity.isError ? `${activity.toolName} failed` : `Ran ${activity.toolName}`;
    }
  }
  const text = child.assistantText.trim();
  if (text.length > 0) return truncate(text);
  if (child.lastToolName) return `Using ${child.lastToolName}`;
  return undefined;
}

/**
 * Terminal summary for a completed child. Prefers the final assistant text
 * (the child's actual output), then any error message, then the latest
 * observable activity. Returns `undefined` when nothing is observable.
 */
export function subagentChildCompletionSummary(child: NormalizedSubagentChild): string | undefined {
  const text = child.assistantText.trim();
  if (text.length > 0) return truncate(text);
  if (child.errorMessage) return truncate(child.errorMessage.trim());
  return subagentChildProgressDescription(child);
}
