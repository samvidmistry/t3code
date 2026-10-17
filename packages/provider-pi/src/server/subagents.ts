import { piRecordField as field, piRecordNumber as number, piRecordString as text } from "./rpc.ts";

const MAX_PROGRESS_LENGTH = 2_000;
type Activity =
  | { readonly kind: "text"; readonly text: string }
  | { readonly kind: "toolCall"; readonly name: string }
  | { readonly kind: "toolResult"; readonly name: string; readonly isError: boolean };

export interface PiSubagentChild {
  readonly index: number;
  readonly agent: string;
  readonly task: string;
  readonly model: string | null;
  readonly assistantText: string;
  readonly activity: Activity | undefined;
  readonly toolCalls: number;
  readonly toolResults: number;
  readonly usage: ReadonlyArray<number | undefined>;
  readonly exitCode: number | undefined;
  readonly stopReason: string | undefined;
  readonly error: string | undefined;
  readonly finished: boolean;
}

export function normalizePiSubagentResult(payload: unknown) {
  const details = field(payload, "details");
  const mode = text(details, "mode");
  const results = field(details, "results");
  if (!Array.isArray(results) || !["single", "parallel", "chain"].includes(mode ?? "")) return;
  const children: PiSubagentChild[] = [];
  for (const [index, result] of results.entries()) {
    const agent = text(result, "agent");
    const task = text(result, "task");
    if (!agent?.trim() && !task?.trim()) continue;
    const messages = field(result, "messages");
    let assistantText = "";
    let activity: Activity | undefined;
    let lastToolName: string | undefined;
    let toolCalls = 0;
    let toolResults = 0;
    const toolNames = new Map<string, string>();
    for (const message of Array.isArray(messages) ? messages : []) {
      if (text(message, "role") === "assistant") {
        const content = field(message, "content");
        let messageText = "";
        for (const part of Array.isArray(content) ? content : []) {
          if (text(part, "type") === "text") {
            messageText += text(part, "text") ?? "";
            activity = { kind: "text", text: messageText };
          } else if (text(part, "type") === "toolCall") {
            toolCalls++;
            const name = text(part, "name");
            if (!name?.trim()) continue;
            lastToolName = name;
            const id = text(part, "id");
            if (id) toolNames.set(id, name);
            activity = { kind: "toolCall", name };
          }
        }
        if (messageText.length > 0) assistantText = messageText;
      } else if (text(message, "role") === "toolResult") {
        toolResults++;
        activity = {
          kind: "toolResult",
          name:
            text(message, "toolName") ??
            toolNames.get(text(message, "toolCallId") ?? "") ??
            lastToolName ??
            "tool",
          isError: field(message, "isError") === true,
        };
      }
    }
    const usage = field(result, "usage");
    children.push({
      index,
      agent: agent ?? `agent-${index + 1}`,
      task: task ?? "",
      model: text(result, "model") ?? null,
      assistantText,
      activity,
      toolCalls,
      toolResults,
      usage: ["input", "output", "cacheRead", "cacheWrite", "cost", "contextTokens", "turns"].map(
        (key) => number(usage, key),
      ),
      exitCode: number(result, "exitCode"),
      stopReason: text(result, "stopReason"),
      error: text(result, "errorMessage") || text(result, "stderr"),
      finished: field(result, "finished") === true,
    });
  }
  return { mode, children };
}

export function subagentChildHasActivity(child: PiSubagentChild) {
  return (
    child.activity !== undefined ||
    child.usage.some((value) => value !== undefined) ||
    (child.exitCode !== undefined && child.exitCode !== -1) ||
    child.error !== undefined ||
    child.stopReason === "aborted" ||
    child.stopReason === "error"
  );
}

function bounded(value: string) {
  return value.length <= MAX_PROGRESS_LENGTH
    ? value
    : `${value.slice(0, MAX_PROGRESS_LENGTH - 1)}…`;
}

export function subagentChildProgressDescription(child: PiSubagentChild) {
  const activity = child.activity;
  if (activity?.kind === "toolCall") return `Using ${activity.name}`;
  if (activity?.kind === "toolResult")
    return activity.isError ? `${activity.name} failed` : `Ran ${activity.name}`;
  const output = (activity?.text || child.assistantText).trim();
  return output.length > 0 ? bounded(output) : undefined;
}

export function subagentChildTerminalStatus(child: PiSubagentChild) {
  if (child.stopReason === "aborted") return "interrupted";
  if (child.stopReason === "error" || (child.exitCode !== undefined && child.exitCode !== 0))
    return "failed";
  return "completed";
}

function hash(value: string) {
  let valueHash = 0x811c9dc5;
  for (let index = 0; index < value.length; index++) {
    valueHash = Math.imul(valueHash ^ value.charCodeAt(index), 0x01000193);
  }
  return (valueHash >>> 0).toString(16);
}

export function subagentChildFingerprint(child: PiSubagentChild) {
  return JSON.stringify([
    child.agent,
    child.task,
    child.model,
    hash(child.assistantText),
    child.activity?.kind === "text" ? hash(child.activity.text) : child.activity,
    child.toolCalls,
    child.toolResults,
    child.usage,
    child.exitCode,
    child.stopReason,
    hash(child.error ?? ""),
    child.finished,
  ]);
}

export interface PiSubagentUpdate {
  readonly nativeTaskId: string;
  readonly toolCallId: string;
  readonly child: Pick<PiSubagentChild, "agent" | "task" | "model">;
  readonly status: "running" | "completed" | "failed" | "interrupted";
  readonly progress: string | undefined;
  readonly result: string | null;
}

export function makePiSubagentTracker() {
  const states = new Map<string, { fingerprint: string; update: PiSubagentUpdate }>();
  return {
    update(
      toolCallId: string,
      payload: unknown,
      completed: boolean,
    ): ReadonlyArray<PiSubagentUpdate> {
      const updates: PiSubagentUpdate[] = [];
      for (const child of normalizePiSubagentResult(payload)?.children ?? []) {
        const nativeTaskId = `${toolCallId}:subagent:${child.index}`;
        const previous = states.get(nativeTaskId);
        if (previous?.update.status !== undefined && previous.update.status !== "running") continue;
        if (!previous && !subagentChildHasActivity(child)) continue;
        const finished = completed || child.finished;
        const status = finished ? subagentChildTerminalStatus(child) : "running";
        const fingerprint = `${status}:${subagentChildFingerprint(child)}`;
        if (previous?.fingerprint === fingerprint) continue;
        const update: PiSubagentUpdate = {
          nativeTaskId,
          toolCallId,
          status,
          child: { agent: child.agent, task: child.task, model: child.model },
          progress: subagentChildProgressDescription(child),
          result: finished
            ? ((
                child.assistantText.trim() ||
                child.error ||
                subagentChildProgressDescription(child)
              )?.slice(0, 10_000) ?? null)
            : null,
        };
        states.set(nativeTaskId, { fingerprint, update });
        updates.push(update);
      }
      return updates;
    },
    finalize(status: "failed" | "interrupted"): ReadonlyArray<PiSubagentUpdate> {
      const updates: PiSubagentUpdate[] = [];
      for (const state of states.values()) {
        if (state.update.status !== "running") continue;
        state.update = { ...state.update, status };
        updates.push(state.update);
      }
      return updates;
    },
  };
}
