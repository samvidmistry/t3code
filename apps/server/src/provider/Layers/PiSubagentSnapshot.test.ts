import { describe, expect, it } from "@effect/vitest";

import {
  normalizePiSubagentResult,
  subagentChildCompletionSummary,
  subagentChildFingerprint,
  subagentChildHasActivity,
  subagentChildProgressDescription,
  subagentChildTaskId,
  subagentChildTerminalStatus,
  type NormalizedSubagentChild,
} from "./PiSubagentSnapshot.ts";

const assistantMessage = (text: string, extra: Record<string, unknown> = {}) => ({
  role: "assistant",
  content: [{ type: "text", text }],
  ...extra,
});

const toolCallMessage = (name: string, id?: string) => ({
  role: "assistant",
  content: [{ type: "toolCall", ...(id ? { id } : {}), name, arguments: {} }],
});

const toolResultMessage = (overrides: Record<string, unknown> = {}) => ({
  role: "toolResult",
  toolCallId: "call-1",
  toolName: "read",
  content: [{ type: "text", text: "file contents" }],
  isError: false,
  ...overrides,
});

const singleResult = (overrides: Record<string, unknown> = {}) => ({
  agent: "worker",
  agentSource: "user",
  task: "do the thing",
  exitCode: 0,
  messages: [assistantMessage("all done")],
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

describe("normalizePiSubagentResult", () => {
  it("returns undefined for malformed / unknown payloads", () => {
    expect(normalizePiSubagentResult(undefined)).toBeUndefined();
    expect(normalizePiSubagentResult(null)).toBeUndefined();
    expect(normalizePiSubagentResult("hello")).toBeUndefined();
    expect(normalizePiSubagentResult(42)).toBeUndefined();
    expect(normalizePiSubagentResult([])).toBeUndefined();
    // recognizable envelope but missing details
    expect(normalizePiSubagentResult({ content: [{ type: "text", text: "hi" }] })).toBeUndefined();
    // details without a valid mode
    expect(normalizePiSubagentResult({ details: { mode: "bogus", results: [] } })).toBeUndefined();
    // details without a results array
    expect(normalizePiSubagentResult({ details: { mode: "single" } })).toBeUndefined();
  });

  it("normalizes a single-mode snapshot", () => {
    const snapshot = normalizePiSubagentResult({
      content: [{ type: "text", text: "all done" }],
      details: { mode: "single", results: [singleResult()] },
    });
    expect(snapshot).toBeDefined();
    expect(snapshot?.mode).toBe("single");
    expect(snapshot?.children).toHaveLength(1);
    const child = snapshot?.children[0];
    expect(child?.agent).toBe("worker");
    expect(child?.task).toBe("do the thing");
    expect(child?.assistantText).toBe("all done");
    expect(child?.exitCode).toBe(0);
    expect(child?.usage).toMatchObject({ input: 100, output: 20 });
    expect(child?.running).toBe(false);
  });

  it("normalizes a parallel-mode snapshot with running placeholders", () => {
    const snapshot = normalizePiSubagentResult({
      details: {
        mode: "parallel",
        results: [
          singleResult({ agent: "a", messages: [assistantMessage("first")] }),
          { agent: "b", task: "task b", exitCode: -1, messages: [], usage: {} },
        ],
      },
    });
    expect(snapshot?.mode).toBe("parallel");
    expect(snapshot?.children).toHaveLength(2);
    expect(snapshot?.children[0]?.running).toBe(false);
    expect(snapshot?.children[1]?.running).toBe(true);
    expect(snapshot?.children[1]?.index).toBe(1);
  });

  it("normalizes a chain-mode snapshot preserving step + tool activity", () => {
    const snapshot = normalizePiSubagentResult({
      details: {
        mode: "chain",
        results: [
          singleResult({ agent: "scout", step: 1 }),
          {
            agent: "planner",
            task: "plan",
            exitCode: 0,
            step: 2,
            messages: [
              toolCallMessage("read"),
              toolCallMessage("grep"),
              assistantMessage("plan ready"),
            ],
          },
        ],
      },
    });
    expect(snapshot?.mode).toBe("chain");
    const planner = snapshot?.children[1];
    expect(planner?.step).toBe(2);
    expect(planner?.toolCallCount).toBe(2);
    expect(planner?.lastToolName).toBe("grep");
    expect(planner?.assistantText).toBe("plan ready");
    // latest observable activity is the trailing assistant text
    expect(planner?.latestActivity).toEqual({ kind: "text", text: "plan ready" });
  });

  it("parses toolResult activity and attributes it to the calling tool", () => {
    const snapshot = normalizePiSubagentResult({
      details: {
        mode: "single",
        results: [
          singleResult({
            messages: [
              assistantMessage("let me look"),
              toolCallMessage("read", "call-1"),
              toolResultMessage({ toolCallId: "call-1", toolName: undefined }),
            ],
          }),
        ],
      },
    });
    const child = snapshot?.children[0];
    expect(child?.toolCallCount).toBe(1);
    expect(child?.toolResultCount).toBe(1);
    // toolName omitted on the result -> resolved from the prior toolCall id
    expect(child?.latestActivity).toEqual({
      kind: "toolResult",
      toolName: "read",
      isError: false,
    });
  });

  it("marks an errored toolResult activity", () => {
    const snapshot = normalizePiSubagentResult({
      details: {
        mode: "single",
        results: [
          singleResult({
            messages: [
              toolCallMessage("bash", "c9"),
              toolResultMessage({ toolName: "bash", isError: true }),
            ],
          }),
        ],
      },
    });
    expect(snapshot?.children[0]?.latestActivity).toEqual({
      kind: "toolResult",
      toolName: "bash",
      isError: true,
    });
  });

  it("skips result entries without agent or task identity", () => {
    const snapshot = normalizePiSubagentResult({
      details: {
        mode: "single",
        results: [{ exitCode: 0, messages: [] }, singleResult()],
      },
    });
    expect(snapshot?.children).toHaveLength(1);
    expect(snapshot?.children[0]?.agent).toBe("worker");
  });

  it("returns an empty-children snapshot for a valid but empty envelope", () => {
    const snapshot = normalizePiSubagentResult({ details: { mode: "single", results: [] } });
    expect(snapshot).toBeDefined();
    expect(snapshot?.children).toHaveLength(0);
  });
});

describe("subagentChildTaskId", () => {
  it("is stable per parent tool call + child index", () => {
    const child = { index: 2 } as NormalizedSubagentChild;
    expect(subagentChildTaskId("tc-1", child)).toBe("pi-subagent:tc-1:2");
    expect(subagentChildTaskId("tc-1", child)).toBe(subagentChildTaskId("tc-1", child));
    expect(subagentChildTaskId("tc-2", child)).not.toBe(subagentChildTaskId("tc-1", child));
  });
});

describe("subagentChildFingerprint", () => {
  const base: NormalizedSubagentChild = {
    index: 0,
    agent: "worker",
    task: "t",
    step: undefined,
    assistantText: "hello",
    lastToolName: "read",
    toolCallCount: 1,
    toolResultCount: 0,
    latestActivity: { kind: "text", text: "hello" },
    usage: undefined,
    exitCode: undefined,
    stopReason: undefined,
    errorMessage: undefined,
    running: false,
  };

  it("is stable for identical observable state", () => {
    expect(subagentChildFingerprint(base)).toBe(subagentChildFingerprint({ ...base }));
  });

  it("changes for same-length but different assistant text", () => {
    // "world" is the same length as "hello" -> a length-only cursor would miss it.
    expect(
      subagentChildFingerprint({
        ...base,
        assistantText: "world",
        latestActivity: { kind: "text", text: "world" },
      }),
    ).not.toBe(subagentChildFingerprint(base));
  });

  it("changes for new tool-call and tool-result activity", () => {
    expect(subagentChildFingerprint({ ...base, toolCallCount: 2 })).not.toBe(
      subagentChildFingerprint(base),
    );
    expect(subagentChildFingerprint({ ...base, lastToolName: "grep" })).not.toBe(
      subagentChildFingerprint(base),
    );
    expect(
      subagentChildFingerprint({
        ...base,
        toolResultCount: 1,
        latestActivity: { kind: "toolResult", toolName: "read", isError: false },
      }),
    ).not.toBe(subagentChildFingerprint(base));
  });

  it("changes for usage-only updates (token streaming)", () => {
    expect(subagentChildFingerprint({ ...base, usage: { input: 10, output: 5 } })).not.toBe(
      subagentChildFingerprint(base),
    );
    expect(subagentChildFingerprint({ ...base, usage: { input: 10, output: 5 } })).not.toBe(
      subagentChildFingerprint({ ...base, usage: { input: 10, output: 6 } }),
    );
  });
});

describe("subagentChildHasActivity", () => {
  const placeholder: NormalizedSubagentChild = {
    index: 0,
    agent: "worker",
    task: "t",
    step: undefined,
    assistantText: "",
    lastToolName: undefined,
    toolCallCount: 0,
    toolResultCount: 0,
    latestActivity: undefined,
    usage: undefined,
    exitCode: -1,
    stopReason: undefined,
    errorMessage: undefined,
    running: true,
  };

  it("is false for a queued parallel placeholder", () => {
    expect(subagentChildHasActivity(placeholder)).toBe(false);
  });

  it("is true once any text, tool, usage, or terminal exit is observable", () => {
    expect(subagentChildHasActivity({ ...placeholder, assistantText: "hi" })).toBe(true);
    expect(
      subagentChildHasActivity({
        ...placeholder,
        latestActivity: { kind: "toolCall", toolName: "read" },
      }),
    ).toBe(true);
    expect(subagentChildHasActivity({ ...placeholder, usage: { input: 1 } })).toBe(true);
    expect(subagentChildHasActivity({ ...placeholder, exitCode: 0, running: false })).toBe(true);
  });
});

describe("subagentChildTerminalStatus", () => {
  const child = (overrides: Partial<NormalizedSubagentChild>): NormalizedSubagentChild => ({
    index: 0,
    agent: "worker",
    task: "t",
    step: undefined,
    assistantText: "",
    lastToolName: undefined,
    toolCallCount: 0,
    toolResultCount: 0,
    latestActivity: undefined,
    usage: undefined,
    exitCode: 0,
    stopReason: undefined,
    errorMessage: undefined,
    running: false,
    ...overrides,
  });

  it("maps successful runs to completed", () => {
    expect(subagentChildTerminalStatus(child({ exitCode: 0 }))).toBe("completed");
  });
  it("maps aborted runs to stopped", () => {
    expect(subagentChildTerminalStatus(child({ stopReason: "aborted", exitCode: 1 }))).toBe(
      "stopped",
    );
  });
  it("maps error stopReason and nonzero exit codes to failed", () => {
    expect(subagentChildTerminalStatus(child({ stopReason: "error" }))).toBe("failed");
    expect(subagentChildTerminalStatus(child({ exitCode: 2 }))).toBe("failed");
  });
});

describe("subagentChildProgressDescription", () => {
  const child = (overrides: Partial<NormalizedSubagentChild>): NormalizedSubagentChild => ({
    index: 0,
    agent: "worker",
    task: "t",
    step: undefined,
    assistantText: "",
    lastToolName: undefined,
    toolCallCount: 0,
    toolResultCount: 0,
    latestActivity: undefined,
    usage: undefined,
    exitCode: undefined,
    stopReason: undefined,
    errorMessage: undefined,
    running: false,
    ...overrides,
  });

  it("prefers the latest activity text", () => {
    expect(
      subagentChildProgressDescription(
        child({ latestActivity: { kind: "text", text: "  working on it  " } }),
      ),
    ).toBe("working on it");
  });
  it("describes the latest tool call", () => {
    expect(
      subagentChildProgressDescription(
        child({ assistantText: "stale", latestActivity: { kind: "toolCall", toolName: "bash" } }),
      ),
    ).toBe("Using bash");
  });
  it("describes the latest tool result, not stale assistant text", () => {
    expect(
      subagentChildProgressDescription(
        child({
          assistantText: "earlier text",
          latestActivity: { kind: "toolResult", toolName: "read", isError: false },
        }),
      ),
    ).toBe("Ran read");
    expect(
      subagentChildProgressDescription(
        child({ latestActivity: { kind: "toolResult", toolName: "bash", isError: true } }),
      ),
    ).toBe("bash failed");
  });
  it("falls back to assistant text then tool name", () => {
    expect(subagentChildProgressDescription(child({ assistantText: "done" }))).toBe("done");
    expect(subagentChildProgressDescription(child({ lastToolName: "grep" }))).toBe("Using grep");
  });
  it("returns undefined when nothing is observable", () => {
    expect(subagentChildProgressDescription(child({}))).toBeUndefined();
  });
});

describe("subagentChildCompletionSummary", () => {
  const child = (overrides: Partial<NormalizedSubagentChild>): NormalizedSubagentChild => ({
    index: 0,
    agent: "worker",
    task: "t",
    step: undefined,
    assistantText: "",
    lastToolName: undefined,
    toolCallCount: 0,
    toolResultCount: 0,
    latestActivity: undefined,
    usage: undefined,
    exitCode: 0,
    stopReason: undefined,
    errorMessage: undefined,
    running: false,
    ...overrides,
  });

  it("prefers final assistant text over the latest tool activity", () => {
    expect(
      subagentChildCompletionSummary(
        child({
          assistantText: "final answer",
          latestActivity: { kind: "toolResult", toolName: "read", isError: false },
        }),
      ),
    ).toBe("final answer");
  });
  it("falls back to the error message then activity", () => {
    expect(subagentChildCompletionSummary(child({ errorMessage: "boom" }))).toBe("boom");
    expect(
      subagentChildCompletionSummary(
        child({ latestActivity: { kind: "toolCall", toolName: "bash" } }),
      ),
    ).toBe("Using bash");
  });
});
