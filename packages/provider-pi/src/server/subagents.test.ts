import { describe, expect, it } from "@effect/vitest";
import {
  makePiSubagentTracker,
  normalizePiSubagentResult,
  subagentChildFingerprint,
  subagentChildProgressDescription,
  subagentChildTerminalStatus,
} from "./subagents.ts";

const assistant = (content: ReadonlyArray<unknown>) => ({ role: "assistant", content });
const call = (name: string) => assistant([{ type: "toolCall", id: "call", name }]);
const payload = (overrides: Record<string, unknown> = {}) => ({
  details: {
    mode: "parallel",
    results: [{ agent: "scout", task: "map files", exitCode: -1, messages: [], ...overrides }],
  },
});
const child = (overrides: Record<string, unknown> = {}) =>
  normalizePiSubagentResult(payload(overrides))!.children[0]!;

describe("Pi subagent snapshots", () => {
  it("rejects malformed snapshots and skips anonymous children", () => {
    for (const value of [
      null,
      [],
      { details: { mode: "invalid", results: [] } },
      { details: { mode: "single" } },
    ]) {
      expect(normalizePiSubagentResult(value)).toBeUndefined();
    }
    expect(
      normalizePiSubagentResult({
        details: { mode: "chain", results: [null, {}, { task: "plan" }] },
      })?.children,
    ).toMatchObject([{ index: 2, agent: "agent-3", task: "plan" }]);
  });

  it("uses the latest tool call, attributed tool result, failure, or text as progress", () => {
    expect(
      subagentChildProgressDescription(
        child({ messages: [assistant([{ type: "text", text: "looking" }]), call("bash")] }),
      ),
    ).toBe("Using bash");
    expect(
      subagentChildProgressDescription(
        child({ messages: [call("read"), { role: "toolResult", toolCallId: "call" }] }),
      ),
    ).toBe("Ran read");
    expect(
      subagentChildProgressDescription(
        child({
          messages: [call("bash"), { role: "toolResult", toolCallId: "call", isError: true }],
        }),
      ),
    ).toBe("bash failed");
    expect(
      subagentChildProgressDescription(
        child({ messages: [call("read"), assistant([{ type: "text", text: "repo mapped" }])] }),
      ),
    ).toBe("repo mapped");
  });

  it("bounds text fingerprints and distinguishes equal-length text and usage changes", () => {
    const a = child({ messages: [assistant([{ type: "text", text: "abc" }])] });
    const b = child({ messages: [assistant([{ type: "text", text: "def" }])] });
    expect(subagentChildFingerprint(a)).not.toBe(subagentChildFingerprint(b));
    expect(subagentChildFingerprint(a)).not.toBe(subagentChildFingerprint({ ...a, usage: [100] }));
    expect(
      subagentChildFingerprint(
        child({ messages: [assistant([{ type: "text", text: "x".repeat(100_000) }])] }),
      ).length,
    ).toBeLessThan(500);
  });

  it.each([
    [{ exitCode: 0 }, "completed"],
    [{ exitCode: 1 }, "failed"],
    [{ exitCode: 0, stopReason: "error" }, "failed"],
    [{ exitCode: 1, stopReason: "aborted" }, "interrupted"],
  ] as const)("maps terminal signals %j to %s", (signals, expected) => {
    expect(subagentChildTerminalStatus(child(signals))).toBe(expected);
  });

  it("defers parallel placeholders, dedupes unchanged children, and emits terminal transitions", () => {
    const tracker = makePiSubagentTracker();
    expect(tracker.update("parent", payload(), false)).toEqual([]);
    const active = payload({ messages: [call("bash")], stopReason: "toolUse" });
    expect(tracker.update("parent", active, false)).toMatchObject([
      { nativeTaskId: "parent:subagent:0", status: "running", progress: "Using bash" },
    ]);
    expect(tracker.update("parent", active, false)).toEqual([]);
    const finished = payload({
      exitCode: 0,
      messages: [assistant([{ type: "text", text: "done" }])],
    });
    expect(tracker.update("parent", finished, true)).toMatchObject([
      { status: "completed", result: "done" },
    ]);
    expect(tracker.update("parent", finished, true)).toEqual([]);
    expect(tracker.finalize("interrupted")).toEqual([]);
  });

  it("finalizes only open children, once, when the owning turn ends", () => {
    const tracker = makePiSubagentTracker();
    tracker.update("parent", payload({ messages: [call("read")] }), false);
    expect(tracker.finalize("interrupted")).toMatchObject([
      { status: "interrupted", progress: "Using read" },
    ]);
    expect(tracker.finalize("failed")).toEqual([]);
    expect(tracker.update("parent", payload({ messages: [call("read")] }), false)).toEqual([]);
  });
});
