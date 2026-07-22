import { describe, expect, it } from "@effect/vitest";
import type { ProviderApprovalDecision } from "@t3tools/contracts";

import type { AgentSessionEvent } from "./PiRpcClient.ts";
import {
  buildPiApprovalResponse,
  buildPiUserInputResponse,
  classifyPiApprovalRequestType,
  classifyPiToolItemType,
  extractPiPartialResultText,
  isPiApprovalConfirmed,
  normalizePiTokenUsage,
  parseNumberedList,
  summarizePiToolArgs,
} from "./PiAdapter.ts";

describe("classifyPiToolItemType", () => {
  it("maps shell / exec tools to command execution", () => {
    expect(classifyPiToolItemType("bash")).toBe("command_execution");
    expect(classifyPiToolItemType("run_shell_command")).toBe("command_execution");
    expect(classifyPiToolItemType("terminal_exec")).toBe("command_execution");
  });

  it("maps write / edit / patch tools to file changes", () => {
    expect(classifyPiToolItemType("write_file")).toBe("file_change");
    expect(classifyPiToolItemType("apply_patch")).toBe("file_change");
    expect(classifyPiToolItemType("edit")).toBe("file_change");
  });

  it("classifies agent, mcp, search, and image tools", () => {
    expect(classifyPiToolItemType("subagent")).toBe("collab_agent_tool_call");
    expect(classifyPiToolItemType("task")).toBe("collab_agent_tool_call");
    expect(classifyPiToolItemType("mcp_call")).toBe("mcp_tool_call");
    expect(classifyPiToolItemType("web_search")).toBe("web_search");
    expect(classifyPiToolItemType("view_image")).toBe("image_view");
  });

  it("falls back to a dynamic tool call for unknown tools", () => {
    expect(classifyPiToolItemType("something_else")).toBe("dynamic_tool_call");
  });
});

describe("classifyPiApprovalRequestType", () => {
  it("derives the approval request type from the tool hint", () => {
    expect(classifyPiApprovalRequestType("bash")).toBe("command_execution_approval");
    expect(classifyPiApprovalRequestType("write_file")).toBe("file_change_approval");
  });

  it("maps non-command/non-file tools to dynamic_tool_call (a surfaced approval)", () => {
    expect(classifyPiApprovalRequestType("web_search")).toBe("dynamic_tool_call");
    expect(classifyPiApprovalRequestType("mcp__server__tool")).toBe("dynamic_tool_call");
    expect(classifyPiApprovalRequestType("some_unknown_tool")).toBe("dynamic_tool_call");
  });
});

describe("normalizePiTokenUsage", () => {
  it("maps Pi assistant usage to a context-window snapshot", () => {
    const snapshot = normalizePiTokenUsage(
      {
        type: "turn_end",
        message: {
          role: "assistant",
          usage: {
            input: 1_000,
            output: 200,
            cacheRead: 8_000,
            cacheWrite: 300,
            reasoning: 150,
            totalTokens: 9_500,
          },
        },
        toolResults: [],
      } as unknown as AgentSessionEvent,
      { contextWindow: 200_000, compactsAutomatically: true },
    );

    expect(snapshot).toEqual({
      usedTokens: 9_500,
      lastUsedTokens: 9_500,
      maxTokens: 200_000,
      inputTokens: 1_000,
      lastInputTokens: 1_000,
      cachedInputTokens: 8_300,
      lastCachedInputTokens: 8_300,
      outputTokens: 200,
      lastOutputTokens: 200,
      reasoningOutputTokens: 150,
      lastReasoningOutputTokens: 150,
      compactsAutomatically: true,
    });
  });

  it("falls back to component totals and ignores events without usage", () => {
    expect(
      normalizePiTokenUsage({
        type: "turn_end",
        message: {
          role: "assistant",
          usage: { input: 10, output: 5, cacheRead: 20, cacheWrite: 2, totalTokens: 0 },
        },
        toolResults: [],
      } as unknown as AgentSessionEvent),
    ).toMatchObject({ usedTokens: 37, lastUsedTokens: 37 });
    expect(normalizePiTokenUsage({ type: "agent_start" })).toBeUndefined();
  });
});

describe("summarizePiToolArgs", () => {
  it("prefers the command, then path, then pattern fields", () => {
    expect(summarizePiToolArgs({ command: "ls -la" })).toBe("ls -la");
    expect(summarizePiToolArgs({ file_path: "/tmp/x.ts" })).toBe("/tmp/x.ts");
    expect(summarizePiToolArgs({ query: "find TODOs" })).toBe("find TODOs");
  });

  it("serializes other objects and ignores non-objects", () => {
    expect(summarizePiToolArgs({ foo: "bar" })).toBe('{"foo":"bar"}');
    expect(summarizePiToolArgs(undefined)).toBeUndefined();
    expect(summarizePiToolArgs("string")).toBeUndefined();
  });
});

describe("parseNumberedList", () => {
  it("parses a title + numbered options", () => {
    expect(parseNumberedList("Pick one\n1. Alpha\n2. Beta")).toEqual({
      title: "Pick one",
      items: [
        { index: 1, label: "Alpha" },
        { index: 2, label: "Beta" },
      ],
    });
  });

  it("returns null when fewer than two options are present", () => {
    expect(parseNumberedList("Just a title\n1. Only")).toBeNull();
    expect(parseNumberedList("No options here")).toBeNull();
  });
});

describe("isPiApprovalConfirmed / buildPiApprovalResponse", () => {
  it("confirms accept and acceptForSession, rejects everything else", () => {
    expect(isPiApprovalConfirmed("accept")).toBe(true);
    expect(isPiApprovalConfirmed("acceptForSession")).toBe(true);
    expect(isPiApprovalConfirmed("decline")).toBe(false);
    expect(isPiApprovalConfirmed("cancel")).toBe(false);
  });

  it("round-trips a confirm request into an extension_ui_response", () => {
    expect(classifyPiApprovalRequestType("bash")).toBe("command_execution_approval");
    expect(buildPiApprovalResponse("ui-42", "accept")).toEqual({
      type: "extension_ui_response",
      id: "ui-42",
      confirmed: true,
    });
    const decline: ProviderApprovalDecision = "decline";
    expect(buildPiApprovalResponse("ui-42", decline)).toEqual({
      type: "extension_ui_response",
      id: "ui-42",
      confirmed: false,
    });
  });
});

describe("buildPiUserInputResponse", () => {
  it("echoes a plain string answer for select / input requests", () => {
    expect(
      buildPiUserInputResponse(
        { piId: "ui-1", questionId: "q1", method: "select" },
        { q1: "Option A" },
      ),
    ).toEqual({ type: "extension_ui_response", id: "ui-1", value: "Option A" });
  });

  it("maps numbered-list selections back to 1-based comma-joined indices", () => {
    expect(
      buildPiUserInputResponse(
        {
          piId: "ui-2",
          questionId: "q2",
          method: "input",
          numberedOptions: ["Alpha", "Beta", "Gamma"],
        },
        { q2: ["Alpha", "Gamma"] },
      ),
    ).toEqual({ type: "extension_ui_response", id: "ui-2", value: "1,3" });
  });

  it("handles a single numbered selection provided as a string", () => {
    expect(
      buildPiUserInputResponse(
        {
          piId: "ui-3",
          questionId: "q3",
          method: "input",
          numberedOptions: ["Alpha", "Beta"],
        },
        { q3: "Beta" },
      ),
    ).toEqual({ type: "extension_ui_response", id: "ui-3", value: "2" });
  });

  it("returns an empty value when the answer is missing", () => {
    expect(
      buildPiUserInputResponse({ piId: "ui-4", questionId: "q4", method: "editor" }, {}),
    ).toEqual({ type: "extension_ui_response", id: "ui-4", value: "" });
  });
});

describe("extractPiPartialResultText", () => {
  it("returns raw strings unchanged", () => {
    expect(extractPiPartialResultText("hello")).toBe("hello");
  });

  it("extracts text parts from a structured AgentToolResult", () => {
    expect(
      extractPiPartialResultText({
        content: [
          { type: "text", text: "line 1\n" },
          { type: "text", text: "line 2" },
        ],
        details: { mode: "single" },
      }),
    ).toBe("line 1\nline 2");
  });

  it("falls back to text / output fields", () => {
    expect(extractPiPartialResultText({ text: "from text" })).toBe("from text");
    expect(extractPiPartialResultText({ output: "from output" })).toBe("from output");
  });

  it("serializes structured objects instead of producing [object Object]", () => {
    const result = extractPiPartialResultText({ foo: "bar", n: 1 });
    expect(result).toBe('{"foo":"bar","n":1}');
    expect(result).not.toContain("[object Object]");
  });

  it("returns undefined for empty / nullish payloads", () => {
    expect(extractPiPartialResultText(undefined)).toBeUndefined();
    expect(extractPiPartialResultText(null)).toBeUndefined();
    expect(extractPiPartialResultText({})).toBeUndefined();
  });
});
