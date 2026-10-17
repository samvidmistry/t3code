import { describe, expect, it } from "vite-plus/test";
import { isNestedToolEvent, piCodemodeScript } from "./codemode.ts";

describe("Pi codemode scripts", () => {
  it("extracts live calls and final output metadata", () => {
    const args = { code: 'const text = await tools.read({ path: "a.ts" });\nconsole.log(text);' };
    expect(
      piCodemodeScript({
        args,
        details: { calls: [{ name: "read", args: 'path: "a.ts"', status: "running" }] },
      }),
    ).toEqual({
      code: args.code,
      calls: [{ name: "read", args: 'path: "a.ts"', status: "running" }],
    });
    expect(
      piCodemodeScript({
        args,
        details: {
          calls: [
            { name: "read", args: 'path: "a.ts"', status: "ok", durationMs: 12.6, cost: 0.01 },
            { name: "bash", status: "error", error: "command exited 1" },
            { name: "write", status: "cancelled" },
          ],
          fullOutputPath: "/tmp/script-output.txt",
        },
        output: "Script failed\nWall time 1.25 seconds\nOutput:\nfirst\nsecond\n",
      }),
    ).toEqual({
      code: args.code,
      calls: [
        { name: "read", args: 'path: "a.ts"', status: "ok", durationMs: 13, cost: 0.01 },
        { name: "bash", args: "", status: "error", error: "command exited 1" },
        { name: "write", args: "", status: "cancelled" },
      ],
      failed: true,
      wallTimeMs: 1_250,
      fullOutputPath: "/tmp/script-output.txt",
      outputPreview: "first\nsecond",
      outputLines: 2,
    });
  });

  it("bounds every string and retains only the latest 256 calls", () => {
    const script = piCodemodeScript({
      args: { code: "c".repeat(100_000), extra: "not forwarded" },
      details: {
        calls: Array.from({ length: 300 }, (_, index) => ({
          name: `${index}:` + "n".repeat(300),
          args: "a".repeat(1_000),
          status: "error",
          error: "e".repeat(1_000),
        })),
        fullOutputPath: "/".repeat(10_000),
      },
      output: "x".repeat(5_000),
    });
    expect(script.code).toHaveLength(20_000);
    expect(script.calls).toHaveLength(256);
    expect(script.calls[0]?.name).toMatch(/^44:/);
    expect(script.calls[255]?.name).toMatch(/^299:/);
    expect(script.omittedCalls).toBe(44);
    expect(
      script.calls.every(
        (call) =>
          call.name.length === 200 && call.args.length === 300 && call.error?.length === 500,
      ),
    ).toBe(true);
    expect(script.fullOutputPath).toHaveLength(4_096);
    expect(script.outputPreview).toHaveLength(2_000);
    const lines = Array.from({ length: 12 }, (_, index) => `line ${index}`);
    expect(piCodemodeScript({ args: {}, output: lines.join("\n") })).toMatchObject({
      outputPreview: lines.slice(0, 8).join("\n"),
      outputLines: 12,
    });
  });

  it("ignores malformed calls and invalid optional metrics", () => {
    expect(
      piCodemodeScript({
        args: null,
        isError: true,
        details: {
          calls: [
            null,
            {},
            { name: "read", status: "unknown" },
            { name: " ", status: "ok" },
            { name: "valid", status: "ok", args: {}, durationMs: -1, cost: Number.NaN },
          ],
        },
      }),
    ).toEqual({ code: "", calls: [{ name: "valid", args: "", status: "ok" }], failed: true });
    expect(piCodemodeScript({ args: [], details: { calls: {} }, output: "" })).toEqual({
      code: "",
      calls: [],
    });
    expect(
      piCodemodeScript({ args: {}, output: "Script completed\nWall time 0 seconds\nOutput:\n" }),
    ).toEqual({
      code: "",
      calls: [],
      wallTimeMs: 0,
    });
  });

  it("recognizes only tool events with a parent call id as nested", () => {
    expect(isNestedToolEvent({ parentToolCallId: "script" })).toBe(true);
    expect(isNestedToolEvent({ parentToolCallId: "" })).toBe(false);
    expect(isNestedToolEvent({ parentToolCallId: 42 })).toBe(false);
    expect(isNestedToolEvent({})).toBe(false);
  });
});
