import type { OrchestrationV2TurnItem } from "@t3tools/contracts";
import { piRecordField, piRecordNumber, piRecordString } from "./rpc.ts";

type ToolScript = NonNullable<Extract<OrchestrationV2TurnItem, { type: "dynamic_tool" }>["script"]>;

const SCRIPT_CODE_CHARS = 20_000;
const SCRIPT_CALLS = 256;
const OUTPUT_PREVIEW_LINES = 8;
const OUTPUT_PREVIEW_CHARS = 2_000;
const SCRIPT_HEADER = /^Script (completed|failed)\nWall time ([\d.]+) seconds\nOutput:\n?/u;

export function isNestedToolEvent(event: unknown): boolean {
  return Boolean(piRecordString(event, "parentToolCallId"));
}

function nonNegative(value: number | undefined): number | undefined {
  return value !== undefined && value >= 0 ? value : undefined;
}

function scriptCalls(details: unknown): Pick<ToolScript, "calls" | "omittedCalls"> {
  const rawCalls = piRecordField(details, "calls");
  if (!Array.isArray(rawCalls)) return { calls: [] };
  const calls: Array<ToolScript["calls"][number]> = [];
  // Keep the latest calls without projecting an arbitrarily large tool history.
  for (let index = Math.max(0, rawCalls.length - SCRIPT_CALLS); index < rawCalls.length; index++) {
    const call: unknown = rawCalls[index];
    const name = piRecordString(call, "name")?.trim().slice(0, 200);
    const status = piRecordString(call, "status");
    if (
      !name ||
      (status !== "running" && status !== "ok" && status !== "error" && status !== "cancelled")
    )
      continue;
    const durationMs = Math.round(piRecordNumber(call, "durationMs") ?? Number.NaN);
    const cost = nonNegative(piRecordNumber(call, "cost"));
    const error = piRecordString(call, "error")?.slice(0, 500);
    calls.push({
      name,
      args: piRecordString(call, "args")?.slice(0, 300) ?? "",
      status,
      ...(Number.isSafeInteger(durationMs) && durationMs >= 0 ? { durationMs } : {}),
      ...(cost === undefined ? {} : { cost }),
      ...(error ? { error } : {}),
    });
  }
  return {
    calls,
    ...(rawCalls.length > SCRIPT_CALLS ? { omittedCalls: rawCalls.length - SCRIPT_CALLS } : {}),
  };
}

export function piCodemodeScript(input: {
  readonly args: unknown;
  readonly details?: unknown;
  readonly output?: string;
  readonly isError?: boolean;
}): ToolScript {
  const header = input.output ? SCRIPT_HEADER.exec(input.output) : null;
  const wallTimeMs = header ? Math.round(Number(header[2]) * 1_000) : Number.NaN;
  const output = (header ? input.output?.slice(header[0].length) : input.output)?.trim();
  let outputLines = 0;
  let previewEnd = Math.min(output?.length ?? 0, OUTPUT_PREVIEW_CHARS);
  if (output) {
    outputLines = 1;
    for (let index = output.indexOf("\n"); index !== -1; index = output.indexOf("\n", index + 1)) {
      if (outputLines === OUTPUT_PREVIEW_LINES) previewEnd = Math.min(previewEnd, index);
      outputLines++;
    }
  }
  const fullOutputPath = piRecordString(input.details, "fullOutputPath")?.slice(0, 4_096);
  return {
    code: piRecordString(input.args, "code")?.slice(0, SCRIPT_CODE_CHARS) ?? "",
    ...scriptCalls(input.details),
    ...(input.isError || header?.[1] === "failed" ? { failed: true } : {}),
    ...(Number.isSafeInteger(wallTimeMs) && wallTimeMs >= 0 ? { wallTimeMs } : {}),
    ...(fullOutputPath ? { fullOutputPath } : {}),
    ...(output ? { outputPreview: output.slice(0, previewEnd), outputLines } : {}),
  };
}
