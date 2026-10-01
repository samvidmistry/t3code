/**
 * Pi's built-in `codemode` tool runs a JavaScript script that calls other tools.
 *
 * Pi reports each call a script makes as its own tool event (marked with the
 * script's call as `parentToolCallId`) and also lists them in the script's
 * `details.calls`. Clients get one script row with that list, never the calls
 * as rows of their own, since the model never issued them.
 */

export const PI_CODEMODE_TOOL = "codemode";

export type PiScriptCallStatus = "running" | "ok" | "error" | "cancelled";

export interface PiScriptCall {
  readonly name: string;
  readonly args: string;
  readonly status: PiScriptCallStatus;
  readonly durationMs?: number;
  readonly error?: string;
  readonly cost?: number;
}

/** `data.script` on a codemode tool row; the activity projection keeps it, bounded. */
export interface PiScriptData {
  readonly code: string;
  readonly calls: ReadonlyArray<PiScriptCall>;
  readonly failed?: true;
  readonly wallTimeMs?: number;
  readonly fullOutputPath?: string;
  /** The first lines of the output; the full text stays in the tool result. */
  readonly outputPreview?: string;
  readonly outputLines?: number;
}

const OUTPUT_PREVIEW_LINES = 8;
const OUTPUT_PREVIEW_CHARS = 2_000;

const SCRIPT_HEADER = /^Script (completed|failed)\nWall time ([\d.]+) seconds\nOutput:\n?/u;

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function finite(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}

export function piCodemodeCode(args: unknown): string {
  const code = record(args)?.code;
  return typeof code === "string" ? code : "";
}

/** The script's first statement, as the row's one-line detail for clients without script support. */
export function piCodemodeDetail(code: string): string | undefined {
  const line = code
    .split("\n")
    .map((entry) => entry.trim())
    .find((entry) => entry.length > 0 && !entry.startsWith("//"));
  return line?.slice(0, 400);
}

function calls(details: unknown): PiScriptCall[] {
  const list = record(details)?.calls;
  if (!Array.isArray(list)) return [];
  return list.flatMap((raw): PiScriptCall[] => {
    const call = record(raw);
    const name = typeof call?.name === "string" ? call.name : undefined;
    const status = call?.status;
    if (
      !call ||
      !name ||
      (status !== "running" && status !== "ok" && status !== "error" && status !== "cancelled")
    ) {
      return [];
    }
    const durationMs = finite(call.durationMs);
    const cost = finite(call.cost);
    return [
      {
        name,
        args: typeof call.args === "string" ? call.args : "",
        status,
        ...(durationMs !== undefined ? { durationMs: Math.round(durationMs) } : {}),
        ...(typeof call.error === "string" && call.error ? { error: call.error } : {}),
        ...(cost ? { cost } : {}),
      },
    ];
  });
}

/**
 * Script data from the call's arguments and its live or final `details`.
 * `output` is the result text, whose header gives the wall time.
 */
export function piCodemodeScript(input: {
  readonly args: unknown;
  readonly details?: unknown;
  readonly output?: string;
  readonly isError?: boolean;
}): PiScriptData {
  const header = input.output ? SCRIPT_HEADER.exec(input.output) : null;
  const wallSeconds = header ? Number(header[2]) : Number.NaN;
  const fullOutputPath = record(input.details)?.fullOutputPath;
  const output = piCodemodeOutput(input.output);
  const lines = output?.split("\n") ?? [];
  return {
    code: piCodemodeCode(input.args),
    calls: calls(input.details),
    ...(input.isError || header?.[1] === "failed" ? { failed: true as const } : {}),
    ...(Number.isFinite(wallSeconds) ? { wallTimeMs: Math.round(wallSeconds * 1000) } : {}),
    ...(typeof fullOutputPath === "string" && fullOutputPath ? { fullOutputPath } : {}),
    ...(output
      ? {
          outputPreview: lines
            .slice(0, OUTPUT_PREVIEW_LINES)
            .join("\n")
            .slice(0, OUTPUT_PREVIEW_CHARS),
          outputLines: lines.length,
        }
      : {}),
  };
}

/** The script's output without Pi's "Script completed / Wall time / Output:" header. */
export function piCodemodeOutput(output: string | undefined): string | undefined {
  if (output === undefined) return undefined;
  const body = output.replace(SCRIPT_HEADER, "").trim();
  return body.length > 0 ? body : undefined;
}
