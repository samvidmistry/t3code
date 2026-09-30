/**
 * Projection of `@quintinshaw/pi-dynamic-workflows` runs into T3 workflow tasks.
 *
 * Foreground runs stream a snapshot in their `workflow` tool updates. Background
 * runs report nothing over RPC after they start, so their state comes from the
 * run file the plugin persists (at run start, after each finished agent, and on
 * every status change). Both shapes are read defensively: the plugin is a
 * third-party package and its file format is not a stable API.
 */
import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";

import type { RuntimeTaskStatus } from "@t3tools/contracts";
import type * as Path from "effect/Path";

export interface PiWorkflowAgent {
  /** The plugin's 1-based agent id, stable for the run. */
  readonly index: number;
  readonly label: string;
  readonly phase: string | undefined;
  readonly status: RuntimeTaskStatus;
  readonly model: string | undefined;
  readonly error: string | undefined;
  readonly tokens: number | undefined;
}

export type PiWorkflowRunStatus = "running" | "completed" | "failed" | "stopped" | "paused";

export interface PiWorkflowSnapshot {
  readonly name: string | undefined;
  readonly runId: string | undefined;
  /** Only run files carry a status; tool snapshots are live until the tool ends. */
  readonly status: PiWorkflowRunStatus | undefined;
  readonly phases: ReadonlyArray<string>;
  readonly currentPhase: string | undefined;
  readonly agents: ReadonlyArray<PiWorkflowAgent>;
  readonly tokens: number | undefined;
}

// Match the Claude workflow caps; a fan-out of hundreds is summarized by the counts.
const MAX_AGENTS = 100;
const MAX_PHASES = 50;

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function text(value: unknown, max = 500): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed.slice(0, max) : undefined;
}

function count(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? Math.round(value)
    : undefined;
}

function agentStatus(value: unknown): RuntimeTaskStatus | undefined {
  switch (value) {
    case "queued":
      return "pending";
    case "running":
      return "running";
    case "done":
      return "completed";
    case "error":
      return "failed";
    case "skipped":
      return "cancelled";
    default:
      return undefined;
  }
}

function runStatus(value: unknown): PiWorkflowRunStatus | undefined {
  switch (value) {
    case "pending":
    case "running":
      return "running";
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "aborted":
      return "stopped";
    // A paused run needs a resume to continue, which re-tracks it.
    case "paused":
      return "paused";
    default:
      return undefined;
  }
}

/**
 * Normalizes a live tool snapshot (`WorkflowSnapshot`) or a persisted run file
 * (`PersistedRunState`). Returns undefined for anything else, such as a
 * background start result, which carries only the run id.
 */
export function normalizePiWorkflowSnapshot(value: unknown): PiWorkflowSnapshot | undefined {
  const source = record(value);
  if (!source || !Array.isArray(source.agents) || !Array.isArray(source.phases)) return undefined;
  const agents: PiWorkflowAgent[] = [];
  for (const raw of source.agents) {
    const agent = record(raw);
    const index = count(agent?.id);
    const status = agentStatus(agent?.status);
    if (!agent || index === undefined || status === undefined) continue;
    const usage = record(agent.tokenUsage);
    agents.push({
      index,
      label: text(agent.label, 200) ?? `agent ${index}`,
      phase: text(agent.phase, 200),
      status,
      model: text(agent.model, 200),
      error: text(agent.error, 2_000),
      tokens: count(usage?.total) ?? count(agent.tokens),
    });
    if (agents.length >= MAX_AGENTS) break;
  }
  const phases = source.phases.flatMap((phase) => text(phase, 200) ?? []).slice(0, MAX_PHASES);
  return {
    name: text(source.workflowName, 200) ?? text(source.name, 200),
    runId: text(source.runId, 200),
    status: runStatus(source.status),
    phases,
    currentPhase: text(source.currentPhase, 200),
    agents,
    tokens: count(record(source.tokenUsage)?.total),
  };
}

/** The run a `workflow` or `workflow_control` tool result started or resumed in the background. */
export function piWorkflowBackgroundRunId(toolName: string, result: unknown): string | undefined {
  const details = record(record(result)?.details);
  if (!details) return undefined;
  if (toolName === "workflow") {
    return details.background === true ? text(details.runId, 200) : undefined;
  }
  if (toolName === "workflow_control" && details.action === "resume") {
    return details.result === "resumed" ? text(record(details.run)?.runId, 200) : undefined;
  }
  return undefined;
}

/** The workflow name a `workflow` tool call declares, before its run file exists. */
export function piWorkflowCallName(args: unknown): string | undefined {
  const input = record(args);
  if (!input) return undefined;
  const named = text(input.name, 200);
  if (named) return named;
  const script = typeof input.script === "string" ? input.script : "";
  return text(/\bname\s*:\s*["'`]([^"'`\n]{1,200})["'`]/u.exec(script)?.[1]);
}

/**
 * Where the plugin keeps a project's runs: `~/.pi/workflows/projects/<key>/runs`,
 * keyed by the Pi process's working directory. Mirrors the plugin's
 * workflowProjectPaths(), using the home directory the Pi process sees.
 */
export function piWorkflowRunsDirectory(
  cwd: string,
  environment: NodeJS.ProcessEnv,
  platform: NodeJS.Platform,
  path: Pick<Path.Path, "basename" | "join" | "resolve">,
): string {
  const home =
    (platform === "win32" ? environment.USERPROFILE : environment.HOME) || NodeOS.homedir();
  const projectPath = path.resolve(cwd);
  const slug =
    (path.basename(projectPath) || "project")
      .toLowerCase()
      .replace(/[^a-z0-9._-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 48) || "project";
  const hash = NodeCrypto.createHash("sha256").update(projectPath).digest("hex").slice(0, 12);
  return path.join(home, ".pi", "workflows", "projects", `${slug}-${hash}`, "runs");
}

/** Plugin run ids are generated slugs; refuse anything that could leave the runs directory. */
export function isPiWorkflowRunId(runId: string): boolean {
  return /^[A-Za-z0-9._-]{1,200}$/u.test(runId) && !runId.startsWith(".");
}

/** One-line coordinator progress, e.g. "Critique · 3/8 agents done, 2 running". */
export function piWorkflowProgressSummary(snapshot: PiWorkflowSnapshot): string {
  const done = snapshot.agents.filter(
    (agent) => agent.status === "completed" || agent.status === "failed",
  ).length;
  const running = snapshot.agents.filter((agent) => agent.status === "running").length;
  const failed = snapshot.agents.filter((agent) => agent.status === "failed").length;
  const agents =
    snapshot.agents.length === 0
      ? "starting"
      : `${done}/${snapshot.agents.length} agents done${running > 0 ? `, ${running} running` : ""}${
          failed > 0 ? `, ${failed} failed` : ""
        }`;
  return snapshot.currentPhase ? `${snapshot.currentPhase} · ${agents}` : agents;
}
