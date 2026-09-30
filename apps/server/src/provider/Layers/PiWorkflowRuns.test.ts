// @effect-diagnostics nodeBuiltinImport:off - compare against the plugin's own path derivation.
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";

import { describe, expect, it } from "@effect/vitest";

import {
  isPiWorkflowRunId,
  normalizePiWorkflowSnapshot,
  piWorkflowBackgroundRunId,
  piWorkflowCallName,
  piWorkflowProgressSummary,
  piWorkflowRunsDirectory,
} from "./PiWorkflowRuns.ts";

describe("PiWorkflowRuns", () => {
  it("reads live tool snapshots and persisted run files alike", () => {
    const agents = [
      { id: 1, label: "recon", phase: "Recon", status: "done", tokens: 90, prompt: "long" },
      {
        id: 2,
        label: "critic",
        phase: "Critique",
        status: "running",
        model: "github-copilot/claude-opus-5",
        tokenUsage: { total: 12 },
      },
      { id: 3, label: "late", status: "queued" },
      { id: 4, label: "broken", status: "error", error: "timed out" },
      { label: "no id", status: "done" },
    ];
    const live = normalizePiWorkflowSnapshot({
      name: "review",
      phases: ["Recon", "Critique"],
      currentPhase: "Critique",
      agents,
      logs: [],
    });
    expect(live).toMatchObject({ name: "review", status: undefined, currentPhase: "Critique" });
    expect(live?.agents.map((agent) => [agent.index, agent.status, agent.tokens])).toEqual([
      [1, "completed", 90],
      [2, "running", 12],
      [3, "pending", undefined],
      [4, "failed", undefined],
    ]);
    expect(piWorkflowProgressSummary(live!)).toBe(
      "Critique · 2/4 agents done, 1 running, 1 failed",
    );

    const persisted = normalizePiWorkflowSnapshot({
      runId: "review-abc",
      workflowName: "review",
      status: "paused",
      phases: ["Recon"],
      agents: [],
      tokenUsage: { total: 102 },
      script: "export const meta = {}",
    });
    expect(persisted).toMatchObject({ runId: "review-abc", status: "paused", tokens: 102 });
    expect(normalizePiWorkflowSnapshot({ runId: "x", background: true })).toBeUndefined();
  });

  it("finds background runs started or resumed by the workflow tools", () => {
    expect(
      piWorkflowBackgroundRunId("workflow", { details: { runId: "r-1", background: true } }),
    ).toBe("r-1");
    // A foreground result carries a run id too, but it is already settled.
    expect(piWorkflowBackgroundRunId("workflow", { details: { runId: "r-1", agents: [] } })).toBe(
      undefined,
    );
    expect(
      piWorkflowBackgroundRunId("workflow_control", {
        details: { action: "resume", result: "resumed", run: { runId: "r-2" } },
      }),
    ).toBe("r-2");
    expect(
      piWorkflowBackgroundRunId("workflow_control", {
        details: { action: "stop", result: "stopped", run: { runId: "r-2" } },
      }),
    ).toBeUndefined();
    expect(piWorkflowCallName({ name: "code-review" })).toBe("code-review");
    expect(
      piWorkflowCallName({
        script: "export const meta = { name: 'audit_deps', description: 'x' }",
      }),
    ).toBe("audit_deps");
    expect(isPiWorkflowRunId("audit-mt0as01r-cbcyrh")).toBe(true);
    expect(isPiWorkflowRunId("../secrets")).toBe(false);
  });

  it("locates runs where the plugin stores them for the Pi process's home and cwd", () => {
    const cwd = "/work/My Project";
    const hash = NodeCrypto.createHash("sha256").update(cwd).digest("hex").slice(0, 12);
    expect(piWorkflowRunsDirectory(cwd, { HOME: "/home/pi" }, "linux", NodePath)).toBe(
      `/home/pi/.pi/workflows/projects/my-project-${hash}/runs`,
    );
  });
});
