import { describe, expect, it } from "@effect/vitest";

import {
  makePiBackgroundJobs,
  piBackgroundCompletion,
  piBackgroundToolJobs,
} from "./backgroundJobs.ts";

const details = { jobId: 1, command: "claude --print review", description: "Review changes" };

describe("Pi background job payloads", () => {
  it("uses structured job identity rather than launcher text or command heuristics", () => {
    expect(piBackgroundToolJobs("bg", { details }, false)).toEqual([
      { jobId: 1, title: "Review changes", status: "running" },
    ]);
    expect(piBackgroundToolJobs("bash", { details }, false)).toEqual([]);
    expect(
      piBackgroundToolJobs("bg", { content: [{ type: "text", text: "Started job #1" }] }, false),
    ).toEqual([]);
    expect(piBackgroundToolJobs("bg", { details }, true)).toEqual([]);
  });

  it("bounds descriptions and falls back to the command", () => {
    expect(
      piBackgroundToolJobs("bg", { details: { ...details, description: " " } }, false)[0]?.title,
    ).toBe(details.command);
    expect(
      piBackgroundToolJobs(
        "bg",
        { details: { ...details, description: "a".repeat(10_000) } },
        false,
      )[0]?.title,
    ).toHaveLength(2_000);
  });

  it("ignores malformed jobs without dropping valid neighbors", () => {
    const jobs = [
      null,
      { ...details, id: -1, status: "running" },
      { ...details, id: 2, status: "running" },
      { ...details, id: 3, status: "killed" },
      { ...details, id: 4, status: "done" },
      { ...details, id: 5, status: "failed" },
    ];
    expect(piBackgroundToolJobs("bg_status", { details: { jobs } }, false)).toEqual([
      { jobId: 2, title: "Review changes", status: "running" },
      { jobId: 3, title: "Review changes", status: "stopped" },
      { jobId: 4, title: "Review changes", status: "completed" },
      { jobId: 5, title: "Review changes", status: "failed" },
    ]);
  });

  it.each([
    ["done", "completed"],
    ["failed", "failed"],
    ["killed", "stopped"],
  ])("maps %s completion and discards cumulative output", (status, expected) => {
    expect(
      piBackgroundCompletion({
        role: "custom",
        customType: "bg-result",
        details: { ...details, status, durationMs: 12, stdout: "large output", stderr: "errors" },
      }),
    ).toEqual({ jobId: 1, title: "Review changes", status: expected, durationMs: 12 });
  });

  it.each([
    null,
    {},
    { details },
    { role: "custom", customType: "other", details: { ...details, status: "done" } },
    { role: "custom", customType: "bg-result", details: { ...details, status: "running" } },
  ])("ignores non-completion messages: %j", (message) =>
    expect(piBackgroundCompletion(message)).toBeUndefined(),
  );

  it("does not resurrect settled jobs from stale snapshots and resets IDs on session switch", () => {
    const jobs = makePiBackgroundJobs();
    const running = piBackgroundToolJobs("bg", { details }, false);
    expect(jobs.observe(running)).toBe(true);
    expect(jobs.observe(running)).toBe(false);
    expect(jobs.pendingTasks()).toEqual([
      { taskId: "pi-bg:1", description: "Review changes", kind: "command" },
    ]);
    expect(jobs.observe([{ jobId: 1, title: "Review changes", status: "completed" }])).toBe(true);
    expect(jobs.hasPendingWork()).toBe(false);
    expect(jobs.observe(running)).toBe(false);
    expect(jobs.pendingTasks()).toEqual([]);
    jobs.clear();
    expect(jobs.observe(running)).toBe(true);
  });

  it("ignores completed jobs restored from an older process without reserving their IDs", () => {
    const jobs = makePiBackgroundJobs();
    expect(jobs.observe([{ jobId: 1, title: "Old job", status: "completed" }], true)).toBe(false);
    expect(jobs.hasPendingWork()).toBe(false);
    expect(jobs.observe(piBackgroundToolJobs("bg", { details }, false))).toBe(true);
  });

  it("keeps a launching job pending until its result and clears all state on session end", () => {
    const jobs = makePiBackgroundJobs();
    jobs.startLaunch("launch");
    expect(jobs.hasPendingWork()).toBe(true);
    jobs.endLaunch("launch");
    expect(jobs.hasPendingWork()).toBe(false);
    jobs.startLaunch("launch");
    jobs.observe(piBackgroundToolJobs("bg", { details }, false));
    jobs.clear();
    expect(jobs.hasPendingWork()).toBe(false);
    expect(jobs.pendingTasks()).toEqual([]);
  });
});
