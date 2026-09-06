import { describe, expect, it } from "@effect/vitest";

import { piBackgroundCompletion, piBackgroundToolJobs } from "./PiBackgroundTasks.ts";

const details = { jobId: 1, command: "claude --print review", description: "Review changes" };

describe("Pi background task payloads", () => {
  it("uses structured job identity, not launcher text or command heuristics", () => {
    expect(piBackgroundToolJobs("bg", { details }, false)).toEqual([
      { jobId: 1, title: "Review changes", status: "running" },
    ]);
    expect(piBackgroundToolJobs("bash", { details }, false)).toEqual([]);
    expect(
      piBackgroundToolJobs("bg", { content: [{ type: "text", text: "Started job #1" }] }, false),
    ).toEqual([]);
    expect(piBackgroundToolJobs("bg", { details }, true)).toEqual([]);
  });

  it("bounds labels and falls back to the command", () => {
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
      { ...details, id: -1 },
      { ...details, id: 2, status: "running" },
      { ...details, id: 3, status: "killed" },
    ];
    expect(piBackgroundToolJobs("bg_status", { details: { jobs } }, false)).toEqual([
      { jobId: 2, title: "Review changes", status: "running" },
      { jobId: 3, title: "Review changes", status: "stopped" },
    ]);
  });

  it.each([
    ["done", "completed"],
    ["failed", "failed"],
    ["killed", "stopped"],
  ])("maps %s completion and discards output", (status, expected) => {
    expect(
      piBackgroundCompletion({
        role: "custom",
        customType: "bg-result",
        details: { ...details, status, durationMs: 12, stdout: "large output", stderr: "errors" },
      }),
    ).toEqual({
      jobId: 1,
      title: "Review changes",
      status: expected,
      durationMs: 12,
    });
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
});
