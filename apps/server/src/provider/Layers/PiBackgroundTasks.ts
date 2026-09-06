/** Structured lifecycle payloads from Pi's bg / bg_status extension. */
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

const JobId = Schema.Int.check(Schema.isGreaterThan(0));
const JobDescription = {
  command: Schema.NonEmptyString,
  description: Schema.String,
};
const JobStatus = Schema.Literals(["running", "done", "failed", "killed"]);
const decodeLaunch = Schema.decodeUnknownOption(
  Schema.Struct({ details: Schema.Struct({ jobId: JobId, ...JobDescription }) }),
);
const decodeStatus = Schema.decodeUnknownOption(
  Schema.Struct({ details: Schema.Struct({ jobs: Schema.Array(Schema.Unknown) }) }),
);
const decodeJob = Schema.decodeUnknownOption(
  Schema.Struct({ id: JobId, ...JobDescription, status: JobStatus }),
);
const decodeCompletion = Schema.decodeUnknownOption(
  Schema.Struct({
    role: Schema.Literal("custom"),
    customType: Schema.Literal("bg-result"),
    details: Schema.Struct({
      jobId: JobId,
      ...JobDescription,
      status: Schema.Literals(["done", "failed", "killed"]),
      durationMs: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
    }),
  }),
);

export interface PiBackgroundJob {
  readonly jobId: number;
  readonly title: string;
  readonly status: "running" | "completed" | "failed" | "stopped";
  readonly durationMs?: number;
}

function title(job: { readonly description: string; readonly command: string }): string {
  return (job.description.trim() || job.command.trim() || "Background job").slice(0, 2_000);
}

function status(value: "running" | "done" | "failed" | "killed"): PiBackgroundJob["status"] {
  return value === "done" ? "completed" : value === "killed" ? "stopped" : value;
}

export function piBackgroundToolJobs(
  toolName: string,
  result: unknown,
  isError: boolean,
): ReadonlyArray<PiBackgroundJob> {
  if (isError) return [];
  if (toolName === "bg") {
    const launch = Option.getOrUndefined(decodeLaunch(result));
    if (!launch) return [];
    return [{ jobId: launch.details.jobId, title: title(launch.details), status: "running" }];
  }
  if (toolName !== "bg_status") return [];
  const snapshot = Option.getOrUndefined(decodeStatus(result));
  if (!snapshot) return [];
  return snapshot.details.jobs.flatMap((raw) => {
    const job = Option.getOrUndefined(decodeJob(raw));
    return job ? [{ jobId: job.id, title: title(job), status: status(job.status) }] : [];
  });
}

export function piBackgroundCompletion(message: unknown): PiBackgroundJob | undefined {
  const completion = Option.getOrUndefined(decodeCompletion(message));
  if (!completion) return undefined;
  const job = completion.details;
  return {
    jobId: job.jobId,
    title: title(job),
    status: status(job.status),
    ...(job.durationMs !== undefined ? { durationMs: job.durationMs } : {}),
  };
}
