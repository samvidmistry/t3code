/** Typed JSONL transport + pure parsing helpers for `pi --mode rpc`. */
import type {
  AgentSessionEvent,
  ModelInfo,
  RpcCommand,
  RpcExtensionUIRequest,
  RpcExtensionUIResponse,
  RpcResponse,
} from "@earendil-works/pi-coding-agent";
import type {
  ModelSelection,
  ServerProviderModel,
  ServerProviderSkill,
  ServerProviderSlashCommand,
} from "@t3tools/contracts";
import type { ModelCapabilities } from "@t3tools/contracts";
import type * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { buildSelectOptionDescriptor } from "../providerSnapshot.ts";
import { createModelCapabilities, getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

export type PiStdoutMessage =
  | { readonly _tag: "barrier"; readonly done: Deferred.Deferred<void> }
  | { readonly _tag: "response"; readonly id: string | undefined; readonly response: RpcResponse }
  | { readonly _tag: "extension-ui"; readonly request: RpcExtensionUIRequest }
  | { readonly _tag: "event"; readonly event: AgentSessionEvent };

export function tryParsePiJsonObject(text: string): Record<string, unknown> | null {
  const trimmed = text.trim();
  if (!trimmed || (trimmed[0] !== "{" && trimmed[0] !== "[")) {
    return null;
  }
  try {
    const value = JSON.parse(trimmed) as unknown; // @effect-diagnostics-ignore preferSchemaOverJson
    return value !== null && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

export function classifyPiStdoutMessage(msg: Record<string, unknown>): PiStdoutMessage | null {
  const type = msg["type"];
  if (type === "response") {
    return {
      _tag: "response",
      id: typeof msg["id"] === "string" ? (msg["id"] as string) : undefined,
      response: msg as unknown as RpcResponse,
    };
  }
  if (type === "extension_ui_request") {
    if (typeof msg["id"] !== "string" || typeof msg["method"] !== "string") return null;
    return { _tag: "extension-ui", request: msg as unknown as RpcExtensionUIRequest };
  }
  if (typeof type === "string" && type.length > 0) {
    return { _tag: "event", event: msg as unknown as AgentSessionEvent };
  }
  return null;
}

export function parsePiStdoutLine(line: string): PiStdoutMessage | null {
  const msg = tryParsePiJsonObject(line);
  return msg ? classifyPiStdoutMessage(msg) : null;
}

// only text_delta is user-visible; thinking/toolcall deltas leak raw json
export function extractAssistantTextDelta(event: AgentSessionEvent): string | null {
  if (event.type !== "message_update") return null;
  const assistantEvent = event.assistantMessageEvent;
  if (!assistantEvent || assistantEvent.type !== "text_delta") return null;
  return typeof assistantEvent.delta === "string" ? assistantEvent.delta : null;
}

export function extractReasoningTextDelta(event: AgentSessionEvent): string | null {
  if (event.type !== "message_update") return null;
  const assistantEvent = event.assistantMessageEvent;
  if (!assistantEvent || assistantEvent.type !== "thinking_delta") return null;
  const delta = (assistantEvent as { delta?: unknown }).delta;
  return typeof delta === "string" ? delta : null;
}

// slugs are provider/id; keep any extra "/" in the id
export function splitPiModelSlug(slug: string): { provider: string; id: string } | null {
  const trimmed = slug.trim();
  const idx = trimmed.indexOf("/");
  if (idx <= 0 || idx >= trimmed.length - 1) return null;
  return { provider: trimmed.slice(0, idx), id: trimmed.slice(idx + 1) };
}

export function piModelSlug(model: Pick<ModelInfo, "provider" | "id">): string {
  return `${model.provider}/${model.id}`;
}

// derived from RpcCommand so it tracks the installed package
export type PiImageContent = NonNullable<Extract<RpcCommand, { type: "prompt" }>["images"]>[number];

export type PiTurnCommand = Extract<RpcCommand, { type: "prompt" } | { type: "steer" }>;

// raw base64, not a data URL
export function piImageContentFromBytes(input: {
  readonly mimeType: string;
  readonly bytes: Uint8Array;
}): PiImageContent {
  return {
    type: "image",
    data: Buffer.from(input.bytes).toString("base64"),
    mimeType: input.mimeType,
  };
}

// mid-turn must "steer": a bare prompt is rejected while streaming and, being
// fire-and-forget, would be silently dropped
export function buildPiTurnCommand(args: {
  readonly isMidTurn: boolean;
  readonly message: string;
  readonly images?: ReadonlyArray<PiImageContent>;
}): PiTurnCommand {
  const hasImages = args.images !== undefined && args.images.length > 0;
  const images = hasImages ? [...(args.images as ReadonlyArray<PiImageContent>)] : undefined;
  return args.isMidTurn
    ? { type: "steer", message: args.message, ...(images ? { images } : {}) }
    : { type: "prompt", message: args.message, ...(images ? { images } : {}) };
}

const PI_THINKING_LEVELS = [
  { value: "off", label: "Off" },
  { value: "minimal", label: "Minimal" },
  { value: "low", label: "Low" },
  { value: "medium", label: "Medium", isDefault: true },
  { value: "high", label: "High" },
  { value: "xhigh", label: "Extra High" },
  { value: "max", label: "Max" },
] as const;

export type PiThinkingLevel = Extract<RpcCommand, { type: "set_thinking_level" }>["level"];

type PiModelThinkingMetadata = Pick<ModelInfo, "reasoning"> & {
  readonly thinkingLevelMap?: Partial<Record<PiThinkingLevel, string | null>>;
};

export const PI_THINKING_OPTION_ID = "thinking";

export const PI_THINKING_LEVEL_VALUES = PI_THINKING_LEVELS.map(
  (level) => level.value,
) as ReadonlyArray<PiThinkingLevel>;

const PI_THINKING_LEVEL_SET: ReadonlySet<string> = new Set(PI_THINKING_LEVEL_VALUES);

export function asPiThinkingLevel(value: string | undefined): PiThinkingLevel | undefined {
  return value !== undefined && PI_THINKING_LEVEL_SET.has(value)
    ? (value as PiThinkingLevel)
    : undefined;
}

export function resolvePiThinkingLevel(
  modelSelection: ModelSelection | null | undefined,
): PiThinkingLevel | undefined {
  return asPiThinkingLevel(
    getModelSelectionStringOptionValue(modelSelection, PI_THINKING_OPTION_ID),
  );
}

export type PiModelSwitchPlan =
  | { readonly kind: "noop" }
  | { readonly kind: "invalid"; readonly slug: string }
  | {
      readonly kind: "switch";
      readonly provider: string;
      readonly modelId: string;
      readonly slug: string;
    };

export function planPiModelSwitch(
  currentModel: string | undefined,
  requestedModel: string | undefined,
): PiModelSwitchPlan {
  if (requestedModel === undefined || requestedModel === currentModel) return { kind: "noop" };
  const parts = splitPiModelSlug(requestedModel);
  if (!parts) return { kind: "invalid", slug: requestedModel };
  return { kind: "switch", provider: parts.provider, modelId: parts.id, slug: requestedModel };
}

export function supportedPiThinkingLevels(
  model: PiModelThinkingMetadata,
): ReadonlyArray<PiThinkingLevel> {
  if (!model.reasoning) return ["off"];

  // Match Pi's getSupportedThinkingLevels behavior. Standard levels are
  // supported unless explicitly disabled; extended levels must be explicitly
  // exposed by the model's thinkingLevelMap.
  return PI_THINKING_LEVELS.flatMap(({ value }) => {
    const mapped = model.thinkingLevelMap?.[value];
    if (mapped === null) return [];
    if ((value === "xhigh" || value === "max") && mapped === undefined) return [];
    return [value];
  });
}

export function piModelCapabilities(model: PiModelThinkingMetadata): ModelCapabilities {
  if (!model.reasoning) return createModelCapabilities({ optionDescriptors: [] });

  const supportedLevels = new Set(supportedPiThinkingLevels(model));
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: "thinking",
        label: "Thinking",
        options: PI_THINKING_LEVELS.filter((level) => supportedLevels.has(level.value)).map(
          (level) => ({ ...level }),
        ),
      }),
    ],
  });
}

export function piModelInfoToServerModel(model: ModelInfo): ServerProviderModel {
  const slug = piModelSlug(model);
  const rawName = (model as unknown as { name?: unknown }).name;
  const name = typeof rawName === "string" && rawName.trim().length > 0 ? rawName.trim() : model.id;
  return {
    slug,
    name,
    isCustom: false,
    capabilities: piModelCapabilities(model),
  };
}

export function piResponseData(response: RpcResponse | undefined): Record<string, unknown> | null {
  if (!response || response.type !== "response" || response.success !== true) return null;
  const data = (response as { data?: unknown }).data;
  return data !== null && typeof data === "object" ? (data as Record<string, unknown>) : null;
}

export function extractSessionFile(response: RpcResponse | undefined): string | undefined {
  const sessionFile = piResponseData(response)?.["sessionFile"];
  return typeof sessionFile === "string" && sessionFile.trim().length > 0
    ? sessionFile.trim()
    : undefined;
}

export interface PiContextConfig {
  readonly contextWindow?: number;
  readonly compactsAutomatically?: boolean;
}

/** Extract context metadata from either get_state or set_model response data. */
export function extractPiContextConfig(response: RpcResponse | undefined): PiContextConfig {
  const data = piResponseData(response);
  if (!data) return {};

  const nestedModel = data["model"];
  const model =
    nestedModel !== null && typeof nestedModel === "object"
      ? (nestedModel as Record<string, unknown>)
      : data;
  const rawContextWindow = model["contextWindow"];
  const contextWindow =
    typeof rawContextWindow === "number" &&
    Number.isFinite(rawContextWindow) &&
    rawContextWindow > 0
      ? Math.round(rawContextWindow)
      : undefined;
  const autoCompactionEnabled = data["autoCompactionEnabled"];

  return {
    ...(contextWindow !== undefined ? { contextWindow } : {}),
    ...(typeof autoCompactionEnabled === "boolean"
      ? { compactsAutomatically: autoCompactionEnabled }
      : {}),
  };
}

export function extractAvailableModels(
  response: RpcResponse | undefined,
): ReadonlyArray<ModelInfo> {
  const models = piResponseData(response)?.["models"];
  if (!Array.isArray(models)) return [];
  return models.flatMap((entry) => {
    if (!entry || typeof entry !== "object") return [];
    const model = entry as Record<string, unknown>;
    if (typeof model["provider"] !== "string" || typeof model["id"] !== "string") return [];
    return [model as unknown as ModelInfo];
  });
}

const decodePiCommand = Schema.decodeUnknownOption(
  Schema.Struct({
    name: Schema.NonEmptyString,
    description: Schema.optional(Schema.String),
    source: Schema.Literals(["extension", "prompt", "skill"]),
    location: Schema.optional(Schema.String),
    path: Schema.optional(Schema.String),
  }),
);

export function extractPiResources(response: RpcResponse | undefined): {
  skills: ServerProviderSkill[];
  slashCommands: ServerProviderSlashCommand[];
} {
  const commands = piResponseData(response)?.["commands"];
  const skills: ServerProviderSkill[] = [];
  const slashCommands: ServerProviderSlashCommand[] = [];
  const seen = new Set<string>();
  if (!Array.isArray(commands)) return { skills, slashCommands };
  for (const entry of commands) {
    const decoded = decodePiCommand(entry);
    if (Option.isNone(decoded)) continue;
    const command = decoded.value;
    const name = command.name.trim();
    if (!name || name === "t3-approval-gate" || seen.has(name)) continue;
    seen.add(name);
    const description = command.description?.trim();
    slashCommands.push({ name, ...(description ? { description } : {}) });
    if (command.source === "skill" && command.path?.trim()) {
      skills.push({
        name: name.replace(/^skill:/, ""),
        path: command.path,
        enabled: true,
        ...(description ? { description } : {}),
        ...(command.location ? { scope: command.location } : {}),
      });
    }
  }
  return { skills, slashCommands };
}

// approval-gate handshake: the sentinel command's presence confirms the gate loaded
export function piResponseHasCommand(
  response: RpcResponse | undefined,
  commandName: string,
): boolean {
  const commands = piResponseData(response)?.["commands"];
  if (!Array.isArray(commands)) return false;
  return commands.some(
    (entry) =>
      typeof entry === "object" &&
      entry !== null &&
      (entry as Record<string, unknown>)["name"] === commandName,
  );
}

export function extractLastAssistantText(response: RpcResponse | undefined): string | null {
  const text = piResponseData(response)?.["text"];
  return typeof text === "string" ? text : null;
}

// fail-closed: a timeout (undefined) or mismatched command counts as failure
export function piResponseSucceeded(response: RpcResponse | undefined, command: string): boolean {
  return (
    response !== undefined &&
    response.type === "response" &&
    response.success === true &&
    (response as { command?: unknown }).command === command
  );
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

export interface PiRpcTransport {
  readonly writeCommand: (command: RpcCommand) => Effect.Effect<void>;
  readonly writeExtensionResponse: (response: RpcExtensionUIResponse) => Effect.Effect<void>;
  // sends a command and awaits its correlated response; times out to `undefined`
  readonly request: (
    command: RpcCommand,
    id: string,
    timeoutMs: number,
  ) => Effect.Effect<RpcResponse | undefined>;
  readonly messages: Queue.Dequeue<PiStdoutMessage, Cause.Done>;
  readonly kill: Effect.Effect<void>;
  /** Wait until the adapter has consumed all notifications queued so far. */
  readonly flushEvents: Effect.Effect<void>;
}

export interface MakePiRpcTransportOptions {
  readonly binaryPath: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
  readonly onExit: Effect.Effect<void>;
}

export const makePiRpcTransport = (options: MakePiRpcTransportOptions) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const spawnCommand = yield* resolveSpawnCommand(options.binaryPath || "pi", options.args, {
      env: options.env,
      extendEnv: true,
    });
    const child = yield* spawner.spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd: options.cwd,
        env: options.env,
        extendEnv: true,
        shell: spawnCommand.shell,
        forceKillAfter: 5000,
      }),
    );

    const outgoing = yield* Queue.unbounded<Uint8Array, Cause.Done>();
    const messages = yield* Queue.unbounded<PiStdoutMessage, Cause.Done>();
    const pendingRequests = new Map<string, Deferred.Deferred<RpcResponse>>();
    // resolved on process exit to unblock in-flight requests (fail fast, not full timeout)
    const closed = yield* Deferred.make<void>();

    const writeLine = (obj: RpcCommand | RpcExtensionUIResponse): Effect.Effect<void> =>
      Queue.offer(outgoing, Buffer.from(`${JSON.stringify(obj)}\n`)).pipe(Effect.asVoid);

    const handleLine = (line: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        const message = parsePiStdoutLine(line);
        if (!message) return;
        if (message._tag === "response") {
          if (message.id !== undefined) {
            const deferred = pendingRequests.get(message.id);
            if (deferred) {
              pendingRequests.delete(message.id);
              yield* Deferred.succeed(deferred, message.response);
            }
          }
          return;
        }
        yield* Queue.offer(messages, message);
      });

    const onProcessExit = Deferred.succeed(closed, undefined).pipe(
      Effect.andThen(Queue.end(messages)),
      Effect.andThen(Effect.sync(() => pendingRequests.clear())),
      Effect.andThen(options.onExit),
    );

    yield* Stream.fromQueue(outgoing).pipe(
      Stream.run(child.stdin),
      Effect.ignore,
      Effect.forkScoped,
    );

    // stderr drain (prevents the pipe from blocking)
    yield* child.stderr.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);

    // RPC is LF-delimited JSON, not a generic text-line protocol.
    let pendingLine = "";
    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Effect.gen(function* () {
          pendingLine += chunk;
          let newline: number;
          while ((newline = pendingLine.indexOf("\n")) !== -1) {
            const line = pendingLine.slice(0, newline);
            pendingLine = pendingLine.slice(newline + 1);
            yield* handleLine(line.endsWith("\r") ? line.slice(0, -1) : line);
          }
        }),
      ),
      Effect.andThen(Effect.suspend(() => (pendingLine ? handleLine(pendingLine) : Effect.void))),
      Effect.ignore,
      Effect.ensuring(onProcessExit),
      Effect.forkScoped,
    );

    const request = (
      command: RpcCommand,
      id: string,
      timeoutMs: number,
    ): Effect.Effect<RpcResponse | undefined> =>
      Effect.gen(function* () {
        if (yield* Deferred.isDone(closed)) return undefined;
        const deferred = yield* Deferred.make<RpcResponse>();
        pendingRequests.set(id, deferred);
        return yield* Effect.gen(function* () {
          yield* writeLine({ ...command, id });
          const outcome = yield* Deferred.await(deferred).pipe(
            Effect.asSome,
            Effect.race(Deferred.await(closed).pipe(Effect.as(Option.none<RpcResponse>()))),
            Effect.timeoutOption(timeoutMs),
          );
          return outcome._tag === "None" ? undefined : Option.getOrUndefined(outcome.value);
        }).pipe(Effect.ensuring(Effect.sync(() => pendingRequests.delete(id))));
      });

    const kill = Effect.gen(function* () {
      // RPC stdin EOF invokes Pi's session_shutdown handlers on every platform.
      // SIGTERM alone skips extension cleanup on Windows, orphaning bg jobs.
      yield* Queue.end(outgoing);
      const exited = yield* Deferred.await(closed).pipe(Effect.timeoutOption(5_000));
      if (Option.isNone(exited)) yield* child.kill().pipe(Effect.ignore);
    });

    return {
      writeCommand: (command) => writeLine(command),
      writeExtensionResponse: (response) => writeLine(response),
      request,
      messages,
      kill,
      flushEvents: Effect.gen(function* () {
        const done = yield* Deferred.make<void>();
        if (yield* Queue.offer(messages, { _tag: "barrier", done })) yield* Deferred.await(done);
      }),
    } satisfies PiRpcTransport;
  });

export type {
  AgentSessionEvent,
  ModelInfo,
  RpcCommand,
  RpcExtensionUIRequest,
  RpcExtensionUIResponse,
  RpcResponse,
};
