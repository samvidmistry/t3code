import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import { makePiRpcTransport } from "./PiRpcClient.ts";

it.effect("retains final JSONL events after exit and preserves Unicode line separators", () =>
  Effect.gen(function* () {
    const exited = yield* Deferred.make<void>();
    const transport = yield* makePiRpcTransport({
      binaryPath: process.execPath,
      args: [
        "-e",
        `
        process.stdout.write(JSON.stringify({type:"message_update", assistantMessageEvent:{type:"text_delta", delta:"a\\u2028b\\u2029c"}}) + "\\r\\n");
        process.stdout.write(JSON.stringify({type:"agent_settled"}) + "\\n");
      `,
      ],
      cwd: process.cwd(),
      env: {},
      onExit: Deferred.succeed(exited, undefined).pipe(Effect.asVoid),
    });
    yield* Deferred.await(exited);
    const messages = yield* Stream.fromQueue(transport.messages).pipe(Stream.runCollect);
    expect(messages).toMatchObject([
      {
        _tag: "event",
        event: { type: "message_update", assistantMessageEvent: { delta: "a\u2028b\u2029c" } },
      },
      { _tag: "event", event: { type: "agent_settled" } },
    ]);
    expect(yield* transport.request({ type: "get_state" }, "after-exit", 1_000)).toBeUndefined();
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("falls back to killing an RPC process that ignores stdin EOF", () =>
  Effect.gen(function* () {
    const exited = yield* Deferred.make<void>();
    const transport = yield* makePiRpcTransport({
      binaryPath: process.execPath,
      args: [
        "-e",
        `
        process.stdin.resume();
        process.stdin.on("end", () => {
          process.stdout.write(JSON.stringify({ type: "eof_received" }) + "\\n");
        });
        setInterval(() => {}, 1000);
      `,
      ],
      cwd: process.cwd(),
      env: {},
      onExit: Deferred.succeed(exited, undefined).pipe(Effect.asVoid),
    });
    const shutdown = yield* transport.kill.pipe(Effect.forkChild);
    const eof = yield* Queue.take(transport.messages);
    expect(eof).toMatchObject({ _tag: "event", event: { type: "eof_received" } });
    yield* TestClock.adjust(5_000);
    yield* Fiber.join(shutdown);
    yield* Deferred.await(exited);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("closes RPC stdin and waits for extension-style cleanup before killing the process", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-rpc-shutdown-" });
    const marker = path.join(cwd, "cleanup.txt");
    const transport = yield* makePiRpcTransport({
      binaryPath: process.execPath,
      args: [
        "-e",
        `
        const fs = require("node:fs");
        process.stdin.resume();
        process.stdin.on("end", () => {
          fs.writeFileSync(process.argv[1], "cleaned up");
          process.exit(0);
        });
        process.stdout.write(JSON.stringify({ type: "ready" }) + "\\n");
      `,
        marker,
      ],
      cwd,
      env: {},
      onExit: Effect.void,
    });
    const ready = yield* Queue.take(transport.messages);
    expect(ready._tag).toBe("event");
    yield* transport.kill;
    expect(yield* fs.readFileString(marker)).toBe("cleaned up");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
