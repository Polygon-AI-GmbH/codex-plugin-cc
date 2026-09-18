#!/usr/bin/env node

import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import process from "node:process";

import { parseArgs } from "./lib/args.mjs";
import { BROKER_BUSY_RPC_CODE, CodexAppServerClient } from "./lib/app-server.mjs";
import { parseBrokerEndpoint } from "./lib/broker-endpoint.mjs";
import { clearBrokerSession, loadBrokerSession } from "./lib/broker-lifecycle.mjs";
import { startBrokerWatchdog } from "./lib/broker-watchdog.mjs";

const STREAMING_METHODS = new Set(["turn/start", "review/start", "thread/compact/start"]);
const SHUTDOWN_CLOSE_TIMEOUT_MS = 1000;
// How long a graceful app-server close gets before its group is SIGKILLed. An
// app-server that ignores SIGTERM would otherwise pin `shutdown()` forever —
// still listening, answering `initialize`, failing every request.
const APP_SERVER_CLOSE_TIMEOUT_MS = 2000;
// Distinct from the 1 that `main()`'s catch uses for a broker that never
// started, so a test or a `ps` survivor's exit status says which happened.
// (broker.log is not where to look: every teardown unlinks it.)
const APP_SERVER_LOST_EXIT_CODE = 2;

function buildStreamThreadIds(method, params, result) {
  const threadIds = new Set();
  if (params?.threadId) {
    threadIds.add(params.threadId);
  }
  if (method === "review/start" && result?.reviewThreadId) {
    threadIds.add(result.reviewThreadId);
  }
  return threadIds;
}

function buildJsonRpcError(code, message, data) {
  return data === undefined ? { code, message } : { code, message, data };
}

function send(socket, message) {
  if (socket.destroyed) {
    return;
  }
  socket.write(`${JSON.stringify(message)}\n`);
}

function isInterruptRequest(message) {
  return message?.method === "turn/interrupt";
}

function writePidFile(pidFile) {
  if (!pidFile) {
    return;
  }
  fs.mkdirSync(path.dirname(pidFile), { recursive: true });
  fs.writeFileSync(pidFile, `${process.pid}\n`, "utf8");
}

function unlinkQuietly(filePath) {
  try {
    fs.unlinkSync(filePath);
  } catch {
    // Already gone — a teardown racing us — and nothing else to do about it.
  }
}

async function main() {
  const [subcommand, ...argv] = process.argv.slice(2);
  if (subcommand !== "serve") {
    throw new Error("Usage: node scripts/app-server-broker.mjs serve --endpoint <value> [--cwd <path>] [--pid-file <path>]");
  }

  const { options } = parseArgs(argv, {
    valueOptions: ["cwd", "pid-file", "endpoint"]
  });

  if (!options.endpoint) {
    throw new Error("Missing required --endpoint.");
  }

  const cwd = options.cwd ? path.resolve(process.cwd(), options.cwd) : process.cwd();
  const endpoint = String(options.endpoint);
  const listenTarget = parseBrokerEndpoint(endpoint);
  const pidFile = options["pid-file"] ? path.resolve(options["pid-file"]) : null;
  writePidFile(pidFile);

  let appClient = null;
  let server = null;
  let watchdog = null;
  let activeRequestSocket = null;
  let activeStreamSocket = null;
  let activeStreamThreadIds = null;
  const sockets = new Set();

  // THE definition of busy, shared by the watchdog and the `ifIdle` shutdown:
  // a client is attached. Not "a request or stream is active" — a `/codex:task`
  // holds one connection across `thread/start` → prompt build → `turn/start`,
  // and in that gap no request is in flight while the job is very much alive.
  // `except` is the socket asking, whose own attachment is not evidence.
  // The active request/stream sockets are always members of `sockets`.
  function isBusy(except = null) {
    for (const socket of sockets) {
      if (socket !== except) {
        return true;
      }
    }
    return false;
  }

  function clearSocketOwnership(socket) {
    if (activeRequestSocket === socket) {
      activeRequestSocket = null;
    }
    if (activeStreamSocket === socket) {
      activeStreamSocket = null;
      activeStreamThreadIds = null;
    }
  }

  function routeNotification(message) {
    const target = activeRequestSocket ?? activeStreamSocket;
    if (!target) {
      return;
    }
    send(target, message);
    if (message.method === "turn/completed" && activeStreamSocket === target) {
      const threadId = message.params?.threadId ?? null;
      if (!threadId || !activeStreamThreadIds || activeStreamThreadIds.has(threadId)) {
        activeStreamSocket = null;
        activeStreamThreadIds = null;
        if (activeRequestSocket === target) {
          activeRequestSocket = null;
        }
      }
    }
  }

  function killAppServerGroup() {
    const childPid = appClient?.proc?.pid;
    if (!Number.isInteger(childPid) || childPid <= 1) {
      return;
    }
    try {
      process.kill(-childPid, "SIGKILL");
    } catch {
      try {
        process.kill(childPid, "SIGKILL");
      } catch {
        // Already gone.
      }
    }
  }

  // Stop accepting. Until the app-server is down a connect would still succeed
  // and `initialize` still answer, and the client's first real request would
  // then fail with an error `withAppServer` does not retry. Idempotent and
  // synchronous, so the `broker/shutdown` branch can call it BEFORE it acks:
  // an ack the client acts on must already mean "nobody else gets in".
  let serverClosed = null;
  function stopAccepting() {
    if (serverClosed) {
      return serverClosed;
    }
    serverClosed = server ? new Promise((resolve) => server.close(() => resolve())) : Promise.resolve();
    if (listenTarget.kind === "unix") {
      unlinkQuietly(listenTarget.path);
    }
    return serverClosed;
  }

  async function shutdown() {
    const closed = stopAccepting();
    for (const socket of sockets) {
      socket.end();
    }
    // Waits for the app-server child to actually exit: `close()` ends its
    // stdin, arms a 50ms SIGTERM-the-group fallback and awaits its `exit`.
    // That is what makes `terminate`'s exit take the app-server down with us
    // rather than orphan it — bounded, with SIGKILL past the bound.
    if (appClient) {
      const closedInTime = await Promise.race([
        appClient.close().then(() => true, () => true),
        new Promise((resolve) => setTimeout(() => resolve(false), APP_SERVER_CLOSE_TIMEOUT_MS))
      ]);
      if (!closedInTime) {
        killAppServerGroup();
      }
    }
    // Bounded: `server.close` only resolves once every connection is gone, and
    // a wedged client could hold one half-open forever.
    await Promise.race([closed, new Promise((resolve) => setTimeout(resolve, SHUTDOWN_CLOSE_TIMEOUT_MS))]);
    if (pidFile) {
      unlinkQuietly(pidFile);
    }
    // Our own record, on every self-initiated exit as well as the RPC and
    // signal paths: a record that outlives its broker sends the next
    // `reuseExistingBroker` connect at a dead endpoint and makes SessionEnd
    // signal a stale pid. Never a newer broker's — endpoints must match.
    try {
      if (loadBrokerSession(cwd)?.endpoint === endpoint) {
        clearBrokerSession(cwd);
      }
    } catch {
      // The state dir is unreadable or gone; the sweep handles the leftover.
    }
  }

  // The single exit, latched: `shutdown()` closes the app-server, which resolves
  // `appClient.exitPromise`, which is itself an exit trigger below. Every path
  // reaches `process.exit`, a throwing shutdown included.
  let terminating = false;
  async function terminate(exitCode) {
    if (terminating) {
      return;
    }
    terminating = true;
    try {
      await shutdown();
    } catch (error) {
      process.stderr.write(`[codex] broker shutdown failed: ${error instanceof Error ? error.message : String(error)}\n`);
    }
    process.exit(exitCode);
  }

  // Registered BEFORE `connect()`, and `connect` is told not to install its own
  // signal handlers: those SIGKILL the child group and re-raise the signal, so
  // the broker died of SIGTERM with its socket and pid file left behind before
  // this handler ever ran. Reaping on exit stays with `connect`; the signal
  // path goes through `terminate`, which reaps the app-server itself.
  // SIGHUP too: the reaper used to cover it, and a detached broker whose
  // controlling terminal goes away is exactly the one that must not die of
  // the signal with its socket and pid file left behind.
  for (const signal of ["SIGTERM", "SIGINT", "SIGHUP"]) {
    process.on(signal, () => {
      void terminate(0);
    });
  }

  appClient = await CodexAppServerClient.connect(cwd, { disableBroker: true, reapOnSignal: false });
  appClient.setNotificationHandler(routeNotification);

  server = net.createServer((socket) => {
    if (terminating) {
      socket.destroy(); // Accepted in the window before `server.close` took hold.
      return;
    }
    sockets.add(socket);
    socket.setEncoding("utf8");
    let buffer = "";

    socket.on("data", async (chunk) => {
      buffer += chunk;
      let newlineIndex = buffer.indexOf("\n");
      while (newlineIndex !== -1) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);
        newlineIndex = buffer.indexOf("\n");

        if (!line.trim()) {
          continue;
        }

        let message;
        try {
          message = JSON.parse(line);
        } catch (error) {
          send(socket, {
            id: null,
            error: buildJsonRpcError(-32700, `Invalid JSON: ${error.message}`)
          });
          continue;
        }

        if (message.id !== undefined && message.method === "initialize") {
          send(socket, {
            id: message.id,
            result: {
              userAgent: "codex-companion-broker"
            }
          });
          continue;
        }

        if (message.method === "initialized" && message.id === undefined) {
          continue;
        }

        if (message.id !== undefined && message.method === "broker/shutdown") {
          // `ifIdle` is the SessionStart sweep asking from another session: it
          // judged the workspace gone, but another socket attached means a job
          // may still be using this broker, and shutting down would close that
          // socket under a multi-minute job. Refused with the busy code; the
          // sweep leaves the record for next time and the watchdog ends this
          // broker once the client leaves (or the bound runs out). SessionEnd
          // sends no `ifIdle` and is honoured regardless.
          if (message.params?.ifIdle === true && isBusy(socket)) {
            send(socket, { id: message.id, error: buildJsonRpcError(BROKER_BUSY_RPC_CODE, "broker is busy") });
            continue;
          }
          stopAccepting();
          send(socket, { id: message.id, result: {} });
          await terminate(0);
          continue; // Reached when the latch returned early; never forward it.
        }

        if (message.id === undefined) {
          continue;
        }

        const allowInterruptDuringActiveStream =
          isInterruptRequest(message) && activeStreamSocket && activeStreamSocket !== socket && !activeRequestSocket;

        if (
          ((activeRequestSocket && activeRequestSocket !== socket) || (activeStreamSocket && activeStreamSocket !== socket)) &&
          !allowInterruptDuringActiveStream
        ) {
          send(socket, {
            id: message.id,
            error: buildJsonRpcError(BROKER_BUSY_RPC_CODE, "Shared Codex broker is busy.")
          });
          continue;
        }

        if (allowInterruptDuringActiveStream) {
          try {
            const result = await appClient.request(message.method, message.params ?? {});
            send(socket, { id: message.id, result });
          } catch (error) {
            send(socket, {
              id: message.id,
              error: buildJsonRpcError(error.rpcCode ?? -32000, error.message)
            });
          }
          continue;
        }

        const isStreaming = STREAMING_METHODS.has(message.method);
        activeRequestSocket = socket;

        try {
          const result = await appClient.request(message.method, message.params ?? {});
          send(socket, { id: message.id, result });
          if (isStreaming) {
            activeStreamSocket = socket;
            activeStreamThreadIds = buildStreamThreadIds(message.method, message.params ?? {}, result);
          }
          if (activeRequestSocket === socket) {
            activeRequestSocket = null;
          }
        } catch (error) {
          send(socket, {
            id: message.id,
            error: buildJsonRpcError(error.rpcCode ?? -32000, error.message)
          });
          if (activeRequestSocket === socket) {
            activeRequestSocket = null;
          }
          if (activeStreamSocket === socket && !isStreaming) {
            activeStreamSocket = null;
          }
        }
      }
    });

    socket.on("close", () => {
      sockets.delete(socket);
      clearSocketOwnership(socket);
      // The idle clock starts HERE, not at the last message: a client that
      // disconnects right after a 40-minute turn must get a full idle window
      // before the broker calls itself abandoned.
      watchdog?.touch();
    });

    socket.on("error", () => {
      sockets.delete(socket);
      clearSocketOwnership(socket);
      watchdog?.touch();
    });
  });

  // Only an explicit `--cwd` is polled: the `process.cwd()` default is this
  // process's own cwd and cannot go missing in a way we could act on.
  watchdog = startBrokerWatchdog({
    cwd: options.cwd ? cwd : null,
    isBusy,
    onExpire: async (reason) => {
      process.stderr.write(`[codex] broker exiting: ${reason}\n`);
      await terminate(0);
    }
  });

  // A broker that has lost its app-server is WORSE than a dead one: it keeps
  // listening, so `ensureBrokerSession` keeps handing the endpoint out as
  // healthy and every job routed through it hangs forever. `exitPromise` covers
  // every way the child can go, and `.then()` still fires if already resolved.
  appClient.exitPromise.then(() => {
    if (terminating) {
      return; // Our own shutdown closed it. Expected, not a loss.
    }
    process.stderr.write(
      `[codex] broker lost its codex app-server${appClient.exitError ? ` (${appClient.exitError.message})` : ""}; ` +
        `exiting so the next command can start a working one\n`
    );
    void terminate(APP_SERVER_LOST_EXIT_CODE);
  });

  server.listen(listenTarget.path);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
});
