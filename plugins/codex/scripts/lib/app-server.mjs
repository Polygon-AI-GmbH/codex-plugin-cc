/**
 * @typedef {Error & { data?: unknown, rpcCode?: number }} ProtocolError
 * @typedef {import("./app-server-protocol").AppServerMethod} AppServerMethod
 * @typedef {import("./app-server-protocol").AppServerNotification} AppServerNotification
 * @typedef {import("./app-server-protocol").AppServerNotificationHandler} AppServerNotificationHandler
 * @typedef {import("./app-server-protocol").ClientInfo} ClientInfo
 * @typedef {import("./app-server-protocol").CodexAppServerClientOptions} CodexAppServerClientOptions
 * @typedef {import("./app-server-protocol").InitializeCapabilities} InitializeCapabilities
 */
import fs from "node:fs";
import net from "node:net";
import process from "node:process";
import { spawn } from "node:child_process";
import readline from "node:readline";
import { parseBrokerEndpoint } from "./broker-endpoint.mjs";
import { ensureBrokerSession, loadBrokerSession } from "./broker-lifecycle.mjs";
import { terminateProcessTree } from "./process.mjs";

/**
 * Every `codex app-server` this process spawned, so an abrupt exit can still
 * take them down.
 *
 * Cleanup used to live ONLY in `close()`, which runs on the graceful path. A
 * dispatcher killed by its harness's foreground timeout — the documented way
 * these jobs die — never reached it, and because the child is not detached it
 * outlived the parent instead of dying with it. Verified: SIGKILL the parent
 * and the child keeps running. They accumulate one per killed dispatch until
 * reboot (observed: 141 orphans holding 6.1 GB, oldest over two days), and
 * each one holds a broker socket that `/codex:cancel` can no longer reach.
 *
 * @type {Set<import("node:child_process").ChildProcess>}
 */
const liveAppServers = new Set();
const liveWatchdogs = new Set();
let reaperInstalled = false;

function reapLiveAppServers() {
  for (const proc of liveAppServers) {
    try {
      // Negative pid = the whole process group, which `detached: true` at spawn
      // made this child the leader of. A plain `kill()` would leave any
      // grandchild the app-server spawned behind.
      process.kill(-proc.pid, "SIGKILL");
    } catch {
      try {
        proc.kill("SIGKILL");
      } catch {
        // Already gone, or not ours any more. Nothing left to do.
      }
    }
  }
  liveAppServers.clear();
}

/**
 * A tiny sibling that outlives us and kills `childPid`'s group when we vanish.
 *
 * **This is the only cleanup that survives SIGKILL of this process**, which is
 * precisely how these dispatchers die: the harness's foreground timeout does
 * not send SIGTERM. No in-process handler can run on SIGKILL — by definition —
 * so the guarantee has to live outside the process. Measured: with signal
 * handlers alone, a SIGKILL'd parent still left the app-server running; with
 * this watchdog it does not.
 *
 * **It blocks on EOF rather than polling.** The guard's stdin is a pipe whose
 * write end only we hold. Any death of ours — SIGKILL included — closes every
 * fd we own, `read` sees EOF, and the group dies within milliseconds. The
 * earlier `while kill -0 $parent; do sleep 1; done` form cost a fork of
 * `/bin/sleep` every second for the life of a 20-40 minute job (~2400 of them),
 * fired up to a second late, and could miss the death entirely if the parent's
 * pid was recycled inside a poll interval — leaking exactly the orphan this
 * exists to prevent. `read` and `kill` are both shell builtins, so the guard
 * also needs no PATH at all; `sleep` was the only reason it ever did.
 *
 * `sh`, not another node: it must cost nothing to keep around for a long job.
 *
 * The PATH rule here is not file-wide — `initialize()` deliberately resolves
 * bare `codex` through the caller's PATH, which is the point of that spawn. The
 * rule is: an INFRASTRUCTURE spawn (this guard, the broker) names an absolute
 * interpreter and passes its own environment, and EVERY spawn, infrastructure
 * or not, handles the async `error` event. That second half is what bit:
 * ENOENT/EACCES/EAGAIN/EMFILE/ENFILE are deferred to an `error` event, so the
 * try/catch below never saw `spawn("sh")` fail under a restricted PATH; it
 * surfaced as an uncaughtException and killed the companion. The try/catch is
 * still load-bearing — ENOTDIR, ENAMETOOLONG and fork ENOMEM throw
 * synchronously — so both paths are needed, not either one.
 *
 * @param {number} childPid
 * @returns {import("node:child_process").ChildProcess | null}
 */
export function spawnParentDeathWatchdog(childPid) {
  if (process.platform === "win32") {
    return null;   // no process groups; terminateProcessTree covers cleanup there
  }
  // A failed `spawn("codex")` leaves pid undefined, and on that path `exit`
  // never fires — so nothing would ever kill a guard we started, and it would
  // sit there for the whole companion lifetime holding a pipe.
  // `> 1`, not merely finite: `kill -9 -1` is POSIX for "every process this uid
  // may signal", so a pid of 1 would turn the guard into a session killer. No
  // production path can produce it — `spawn()` never returns pid 1 — but this
  // function is exported now, and the cost of the guard being wrong here is the
  // developer's whole desktop session.
  if (!Number.isInteger(childPid) || childPid <= 1) {
    return null;
  }
  let guard;
  try {
    guard = spawn(
      "/bin/sh",
      [
        "-c",
        // Blocks until our end of the pipe closes, then kills the child's whole
        // GROUP, falling back to the bare pid if it leads no group.
        `read -r _ 2>/dev/null; ` +
          `kill -9 -${childPid} 2>/dev/null || kill -9 ${childPid} 2>/dev/null`
      ],
      { stdio: ["pipe", "ignore", "ignore"], detached: true, env: {} }
    );
  } catch (error) {
    warnWatchdogUnavailable(error);
    return null;            // best-effort: the in-process reaper still applies
  }
  // Async spawn/exec failures land here, not in the catch. Announced rather
  // than swallowed: losing this guard silently removes the ONLY cleanup that
  // survives SIGKILL, and the next orphan report would have nothing to
  // correlate against.
  guard.on("error", warnWatchdogUnavailable);
  // The pipe is a lifetime signal, never written to. A listener so that an
  // EPIPE (the guard died first) cannot become an unhandled stream error.
  guard.stdin?.on("error", () => {});
  guard.stdin?.unref();
  guard.unref();            // must not hold OUR event loop open
  // Registered HERE, not at the call site, so every caller gets retention by
  // construction and releases it through `releaseParentDeathWatchdog`.
  //
  // DEFENSIVE, and honestly so: the pipe is the lifetime signal, so a collected
  // ChildProcess could in principle have its stdin finalized and kill a healthy
  // app-server. A hand-inlined guard with no reference DOES die that way under
  // forced GC — but through this function I could not reproduce it (forced
  // `global.gc()`, heap churn, and both together all left the child alive), so
  // something already keeps the handle reachable. The Set costs one Set entry
  // per client and makes the guarantee explicit instead of incidental; do not
  // remove it on the grounds that no test fails, because no test can currently
  // tell the difference.
  liveWatchdogs.add(guard);
  return guard;
}

/**
 * Stop a guard and drop the retention `spawnParentDeathWatchdog` took.
 *
 * The counterpart to the `liveWatchdogs.add` above: without an exported release
 * the Set would pin one ChildProcess — and one open socketpair fd — per call,
 * for the lifetime of the process.
 *
 * @param {import("node:child_process").ChildProcess | null} guard
 */
export function releaseParentDeathWatchdog(guard) {
  if (!guard) return;
  try {
    guard.kill("SIGKILL");
  } catch {
    // Already gone; it exits on its own once we do.
  }
  liveWatchdogs.delete(guard);
}

function warnWatchdogUnavailable(error) {
  try {
    process.stderr.write(
      `[codex] parent-death watchdog unavailable (${error?.code ?? error}); ` +
        `an app-server may outlive a SIGKILL of this process\n`
    );
  } catch {
    // stderr is gone too. Nothing useful left to do.
  }
}

export function installReaper() {
  if (reaperInstalled) {
    return;
  }
  reaperInstalled = true;
  // `exit` is synchronous-only, which is exactly why the kill above uses the
  // sync `process.kill` rather than anything awaited.
  process.on("exit", reapLiveAppServers);
  for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
    process.on(signal, () => {
      reapLiveAppServers();
      // Re-raise with the handler removed, so the exit code still reports the
      // signal rather than a plain 0.
      process.removeAllListeners(signal);
      process.kill(process.pid, signal);
    });
  }
  // NO `uncaughtException` handler. `process.on("exit")` above ALREADY runs on
  // an uncaught exception, so one is redundant for OUR reaping — and the
  // rethrowing form this file used to carry had two costs beyond that. It DID
  // reap (its first statement ran) but the rethrow made the exit code 7,
  // `Internal Exception Handler Run-Time Failure`, instead of 1, and installing
  // any uncaughtException listener SUPPRESSES every `exit` listener, so any
  // future cleanup hung there would silently stop running on the crash path.
  // Verified on node v26.7.0 both ways. The exit-7 conversion is also how a
  // stray spawn error in the watchdog above could kill this process at all.
}

const PLUGIN_MANIFEST_URL = new URL("../../.claude-plugin/plugin.json", import.meta.url);
const PLUGIN_MANIFEST = JSON.parse(fs.readFileSync(PLUGIN_MANIFEST_URL, "utf8"));

export const BROKER_ENDPOINT_ENV = "CODEX_COMPANION_APP_SERVER_ENDPOINT";
export const BROKER_BUSY_RPC_CODE = -32001;

/** @type {ClientInfo} */
const DEFAULT_CLIENT_INFO = {
  title: "Codex Plugin",
  name: "Claude Code",
  version: PLUGIN_MANIFEST.version ?? "0.0.0"
};

/** @type {InitializeCapabilities} */
const DEFAULT_CAPABILITIES = {
  experimentalApi: false,
  requestAttestation: false,
  optOutNotificationMethods: [
    "item/agentMessage/delta",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/summaryPartAdded",
    "item/reasoning/textDelta"
  ]
};

function buildJsonRpcError(code, message, data) {
  return data === undefined ? { code, message } : { code, message, data };
}

function createProtocolError(message, data) {
  const error = /** @type {ProtocolError} */ (new Error(message));
  error.data = data;
  if (data?.code !== undefined) {
    error.rpcCode = data.code;
  }
  return error;
}

class AppServerClientBase {
  constructor(cwd, options = {}) {
    this.cwd = cwd;
    this.options = options;
    this.pending = new Map();
    this.nextId = 1;
    this.stderr = "";
    this.closed = false;
    this.exitError = null;
    /** @type {AppServerNotificationHandler | null} */
    this.notificationHandler = null;
    this.lineBuffer = "";
    this.transport = "unknown";

    this.exitPromise = new Promise((resolve) => {
      this.resolveExit = resolve;
    });
  }

  setNotificationHandler(handler) {
    this.notificationHandler = handler;
  }

  /**
   * @template {AppServerMethod} M
   * @param {M} method
   * @param {import("./app-server-protocol").AppServerRequestParams<M>} params
   * @returns {Promise<import("./app-server-protocol").AppServerResponse<M>>}
   */
  request(method, params) {
    if (this.closed) {
      throw new Error("codex app-server client is closed.");
    }

    const id = this.nextId;
    this.nextId += 1;

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject, method });
      this.sendMessage({ id, method, params });
    });
  }

  notify(method, params = {}) {
    if (this.closed) {
      return;
    }
    this.sendMessage({ method, params });
  }

  handleChunk(chunk) {
    this.lineBuffer += chunk;
    let newlineIndex = this.lineBuffer.indexOf("\n");
    while (newlineIndex !== -1) {
      const line = this.lineBuffer.slice(0, newlineIndex);
      this.lineBuffer = this.lineBuffer.slice(newlineIndex + 1);
      this.handleLine(line);
      newlineIndex = this.lineBuffer.indexOf("\n");
    }
  }

  handleLine(line) {
    if (!line.trim()) {
      return;
    }

    let message;
    try {
      message = JSON.parse(line);
    } catch (error) {
      this.handleExit(createProtocolError(`Failed to parse codex app-server JSONL: ${error.message}`, { line }));
      return;
    }

    if (message.id !== undefined && message.method) {
      this.handleServerRequest(message);
      return;
    }

    if (message.id !== undefined) {
      const pending = this.pending.get(message.id);
      if (!pending) {
        return;
      }
      this.pending.delete(message.id);

      if (message.error) {
        pending.reject(createProtocolError(message.error.message ?? `codex app-server ${pending.method} failed.`, message.error));
      } else {
        pending.resolve(message.result ?? {});
      }
      return;
    }

    if (message.method && this.notificationHandler) {
      this.notificationHandler(/** @type {AppServerNotification} */ (message));
    }
  }

  handleServerRequest(message) {
    this.sendMessage({
      id: message.id,
      error: buildJsonRpcError(-32601, `Unsupported server request: ${message.method}`)
    });
  }

  handleExit(error) {
    if (this.exitResolved) {
      return;
    }

    this.exitResolved = true;
    this.exitError = error ?? null;

    for (const pending of this.pending.values()) {
      pending.reject(this.exitError ?? new Error("codex app-server connection closed."));
    }
    this.pending.clear();
    this.resolveExit(undefined);
  }

  sendMessage(_message) {
    throw new Error("sendMessage must be implemented by subclasses.");
  }
}

class SpawnedCodexAppServerClient extends AppServerClientBase {
  constructor(cwd, options = {}) {
    super(cwd, options);
    this.transport = "direct";
  }

  async initialize() {
    installReaper();
    this.proc = spawn("codex", ["app-server"], {
      cwd: this.cwd,
      env: this.options.env ?? process.env,
      stdio: ["pipe", "pipe", "pipe"],
      shell: process.platform === "win32" ? (process.env.SHELL || true) : false,
      // Its own process group, so cleanup can signal the app-server AND
      // anything it spawned as one unit. Not `unref`'d: this process should
      // still wait for it on the normal path.
      detached: process.platform !== "win32",
      windowsHide: true
    });
    liveAppServers.add(this.proc);
    this.watchdog = spawnParentDeathWatchdog(this.proc.pid);
    this.proc.once("exit", () => {
      liveAppServers.delete(this.proc);
      releaseParentDeathWatchdog(this.watchdog);
      this.watchdog = null;
    });

    this.proc.stdout.setEncoding("utf8");
    this.proc.stderr.setEncoding("utf8");

    this.proc.stderr.on("data", (chunk) => {
      this.stderr += chunk;
    });

    this.proc.on("error", (error) => {
      this.handleExit(error);
    });

    this.proc.on("exit", (code, signal) => {
      const stderr = this.stderr.trim();
      const detail =
        code === 0
          ? null
          : createProtocolError(
              `codex app-server exited unexpectedly (${signal ? `signal ${signal}` : `exit ${code}`}).${stderr ? `\n${stderr}` : ""}`
            );
      this.handleExit(detail);
    });

    this.readline = readline.createInterface({ input: this.proc.stdout });
    this.readline.on("line", (line) => {
      this.handleLine(line);
    });

    await this.request("initialize", {
      clientInfo: this.options.clientInfo ?? DEFAULT_CLIENT_INFO,
      capabilities: this.options.capabilities ?? DEFAULT_CAPABILITIES
    });
    this.notify("initialized", {});
  }

  async close() {
    if (this.closed) {
      await this.exitPromise;
      return;
    }

    this.closed = true;

    if (this.readline) {
      this.readline.close();
    }

    if (this.proc && !this.proc.killed) {
      this.proc.stdin.end();
      // A REF'd timer, deliberately. This used to be `.unref()`'d, so if Node
      // reached its exit before the 50ms elapsed the timer never fired and
      // nothing was ever signalled — the graceful path leaking exactly like
      // the abrupt one. Holding the loop open for 50ms costs nothing; the
      // handle is cleared the moment the child exits on its own.
      const killTimer = setTimeout(() => {
        if (this.proc && !this.proc.killed && this.proc.exitCode === null) {
          // On Windows with shell: true, the direct child is cmd.exe.
          // Use terminateProcessTree to kill the entire tree including
          // the grandchild node process.
          if (process.platform === "win32") {
            try {
              terminateProcessTree(this.proc.pid);
            } catch {
              // Best-effort cleanup during shutdown — swallow errors rather
              // than crash the host process.
            }
          } else {
            try {
              // The GROUP (spawned detached), not just the leader: an
              // app-server that spawned helpers would otherwise leave them.
              process.kill(-this.proc.pid, "SIGTERM");
            } catch {
              this.proc.kill("SIGTERM");
            }
          }
        }
      }, 50);
      this.proc.once("exit", () => clearTimeout(killTimer));
    }

    await this.exitPromise;
  }

  sendMessage(message) {
    const line = `${JSON.stringify(message)}\n`;
    const stdin = this.proc?.stdin;
    if (!stdin) {
      throw new Error("codex app-server stdin is not available.");
    }
    stdin.write(line);
  }
}

class BrokerCodexAppServerClient extends AppServerClientBase {
  constructor(cwd, options = {}) {
    super(cwd, options);
    this.transport = "broker";
    this.endpoint = options.brokerEndpoint;
  }

  async initialize() {
    await new Promise((resolve, reject) => {
      const target = parseBrokerEndpoint(this.endpoint);
      this.socket = net.createConnection({ path: target.path });
      this.socket.setEncoding("utf8");
      this.socket.on("connect", resolve);
      this.socket.on("data", (chunk) => {
        this.handleChunk(chunk);
      });
      this.socket.on("error", (error) => {
        if (!this.exitResolved) {
          reject(error);
        }
        this.handleExit(error);
      });
      this.socket.on("close", () => {
        this.handleExit(this.exitError);
      });
    });

    await this.request("initialize", {
      clientInfo: this.options.clientInfo ?? DEFAULT_CLIENT_INFO,
      capabilities: this.options.capabilities ?? DEFAULT_CAPABILITIES
    });
    this.notify("initialized", {});
  }

  async close() {
    if (this.closed) {
      await this.exitPromise;
      return;
    }

    this.closed = true;
    if (this.socket) {
      this.socket.end();
    }
    await this.exitPromise;
  }

  sendMessage(message) {
    const line = `${JSON.stringify(message)}\n`;
    const socket = this.socket;
    if (!socket) {
      throw new Error("codex app-server broker connection is not connected.");
    }
    socket.write(line);
  }
}

export class CodexAppServerClient {
  static async connect(cwd, options = {}) {
    let brokerEndpoint = null;
    if (!options.disableBroker) {
      brokerEndpoint = options.brokerEndpoint ?? options.env?.[BROKER_ENDPOINT_ENV] ?? process.env[BROKER_ENDPOINT_ENV] ?? null;
      if (!brokerEndpoint && options.reuseExistingBroker) {
        brokerEndpoint = loadBrokerSession(cwd)?.endpoint ?? null;
      }
      if (!brokerEndpoint && !options.reuseExistingBroker) {
        const brokerSession = await ensureBrokerSession(cwd, { env: options.env });
        brokerEndpoint = brokerSession?.endpoint ?? null;
      }
    }
    const client = brokerEndpoint
      ? new BrokerCodexAppServerClient(cwd, { ...options, brokerEndpoint })
      : new SpawnedCodexAppServerClient(cwd, options);
    await client.initialize();
    return client;
  }
}
