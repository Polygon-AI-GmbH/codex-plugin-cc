import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
// app-server.mjs imports this module too; the cycle is harmless because the
// constant is only read inside a function, never at module evaluation.
import { BROKER_BUSY_RPC_CODE } from "./app-server.mjs";
import { createBrokerEndpoint, parseBrokerEndpoint } from "./broker-endpoint.mjs";
import { readJsonFile, workspaceIsGone, writeJsonFile } from "./fs.mjs";
import { ensureStateDir, fallbackStateRoot, isSafeStateRoot, resolveStateDir, resolveStateRoot } from "./state.mjs";
import { resolveWorkspaceRoot } from "./workspace.mjs";

export const PID_FILE_ENV = "CODEX_COMPANION_APP_SERVER_PID_FILE";
export const LOG_FILE_ENV = "CODEX_COMPANION_APP_SERVER_LOG_FILE";
const BROKER_STATE_FILE = "broker.json";
const SESSION_DIR_PREFIX = "cxc-";
// The SessionStart hook has a 5s timeout in hooks.json and the sweep is not
// what the user is waiting for, so it gets half of that and stops mid-walk.
// Whatever it does not reach this time is still there next session, and each
// broker also expires on its own.
const SWEEP_BUDGET_MS = 2500;
const SWEEP_SHUTDOWN_TIMEOUT_MS = 250;
const SWEEP_MAX_IN_FLIGHT = 16;
// A connect that fails one of these ways proves nobody is listening: the
// endpoint's files can go. Anything else (EACCES, a timeout) proves nothing.
const NO_LISTENER_ERRNOS = new Set(["ENOENT", "ECONNREFUSED"]);

export function createBrokerSessionDir(prefix = SESSION_DIR_PREFIX) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

// Every tmp root a session dir could have been created under from this
// machine: this process's, and the one the Claude Code Bash sandbox exports as
// TMPDIR. A record written from inside the sandbox names a session dir under
// the latter, and confining to `os.tmpdir()` alone left those files behind
// while the record itself was unlinked — an unsweepable leak.
function sessionTmpRoots() {
  const roots = [os.tmpdir()];
  const uid = process.getuid?.();
  if (process.platform !== "win32" && uid !== undefined) {
    roots.push(path.join("/tmp", `claude-${uid}`));
  }
  return roots;
}

function sessionDirSegments(candidate) {
  if (typeof candidate !== "string" || !candidate) {
    return null;
  }
  const resolved = path.resolve(candidate);
  for (const root of sessionTmpRoots()) {
    const relative = path.relative(root, resolved);
    if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
      continue;
    }
    const segments = relative.split(path.sep);
    if (!segments[0].startsWith(SESSION_DIR_PREFIX)) {
      continue; // One root may contain another; a later one can still match.
    }
    // Lexical confinement is not enough: a `cxc-*` entry that is a symlink
    // would let every unlink below land wherever it points. A missing entry
    // is still a valid shape — there is nothing to unlink through.
    try {
      if (fs.lstatSync(path.join(root, segments[0])).isSymbolicLink()) {
        return null;
      }
    } catch {
      // Absent. The shape is right; the unlinks will simply find nothing.
    }
    return segments;
  }
  return null;
}

/** A directory `createBrokerSessionDir` could have handed out. */
export function isBrokerSessionDir(candidate) {
  return sessionDirSegments(candidate)?.length === 1;
}

/**
 * A file directly inside a directory `createBrokerSessionDir` could have handed
 * out. The teardown below unlinks whatever a `broker.json` names, and that
 * record may have been planted by someone else — so only paths of this shape
 * are ever removed; everything else in a record is left alone.
 */
export function isBrokerSessionPath(candidate) {
  return sessionDirSegments(candidate)?.length === 2;
}

function unlinkQuietly(filePath) {
  try {
    fs.unlinkSync(filePath);
  } catch {
    // Already gone (a broker unlinks its own files on a clean exit) or not ours
    // to remove. Neither may abort the teardown.
  }
}

function connectToEndpoint(endpoint) {
  const target = parseBrokerEndpoint(endpoint);
  return net.createConnection({ path: target.path });
}

export async function waitForBrokerEndpoint(endpoint, timeoutMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const ready = await new Promise((resolve) => {
      const socket = connectToEndpoint(endpoint);
      socket.on("connect", () => {
        socket.end();
        resolve(true);
      });
      socket.on("error", () => resolve(false));
    });
    if (ready) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return false;
}

// The one reply line a shutdown gets back, once a whole line has arrived (the
// broker acks before it terminates, so the line always completes — but not
// necessarily in one `data` event). Only the two shapes the broker actually
// sends mean anything: `result` is its acknowledgement, the busy error code
// its refusal. Anything else — a parse failure, some other error — proves
// nothing about whether the process behind the socket is done, and the
// sweep's file removal must not treat it as an ack.
function classifyShutdownReply(line) {
  let message;
  try {
    message = JSON.parse(line);
  } catch {
    return "BADREPLY";
  }
  if (message?.error?.code === BROKER_BUSY_RPC_CODE) {
    return "BUSY";
  }
  return message && typeof message === "object" && "result" in message && !("error" in message) ? true : "BADREPLY";
}

/**
 * @param {string} endpoint
 * @param {{ timeoutMs?: number, ifIdle?: boolean }} [options] `timeoutMs`
 *   bounds the wait for a broker that accepts the connection and then never
 *   answers. Off by default; the sweep and the hooks pass one because any
 *   broker may be wedged. `ifIdle` asks rather than tells: a broker with a
 *   request or stream active on another socket refuses instead of ending the
 *   job mid-turn. The sweep passes it — it is a stranger to every broker it
 *   reaches; SessionEnd does not — it owns the session's own broker.
 * @returns {Promise<true | false | string>} `true` when the broker REPLIED
 *   with an acknowledgement, `"BUSY"` when it refused an `ifIdle` shutdown,
 *   `"BADREPLY"` when what came back was neither (unparseable, or an error
 *   other than the refusal), `false` when it did not answer within the timeout
 *   (or closed without answering), or the errno string when the connect itself
 *   failed — `ENOENT` / `ECONNREFUSED` mean nobody is listening. Callers that
 *   remove files must act only on `true` or a no-listener errno.
 */
export async function sendBrokerShutdown(endpoint, { timeoutMs = 0, ifIdle = false } = {}) {
  return new Promise((resolve) => {
    const socket = connectToEndpoint(endpoint);
    socket.setEncoding("utf8");
    let timer = null;
    let settled = false;
    let replyBuffer = "";
    const settle = (outcome) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      resolve(outcome);
    };
    if (timeoutMs > 0) {
      timer = setTimeout(() => {
        socket.destroy();
        settle(false);
      }, timeoutMs);
    }
    socket.on("connect", () => {
      socket.write(`${JSON.stringify({ id: 1, method: "broker/shutdown", params: ifIdle ? { ifIdle: true } : {} })}\n`);
    });
    socket.on("data", (chunk) => {
      // Buffer to the first newline, like the broker's own reader: a reply can
      // land in more than one chunk, and half a line parses as nothing.
      replyBuffer += chunk;
      const newlineIndex = replyBuffer.indexOf("\n");
      if (newlineIndex === -1) {
        return; // The timeout (or a close) bounds a reply that never completes.
      }
      socket.end();
      settle(classifyShutdownReply(replyBuffer.slice(0, newlineIndex)));
    });
    socket.on("error", (error) => settle(String(error?.code ?? "error")));
    socket.on("close", () => settle(false));
  });
}

// Announced, never silent — the same policy as the watchdog's. A broker that
// failed to start costs every later command a full `waitForBrokerEndpoint`
// timeout before falling back, and without this line nothing in stderr or the
// broker log says why.
function warnBrokerSpawnFailed(error) {
  try {
    process.stderr.write(
      `[codex] broker process failed to start (${error?.code ?? error}); ` +
        `falling back to a direct codex app-server\n`
    );
  } catch {
    // stderr is gone too. Nothing useful left to do.
  }
}

export function spawnBrokerProcess({ scriptPath, cwd, endpoint, pidFile, logFile, env = process.env }) {
  const logFd = fs.openSync(logFile, "a");
  // Same rule as the watchdog in app-server.mjs, and it needs BOTH halves,
  // because node splits spawn failures across two mechanisms. EACCES, EAGAIN,
  // EMFILE, ENFILE and ENOENT are deferred to an async `error` event; every
  // other errno — fork ENOMEM, ENOTDIR, ENAMETOOLONG on `scriptPath` — throws
  // synchronously. Unhandled, the async half becomes an uncaughtException
  // (the sole caller immediately awaits `waitForBrokerEndpoint`, so the loop is
  // live) and the sync half escapes past two callers that have no try, taking
  // the companion down; either way it skips the `closeSync` below and leaks the
  // log fd on every attempt. Both paths instead fall through to the
  // direct-spawn path `connect()` already handles when the endpoint is null.
  let child;
  try {
    child = spawn(process.execPath, [scriptPath, "serve", "--endpoint", endpoint, "--cwd", cwd, "--pid-file", pidFile], {
      cwd,
      env,
      detached: true,
      stdio: ["ignore", logFd, logFd]
    });
  } catch (error) {
    fs.closeSync(logFd);
    warnBrokerSpawnFailed(error);
    return null;
  }
  child.on("error", warnBrokerSpawnFailed);
  child.unref();
  fs.closeSync(logFd);
  return child;
}

function resolveBrokerStateFile(cwd) {
  return path.join(resolveStateDir(cwd), BROKER_STATE_FILE);
}

// The record is keyed on the workspace root (resolveStateDir), so the cwd it
// carries — and the one the broker polls — must be that same root: a broker
// invoked from a subdirectory would otherwise die when the SUBDIRECTORY went,
// and the sweep would judge a live workspace by a path inside it.
function resolveRecordedCwd(cwd) {
  return path.resolve(resolveWorkspaceRoot(cwd));
}

export function loadBrokerSession(cwd) {
  const stateFile = resolveBrokerStateFile(cwd);
  if (!fs.existsSync(stateFile)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(stateFile, "utf8"));
  } catch {
    return null;
  }
}

export function saveBrokerSession(cwd, session) {
  // Via ensureStateDir, not a bare mkdirSync: this runs BEFORE any job on the
  // first-run path (/codex:setup -> getCodexAuthStatus -> ensureBrokerSession),
  // so a modeless mkdir here created the whole state tree 0755 and left every
  // later `mode: 0o700` inert — mkdirSync ignores mode for an existing dir.
  ensureStateDir(cwd);
  // `cwd` is recorded, absolute, and always wins over anything the caller put
  // in `session`: it is the only thing that lets a LATER, unrelated process
  // (sweepOrphanedBrokerSessions) tell a live broker from one whose review-gate
  // snapshot was deleted. A relative path would be resolved against that other
  // process's cwd, which is not the same directory.
  writeJsonFile(resolveBrokerStateFile(cwd), { ...session, cwd: resolveRecordedCwd(cwd) });
}

export function clearBrokerSession(cwd) {
  // No existsSync-then-unlink: the broker clears its own record on exit, so
  // SessionEnd's clear races it and an ENOENT here is the expected outcome,
  // not a failure. ENOTDIR (a path component is a file) likewise means there
  // is no record to clear.
  try {
    fs.unlinkSync(resolveBrokerStateFile(cwd));
  } catch (error) {
    if (error?.code !== "ENOENT" && error?.code !== "ENOTDIR") {
      throw error;
    }
  }
}

export async function isBrokerEndpointReady(endpoint) {
  if (!endpoint) {
    return false;
  }
  try {
    return await waitForBrokerEndpoint(endpoint, 150);
  } catch {
    return false;
  }
}

export async function ensureBrokerSession(cwd, options = {}) {
  const existing = loadBrokerSession(cwd);
  if (existing && (await isBrokerEndpointReady(existing.endpoint))) {
    // A record from before 1.0.9 has no cwd, so the sweep cannot judge it.
    // Re-saving it with one is what makes that broker sweepable at all.
    if (typeof existing.cwd !== "string" || !existing.cwd) {
      saveBrokerSession(cwd, existing);
    }
    return existing;
  }

  if (existing) {
    teardownBrokerSession({
      endpoint: existing.endpoint ?? null,
      pidFile: existing.pidFile ?? null,
      logFile: existing.logFile ?? null,
      sessionDir: existing.sessionDir ?? null,
      pid: existing.pid ?? null,
      killProcess: options.killProcess ?? null
    });
    clearBrokerSession(cwd);
  }

  const workspaceRoot = resolveRecordedCwd(cwd);
  const sessionDir = createBrokerSessionDir();
  const endpointFactory = options.createBrokerEndpoint ?? createBrokerEndpoint;
  const endpoint = endpointFactory(sessionDir, options.platform);
  const pidFile = path.join(sessionDir, "broker.pid");
  const logFile = path.join(sessionDir, "broker.log");
  const scriptPath =
    options.scriptPath ??
    fileURLToPath(new URL("../app-server-broker.mjs", import.meta.url));

  const child = spawnBrokerProcess({
    scriptPath,
    cwd: workspaceRoot,
    endpoint,
    pidFile,
    logFile,
    env: options.env ?? process.env
  });

  // `child` is null when the spawn failed outright; skip the 2s wait for a
  // broker that was never started, but still tear the session dir down.
  const ready = child
    ? await waitForBrokerEndpoint(endpoint, options.timeoutMs ?? 2000)
    : false;
  if (!ready) {
    teardownBrokerSession({
      endpoint,
      pidFile,
      logFile,
      sessionDir,
      pid: child?.pid ?? null,
      killProcess: options.killProcess ?? null
    });
    return null;
  }

  const session = {
    endpoint,
    pidFile,
    logFile,
    sessionDir,
    pid: child.pid ?? null
  };
  saveBrokerSession(cwd, session);
  return session;
}

// The broker unlinks its pid file on every clean exit, so a pid file that still
// names `pid` is the one shape a crashed or wedged broker leaves — and the only
// one the kill exists for. Without the file the pid is stale and very likely
// reused; signalling it would hit a stranger's process group.
// Strictly positive integers only, on BOTH sides: `Number("")` is 0, so an
// empty pid file next to a record whose pid is 0 would otherwise "match" and
// `kill(0)` signals the caller's own process group.
function isPid(value) {
  return Number.isInteger(value) && value > 0;
}

function pidFileNames(pidFile, pid) {
  try {
    const recorded = Number(fs.readFileSync(pidFile, "utf8").trim());
    return isPid(recorded) && recorded === pid;
  } catch {
    return false;
  }
}

/**
 * Remove what a broker session left behind. Confined: only paths of the shape
 * `createBrokerSessionDir` produces are ever unlinked, and `pid` is only ever
 * signalled when its pid file still names it — the record may be stale, or
 * not ours at all.
 */
export function teardownBrokerSession({ endpoint = null, pidFile, logFile, sessionDir = null, pid = null, killProcess = null }) {
  const ownedPidFile = isBrokerSessionPath(pidFile) ? pidFile : null;
  const ownedLogFile = isBrokerSessionPath(logFile) ? logFile : null;

  if (isPid(pid) && killProcess && ownedPidFile && pidFileNames(ownedPidFile, pid)) {
    try {
      killProcess(pid);
    } catch {
      // Ignore missing or already-exited broker processes.
    }
  }

  if (ownedPidFile) {
    unlinkQuietly(ownedPidFile);
  }

  if (ownedLogFile) {
    unlinkQuietly(ownedLogFile);
  }

  if (endpoint) {
    try {
      const target = parseBrokerEndpoint(endpoint);
      if (target.kind === "unix" && isBrokerSessionPath(target.path)) {
        unlinkQuietly(target.path);
      }
    } catch {
      // Ignore malformed broker endpoints during teardown.
    }
  }

  const resolvedSessionDir =
    sessionDir ?? (ownedPidFile ? path.dirname(ownedPidFile) : ownedLogFile ? path.dirname(ownedLogFile) : null);
  if (isBrokerSessionDir(resolvedSessionDir)) {
    try {
      fs.rmdirSync(resolvedSessionDir);
    } catch {
      // Ignore non-empty or missing directories.
    }
  }
}

// TEST-ONLY. A `path.delimiter`-separated list that replaces the candidate
// roots below wholesale, so a test can run the SessionStart hook as a real
// subprocess without it walking — and sweeping — this machine's actual state
// roots. The `/tmp/claude-<uid>` root is hardcoded and no other env var moves
// it. Unset in production, where the default list is the only behaviour.
export const SWEEP_ROOTS_ENV = "CODEX_COMPANION_SWEEP_ROOTS";

// Every root a record could have been written under from this machine: the
// configured one, the tmp fallback, and the fallback as seen from inside the
// Claude Code Bash sandbox, whose TMPDIR is `/tmp/claude-<uid>`. A broker
// started from one of those is invisible to a sweep that walks only another.
export function sweepCandidateRoots(env = process.env) {
  const override = env[SWEEP_ROOTS_ENV];
  if (override) {
    return override.split(path.delimiter).filter(Boolean);
  }
  const roots = [resolveStateRoot(), fallbackStateRoot()];
  const uid = process.getuid?.();
  if (process.platform !== "win32" && uid !== undefined) {
    roots.push(path.join("/tmp", `claude-${uid}`, `codex-companion-${uid}`));
  }
  return roots;
}

/**
 * Shut down every recorded broker whose workspace no longer exists.
 *
 * The backstop for a post-1.0.9 broker whose own watchdog did not fire
 * (SIGSTOPped, wedged — see lib/broker-watchdog.mjs for the leak it exists
 * for). It is NOT a backstop for legacy brokers: records written before 1.0.9
 * carry no `cwd` and are skipped, so those need a one-time manual cleanup on
 * upgrade.
 *
 * Never throws and never runs unbounded: it sits on SessionStart, which has a
 * 5s hook timeout, and a session start that fails or stalls over housekeeping
 * is a worse bug than the leak.
 *
 * @param {{ budgetMs?: number, roots?: string[] }} [options] `roots` replaces
 *   `sweepCandidateRoots()`; tests pass their own so a run never walks this
 *   machine's real state roots.
 * @returns {Promise<{ scanned: number, swept: number }>}
 */
export async function sweepOrphanedBrokerSessions({ budgetMs = SWEEP_BUDGET_MS, roots = sweepCandidateRoots() } = {}) {
  const deadline = Date.now() + budgetMs;
  let scanned = 0;
  let swept = 0;

  // Classify first, synchronously; the slow part — asking strangers to shut
  // down — then runs concurrently over the whole set under one deadline. The
  // walk is under the same deadline: it is a readJsonFile plus a stat per
  // state dir (~540 on one machine), and the stat can block on a stalled
  // mount. What it does not reach is still there next session.
  const orphans = [];
  const seenRoots = new Set();
  for (const root of roots) {
    if (Date.now() >= deadline) {
      break;
    }
    let key;
    try {
      key = fs.realpathSync.native(root);
    } catch {
      continue; // No such root. Nothing recorded under it.
    }
    if (seenRoots.has(key)) {
      continue;
    }
    // A root someone else could have written into is a root whose records
    // must not be acted on — they name the files this sweep unlinks. Checked
    // BEFORE the root is marked seen: a rejected candidate (a symlinked
    // `<plugin-data>/state`, say) must not shadow the real root it points at.
    if (!isSafeStateRoot(root)) {
      continue;
    }
    seenRoots.add(key);

    let entries;
    try {
      entries = fs.readdirSync(root, { withFileTypes: true });
    } catch {
      continue; // Not readable. Nothing to do.
    }
    for (const entry of entries) {
      if (Date.now() >= deadline) {
        break; // The root loop above stops too. Untouched; the next session start retries.
      }
      if (!entry.isDirectory()) {
        continue;
      }

      const stateFile = path.join(root, entry.name, BROKER_STATE_FILE);
      let session;
      try {
        session = readJsonFile(stateFile);
      } catch {
        continue; // Absent, unreadable or corrupt — not this sweep's business.
      }
      scanned += 1;

      // Legacy records, written before `cwd` existed, are skipped rather than
      // guessed at: with no cwd there is no evidence of orphanhood, and shutting
      // down a broker another live session is using would break that session.
      if (typeof session.cwd !== "string" || !session.cwd || !workspaceIsGone(session.cwd)) {
        continue;
      }
      orphans.push({ stateFile, session });
    }
  }

  async function sweepOrphan({ stateFile, session }) {
    const timeoutMs = Math.min(SWEEP_SHUTDOWN_TIMEOUT_MS, deadline - Date.now());
    if (timeoutMs <= 0) {
      return; // Out of budget. Untouched; the next session start retries.
    }
    // No `killProcess`. These records are days old by the time anyone sweeps
    // them, and the recorded pid has very likely been reused by then —
    // terminating that process tree would kill a stranger. The shutdown RPC
    // reaches a live broker; a dead one leaves only the files below.
    // `ifIdle`: this sweep is a stranger to the broker, and a gone workspace
    // is not proof the broker is done with it — a detached `codex task` for a
    // deleted worktree may still be attached. A busy broker refuses and is
    // left alone, record and socket included; its own watchdog ends it once
    // the client leaves, or one idle window after the workspace went.
    const outcome = session.endpoint ? await sendBrokerShutdown(session.endpoint, { timeoutMs, ifIdle: true }) : "ENOENT";
    // Files go only once the broker acknowledged or the connect proved nobody
    // is listening. A timeout is a wedged OR merely slow broker, and unlinking
    // its socket would strand a live process with no endpoint: leave the
    // record for the next sweep. `BUSY` and an unreadable reply likewise.
    if (outcome !== true && !NO_LISTENER_ERRNOS.has(outcome)) {
      return;
    }
    teardownBrokerSession(session);
    unlinkQuietly(stateFile);
    swept += 1;
  }

  let next = 0;
  async function drain() {
    while (next < orphans.length && Date.now() < deadline) {
      try {
        await sweepOrphan(orphans[next++]);
      } catch {
        // One unsweepable session must not stop the walk.
      }
    }
  }
  await Promise.allSettled(Array.from({ length: Math.min(SWEEP_MAX_IN_FLIGHT, orphans.length) }, drain));

  return { scanned, swept };
}
