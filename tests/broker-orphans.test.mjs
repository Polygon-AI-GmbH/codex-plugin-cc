/**
 * The broker orphan leak — the story is in lib/broker-watchdog.mjs's header.
 *
 * Like app-server-orphans.test.mjs, these drive the REAL broker against the
 * fake `codex` fixture rather than a hand-written stand-in: what is under test
 * is the LIFECYCLE, which is ours.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import test from "node:test";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

import { buildEnv, installFakeCodex, readFakeCodexState } from "./fake-codex-fixture.mjs";
import { alive, initGitRepo, makeTempDir, run, until, withPluginData } from "./helpers.mjs";
import { BROKER_BUSY_RPC_CODE, CodexAppServerClient } from "../plugins/codex/scripts/lib/app-server.mjs";
import { createBrokerEndpoint, parseBrokerEndpoint } from "../plugins/codex/scripts/lib/broker-endpoint.mjs";
import {
  ensureBrokerSession,
  isBrokerSessionDir,
  isBrokerSessionPath,
  loadBrokerSession,
  saveBrokerSession,
  sendBrokerShutdown,
  SWEEP_ROOTS_ENV,
  sweepCandidateRoots,
  sweepOrphanedBrokerSessions,
  teardownBrokerSession,
  waitForBrokerEndpoint
} from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";
import {
  BROKER_IDLE_MS_ENV,
  DEFAULT_BROKER_IDLE_MS,
  MIN_BROKER_IDLE_MS,
  resolveBrokerIdleMs,
  resolveWatchdogTickMs
} from "../plugins/codex/scripts/lib/broker-watchdog.mjs";
import { terminateProcessTree } from "../plugins/codex/scripts/lib/process.mjs";
import { fallbackStateRoot, isSafeStateRoot, resolveStateDir, resolveStateRoot } from "../plugins/codex/scripts/lib/state.mjs";
import { resolveWorkspaceRoot } from "../plugins/codex/scripts/lib/workspace.mjs";

const BROKER_SCRIPT = fileURLToPath(new URL("../plugins/codex/scripts/app-server-broker.mjs", import.meta.url));
const HOOK_SCRIPT = fileURLToPath(new URL("../plugins/codex/scripts/session-lifecycle-hook.mjs", import.meta.url));

// Unix sockets and process groups: the broker tests are POSIX-only, like their
// siblings in app-server-orphans.test.mjs.
const POSIX_ONLY = { skip: process.platform === "win32" ? "unix sockets and process groups" : false };

// The floor the watchdog accepts. Its tick is idleMs / 4 (250ms here), so the
// broker gets a few ticks before it may call itself idle, and a client has a
// full second after `startBroker` returns to attach before the clock can run
// out — the "must not exit" cases connect immediately for exactly that reason.
const TEST_IDLE_MS = MIN_BROKER_IDLE_MS;
// Long enough that only an explicit trigger — never the idle clock — can end
// the broker inside a test's window.
const NEVER_IDLE_MS = 60_000;
const EXIT_BUDGET_MS = 6000;

/** Spawn the REAL broker against the fake codex, and wait until it listens. */
async function startBroker({ cwd, idleMs = TEST_IDLE_MS, behavior = "review-ok" }) {
  // Not `spawnBrokerProcess`: these tests need piped stderr and a non-detached
  // child's `close` event, and that helper gives neither.
  const binDir = makeTempDir("codex-broker-bin-");
  const statePath = installFakeCodex(binDir, behavior);
  const sessionDir = makeTempDir("cxc-test-");
  const endpoint = createBrokerEndpoint(sessionDir);
  const pidFile = path.join(sessionDir, "broker.pid");

  const proc = spawn(
    process.execPath,
    [BROKER_SCRIPT, "serve", "--endpoint", endpoint, "--cwd", cwd, "--pid-file", pidFile],
    {
      cwd,
      env: { ...buildEnv(binDir), [BROKER_IDLE_MS_ENV]: String(idleMs) },
      stdio: ["ignore", "pipe", "pipe"]
    }
  );

  let stderr = "";
  proc.stderr.setEncoding("utf8");
  proc.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  // Recorded eagerly: a `once` registered after the fact never fires. `close`,
  // not `exit`: it fires once stdio has drained, so `stderr()` is complete.
  const exit = { done: false, code: null, signal: null };
  proc.once("close", (code, signal) => {
    exit.done = true;
    exit.code = code;
    exit.signal = signal;
  });

  const ready = await waitForBrokerEndpoint(endpoint, 10000);
  if (!ready) {
    // Kill before throwing: a broker that listened just after the deadline
    // would otherwise be orphaned by the very assertion complaining about it.
    proc.kill("SIGKILL");
    assert.fail(`broker never listened on ${endpoint}\n${stderr}`);
  }

  // The fixture records its pid at app-server boot, which happens inside the
  // broker's `connect()` — i.e. strictly before it listens, so this is set.
  const appServerPids = readFakeCodexState(statePath).appServerPids ?? [];
  return {
    proc,
    endpoint,
    socketPath: parseBrokerEndpoint(endpoint).path,
    pidFile,
    sessionDir,
    binDir,
    statePath,
    appServerPids,
    appServerPid: appServerPids.at(-1),
    exit,
    stderr: () => stderr
  };
}

function connectClient(endpoint) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ path: parseBrokerEndpoint(endpoint).path });
    socket.setEncoding("utf8");
    socket.on("connect", () => resolve(socket));
    socket.on("error", reject);
  });
}

/**
 * A minimal JSON-RPC caller over a raw client socket. One persistent line
 * reader, so a notification the broker forwards between two replies never
 * leaves a half-line behind for the next request to choke on.
 */
function attachRpc(socket) {
  const pending = new Map();
  let nextId = 1;
  let buffer = "";
  socket.on("data", (chunk) => {
    buffer += chunk;
    let newlineIndex = buffer.indexOf("\n");
    while (newlineIndex !== -1) {
      const line = buffer.slice(0, newlineIndex);
      buffer = buffer.slice(newlineIndex + 1);
      newlineIndex = buffer.indexOf("\n");
      if (!line.trim()) continue;
      const message = JSON.parse(line);
      const settle = pending.get(message.id);
      if (!settle) continue;
      pending.delete(message.id);
      if (message.error) settle.reject(new Error(message.error.message));
      else settle.resolve(message.result);
    }
  });
  return {
    request(method, params = {}) {
      const id = nextId++;
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
        socket.write(`${JSON.stringify({ id, method, params })}\n`);
      });
    }
  };
}

/**
 * Put the broker mid-turn: with the `interruptible-slow-task` fake the turn
 * stays open for 5s, so `activeStreamSocket` is this client for that long —
 * the shape of a detached `codex task` whose worktree gets deleted while it
 * streams.
 */
async function startSlowTurn(endpoint, cwd) {
  const client = await connectClient(endpoint);
  const rpc = attachRpc(client);
  const { thread } = await rpc.request("thread/start", { cwd });
  await rpc.request("turn/start", { threadId: thread.id, input: [{ type: "text", text: "keep the broker busy" }] });
  return client;
}

/** Belt and braces: this suite must not itself leak what it is testing for. */
async function reapBroker(broker) {
  if (!broker) return;
  try {
    broker.proc.kill("SIGKILL");
  } catch {
    // already gone
  }
  // The parent-death watchdog takes the app-server group down once the broker
  // is gone; give it a moment, then fall back to the bare pid. Never `-pid`:
  // a pid that has already been reused could lead a stranger's group.
  for (const pid of broker.appServerPids) {
    if (!(await until(() => !alive(pid), 1500))) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {
        // already gone
      }
    }
  }
}

/**
 * A listener that accepts and never answers — a wedged broker's shape. `stop`
 * destroys what it accepted first: a never-read connection would otherwise
 * hold `server.close` open forever.
 */
function listenWedged(socketPath) {
  const sockets = new Set();
  const server = net.createServer((socket) => sockets.add(socket));
  const stop = () =>
    new Promise((resolve) => {
      for (const socket of sockets) socket.destroy();
      server.close(() => resolve());
    });
  return new Promise((resolve) => server.listen(socketPath, () => resolve({ server, stop })));
}

/**
 * Run `fn` with TMPDIR — and so `os.tmpdir()`, `fallbackStateRoot()` and the
 * session-dir confinement — pointed at a fresh temp dir, then restore it. The
 * tests that plant under the fallback root or beside a `cxc-*` entry would
 * otherwise create things in this machine's real tmp root.
 */
function withTmpDir(fn) {
  const previous = process.env.TMPDIR;
  process.env.TMPDIR = makeTempDir("codex-tmpdir-");
  return Promise.resolve()
    .then(fn)
    .finally(() => {
      if (previous == null) delete process.env.TMPDIR;
      else process.env.TMPDIR = previous;
    });
}

/** A `broker.json` planted directly under a state root, bypassing the API. */
function plantRecord(root, dirName, session) {
  const dir = path.join(root, dirName);
  fs.mkdirSync(dir, { recursive: true });
  const recordFile = path.join(dir, "broker.json");
  fs.writeFileSync(recordFile, JSON.stringify(session), "utf8");
  return recordFile;
}

test("a broker whose --cwd is removed shuts itself down once no client is attached", POSIX_ONLY, async () => {
  // A client socket is held open across the removal ON PURPOSE. It pins the
  // idle clock (a broker with sockets attached is never idle), and it must pin
  // the cwd check too: a detached `codex task` whose worktree is deleted while
  // it streams would otherwise lose its multi-minute job to `cwd-removed`. So
  // the ONLY thing that can end this process is the cwd check, and only after
  // the client has left — delete either half and this fails.
  //
  // The pin is bounded by the idle window (the test below), so the window here
  // is wide enough that "still alive 2.5 ticks after the removal" cannot be
  // explained by the bound not having been reached yet.
  const cwd = makeTempDir("codex-broker-cwd-");
  const idleMs = TEST_IDLE_MS * 2;
  const tickMs = resolveWatchdogTickMs(idleMs);
  let broker = null;
  let client = null;
  try {
    broker = await startBroker({ cwd, idleMs });
    client = await connectClient(broker.endpoint);
    await delay(idleMs * 2);
    assert.equal(broker.exit.done, false, `broker exited while its cwd still existed\n${broker.stderr()}`);

    fs.rmSync(cwd, { recursive: true, force: true });

    // Two ticks and then some: the two-miss rule would have fired by now, and
    // the busy bound (one idle window) is still ahead.
    await delay(tickMs * 2.5);
    assert.equal(broker.exit.done, false, `broker exited on cwd-removed with a client mid-turn\n${broker.stderr()}`);

    client.end();
    client = null;
    assert.ok(
      await until(() => broker.exit.done, EXIT_BUDGET_MS),
      `broker outlived its --cwd; a review-gate snapshot leaks one of these per review\n${broker.stderr()}`
    );
    assert.match(broker.stderr(), /cwd-removed/, "the cwd check, not the idle clock, must be what ended it");
    assert.equal(broker.exit.code, 0, "a self-terminating broker exits cleanly");
    assert.ok(
      await until(() => !alive(broker.appServerPid), 2000),
      "the codex app-server child must die with the broker, not outlive it"
    );
    assert.equal(fs.existsSync(broker.socketPath), false, "the unix socket must be unlinked");
    assert.equal(fs.existsSync(broker.pidFile), false, "the pid file must be unlinked");
  } finally {
    client?.destroy();
    await reapBroker(broker);
  }
});

test("a broker whose --cwd is removed exits after one idle window even while a client stays attached", POSIX_ONLY, async () => {
  // The bound on the pin above. A wedged client — the pipe-exhaustion case in
  // lib/broker-watchdog.mjs's header — holds its socket open forever; without
  // a bound a gone-cwd broker with such a client is immortal, and the sweep's
  // if-idle shutdown gets BUSY from it forever. The client here never sends a
  // byte and never leaves.
  const cwd = makeTempDir("codex-broker-cwd-");
  let broker = null;
  let client = null;
  try {
    broker = await startBroker({ cwd });
    client = await connectClient(broker.endpoint);
    fs.rmSync(cwd, { recursive: true, force: true });

    assert.ok(
      await until(() => broker.exit.done, EXIT_BUDGET_MS),
      `broker outlived its --cwd by more than an idle window with a client attached\n${broker.stderr()}`
    );
    // The client never called `end()`: nothing but the bound could have ended it.
    assert.match(broker.stderr(), /cwd-removed \(busy past idle window\)/, "the bounded path must name itself");
    assert.equal(broker.exit.code, 0, "a self-terminating broker exits cleanly");
    assert.equal(fs.existsSync(broker.socketPath), false, "the unix socket must be unlinked");
  } finally {
    client?.destroy();
    await reapBroker(broker);
  }
});

test("a broker nobody is talking to exits after the idle limit, even if its app-server ignores SIGTERM", POSIX_ONLY, async () => {
  // The stubborn fake ignores SIGTERM and lingers after stdin closes, so the
  // parent-death watchdog's SIGKILL — which fires only once the BROKER is gone
  // — cannot be what ends it here: the app-server must be dead by the time the
  // broker exits, via `appClient.close()` and its SIGKILL escalation. The
  // SIGTERM count proves the graceful close was attempted first. Delete
  // `await appClient.close()` from `shutdown()` and both assertions fail.
  const cwd = makeTempDir("codex-broker-cwd-");
  let broker = null;
  try {
    broker = await startBroker({ cwd, behavior: "stubborn-app-server" });
    assert.ok(
      await until(() => broker.exit.done, EXIT_BUDGET_MS),
      `broker never idled out; every abandoned one holds an app-server and its pipes until reboot\n${broker.stderr()}`
    );
    assert.equal(broker.exit.code, 0, "an idled-out broker exits cleanly");
    // Polled: the broker's `close` fires once ITS stdio drains, which can be a
    // beat before the kernel has reaped the app-server it just SIGKILLed.
    assert.ok(
      await until(() => !alive(broker.appServerPid), 2000),
      "the codex app-server child must be dead once the broker has exited"
    );
    assert.ok(
      (readFakeCodexState(broker.statePath).appServerSigterms ?? 0) >= 1,
      "the broker must attempt a graceful close (SIGTERM) before escalating"
    );
    assert.equal(fs.existsSync(cwd), true, "the cwd is untouched — this is the idle path, not the cwd path");
  } finally {
    await reapBroker(broker);
  }
});

test("a broker whose app-server dies exits instead of serving work it cannot do", POSIX_ONLY, async () => {
  // Observed live: broker pid 6991, 4.5 days old, whose `codex app-server`
  // child had been killed during an unrelated cleanup. The broker kept
  // listening, `ensureBrokerSession` kept handing the endpoint out as healthy,
  // and every job routed through it hung at "Starting Codex task thread"
  // forever. A broker that has lost its app-server has nothing left to broker:
  // the endpoint must die with it so the next command respawns a working one.
  const cwd = makeTempDir("codex-broker-cwd-");
  let broker = null;
  try {
    broker = await startBroker({ cwd, idleMs: NEVER_IDLE_MS });
    assert.ok(alive(broker.appServerPid), "the fake app-server should be running");
    process.kill(broker.appServerPid, "SIGKILL");

    assert.ok(
      await until(() => broker.exit.done, EXIT_BUDGET_MS),
      `broker outlived its app-server; every job routed to it hangs forever\n${broker.stderr()}`
    );
    assert.notEqual(broker.exit.code, 0, "losing the app-server is a failure exit, not a clean one");
    assert.equal(broker.exit.signal, null, "it must exit under its own control, not die of a signal");
    assert.match(broker.stderr(), /app-server/, "the broker log must name the cause");
    assert.equal(
      fs.existsSync(broker.socketPath),
      false,
      "the endpoint must be unlinked so ensureBrokerSession respawns instead of reusing a dead broker"
    );
    assert.equal(fs.existsSync(broker.pidFile), false, "the pid file must be unlinked");
  } finally {
    await reapBroker(broker);
  }
});

test("a broker with a connected socket does not idle out", POSIX_ONLY, async () => {
  // The control for the test above. Without it an over-eager watchdog — one
  // that ignores attached sockets — would kill a broker mid-review and every
  // idle assertion here would still pass.
  const cwd = makeTempDir("codex-broker-cwd-");
  let broker = null;
  let client = null;
  try {
    broker = await startBroker({ cwd });
    client = await connectClient(broker.endpoint);
    await delay(TEST_IDLE_MS * 3);
    assert.equal(
      broker.exit.done,
      false,
      `broker exited at ${TEST_IDLE_MS}ms idle with a client attached\n${broker.stderr()}`
    );

    // ...and the clock was only ever held by that socket, not stopped.
    client.end();
    client = null;
    assert.ok(
      await until(() => broker.exit.done, EXIT_BUDGET_MS),
      `broker never idled out after its last client left\n${broker.stderr()}`
    );
  } finally {
    client?.destroy();
    await reapBroker(broker);
  }
});

test("a broker stops accepting connections the moment it starts shutting down", POSIX_ONLY, async () => {
  // Verified before the fix: during the app-server close window a connect
  // succeeded, `initialize` was answered, and the first real request failed
  // with `-32000 client is closed` — an error `withAppServer` does not retry.
  // The stubborn fake holds that window open for ~2s, long enough to look.
  const cwd = makeTempDir("codex-broker-cwd-");
  let broker = null;
  try {
    broker = await startBroker({ cwd, idleMs: NEVER_IDLE_MS, behavior: "stubborn-app-server" });
    assert.equal(await sendBrokerShutdown(broker.endpoint, { timeoutMs: 2000 }), true, "the shutdown RPC must be acknowledged");

    let refused = null;
    try {
      const socket = await connectClient(broker.endpoint);
      socket.destroy();
    } catch (error) {
      refused = error;
    }
    assert.equal(broker.exit.done, false, `the close window had already passed; the probe proves nothing\n${broker.stderr()}`);
    assert.ok(refused, "a connect during the close window must be refused, not accepted and then failed");
    assert.ok(
      ["ENOENT", "ECONNREFUSED"].includes(refused.code),
      `expected ENOENT/ECONNREFUSED during the close window, got ${refused.code ?? refused.message}`
    );

    assert.ok(await until(() => broker.exit.done, EXIT_BUDGET_MS), `broker never exited after broker/shutdown\n${broker.stderr()}`);
    assert.equal(broker.exit.code, 0);
  } finally {
    await reapBroker(broker);
  }
});

for (const signal of ["SIGTERM", "SIGHUP"]) {
  test(`${signal} takes the broker through its graceful shutdown`, POSIX_ONLY, async () => {
    // Verified by probe: the reaper `connect()` installs registered its signal
    // handler first, SIGKILLed the child group and re-raised, so the broker
    // died OF the signal with its socket and pid file left behind. SIGHUP is
    // in the list because the broker no longer lets the reaper handle it.
    const cwd = makeTempDir("codex-broker-cwd-");
    let broker = null;
    try {
      broker = await startBroker({ cwd, idleMs: NEVER_IDLE_MS });
      broker.proc.kill(signal);
      assert.ok(await until(() => broker.exit.done, EXIT_BUDGET_MS), `broker ignored ${signal}\n${broker.stderr()}`);
      assert.equal(broker.exit.signal, null, "the broker must exit under its own control, not die of the signal");
      assert.equal(broker.exit.code, 0, "a signalled broker exits cleanly");
      assert.equal(fs.existsSync(broker.socketPath), false, "the unix socket must be unlinked");
      assert.equal(fs.existsSync(broker.pidFile), false, "the pid file must be unlinked");
      assert.ok(await until(() => !alive(broker.appServerPid), 2000), "the app-server must still be reaped on the signal path");
    } finally {
      await reapBroker(broker);
    }
  });
}

test("saveBrokerSession records the cwd a sweep needs to judge the session", async () => {
  await withPluginData(() => {
    const workspace = makeTempDir("codex-broker-cwd-");
    saveBrokerSession(workspace, { endpoint: "unix:/tmp/fake-broker.sock" });

    const loaded = loadBrokerSession(workspace);
    assert.equal(loaded.endpoint, "unix:/tmp/fake-broker.sock");
    assert.equal(
      loaded.cwd,
      path.resolve(workspace),
      "without a recorded cwd the SessionStart sweep cannot tell a live broker from an orphan"
    );
  });
});

test("the recorded cwd is the workspace root the record is keyed on, not the invoking subdirectory", async () => {
  // Verified: a broker keyed on the repo but polling the invoking subdir exited
  // 30s after that subdir was removed, and the sweep judged the live workspace
  // by the same path.
  await withPluginData(() => {
    const workspace = makeTempDir("codex-broker-repo-");
    initGitRepo(workspace);
    const subdir = path.join(workspace, "packages", "app");
    fs.mkdirSync(subdir, { recursive: true });

    saveBrokerSession(subdir, { endpoint: "unix:/tmp/fake-broker.sock" });

    const loaded = loadBrokerSession(workspace);
    assert.ok(loaded, "the record is keyed on the workspace root");
    assert.equal(loaded.cwd, path.resolve(resolveWorkspaceRoot(subdir)));
    assert.notEqual(loaded.cwd, subdir);
  });
});

test("ensureBrokerSession re-saves a live legacy record so it becomes sweepable", POSIX_ONLY, async () => {
  await withPluginData(async () => {
    const workspace = makeTempDir("codex-broker-cwd-");
    const sessionDir = makeTempDir("cxc-test-");
    const endpoint = createBrokerEndpoint(sessionDir);
    const wedged = await listenWedged(parseBrokerEndpoint(endpoint).path);
    try {
      // A ≤1.0.8 record: written straight to disk, no cwd.
      fs.mkdirSync(resolveStateDir(workspace), { recursive: true });
      fs.writeFileSync(path.join(resolveStateDir(workspace), "broker.json"), JSON.stringify({ endpoint }), "utf8");

      const session = await ensureBrokerSession(workspace);
      assert.equal(session?.endpoint, endpoint, "a live record must be reused, not respawned");
      assert.equal(loadBrokerSession(workspace).cwd, path.resolve(workspace), "the reused record must now carry a cwd");
    } finally {
      await wedged.stop();
    }
  });
});

test("the SessionStart sweep shuts down a broker whose cwd is gone and leaves a live one alone", POSIX_ONLY, async () => {
  await withPluginData(async () => {
    const binDir = makeTempDir("codex-broker-bin-");
    installFakeCodex(binDir);
    // Long idle on purpose: the orphan's OWN watchdog also exits it on
    // cwd-removed within two ticks (10s at the fixture's 20s), which would be
    // inside the window below. At 60s only the sweep's RPC can end it — delete
    // that RPC from `sweepOrphan` and the process assertion fails.
    const env = { ...buildEnv(binDir), [BROKER_IDLE_MS_ENV]: String(NEVER_IDLE_MS) };

    const goneCwd = makeTempDir("codex-broker-gone-");
    const liveCwd = makeTempDir("codex-broker-live-");
    const goneStateFile = path.join(resolveStateDir(goneCwd), "broker.json");
    const liveStateFile = path.join(resolveStateDir(liveCwd), "broker.json");

    // Declared outside the try so the finally covers BOTH spawns: a failure
    // between them (or in the assertions below) used to strand the first
    // broker, which is precisely the leak this file is about.
    let gone = null;
    let live = null;
    try {
      gone = await ensureBrokerSession(goneCwd, { env });
      live = await ensureBrokerSession(liveCwd, { env });
      assert.ok(gone?.endpoint, "the orphan-to-be broker did not start");
      assert.ok(live?.endpoint, "the live broker did not start");

      fs.rmSync(goneCwd, { recursive: true, force: true });

      const result = await sweepOrphanedBrokerSessions({ roots: [resolveStateRoot()] });

      assert.equal(fs.existsSync(goneStateFile), false, "the orphan's broker.json must be deleted");
      assert.equal(fs.existsSync(liveStateFile), true, "a live workspace's broker.json must survive the sweep");
      assert.ok(
        await until(() => !alive(gone.pid), 3000),
        "the orphaned broker PROCESS must be gone — deleting its files while it runs is the leak, not the fix"
      );
      assert.equal(await waitForBrokerEndpoint(gone.endpoint, 500), false, "the orphaned broker must be shut down");
      assert.equal(alive(live.pid), true, "the live broker's process must survive the sweep");
      assert.equal(
        await waitForBrokerEndpoint(live.endpoint, 1500),
        true,
        "the live broker must still be serving — sweeping it would break the running session"
      );
      assert.equal(result.swept, 1, `expected exactly one swept session, got ${JSON.stringify(result)}`);
      assert.equal(result.scanned, 2, `both records must be classified, got ${JSON.stringify(result)}`);
    } finally {
      for (const session of [live, gone]) {
        if (session?.endpoint) {
          await sendBrokerShutdown(session.endpoint, { timeoutMs: 1000 });
          teardownBrokerSession({ ...session, killProcess: terminateProcessTree });
        }
      }
    }
  });
});

test("a broker mid-turn refuses an if-idle shutdown, honours a forced one, and exits on if-idle once idle", POSIX_ONLY, async () => {
  // The sweep is a stranger to every broker it reaches, so its shutdown must
  // be conditional: another session's SessionStart used to kill a broker that
  // was streaming a job for a deleted worktree, and the job with it.
  const cwd = makeTempDir("codex-broker-cwd-");
  let broker = null;
  let client = null;
  try {
    broker = await startBroker({ cwd, idleMs: NEVER_IDLE_MS, behavior: "interruptible-slow-task" });
    client = await startSlowTurn(broker.endpoint, cwd);

    assert.equal(
      await sendBrokerShutdown(broker.endpoint, { timeoutMs: 2000, ifIdle: true }),
      "BUSY",
      "a broker with an active stream must refuse an if-idle shutdown"
    );
    await delay(300);
    assert.equal(broker.exit.done, false, `broker exited on a refused if-idle shutdown\n${broker.stderr()}`);
    assert.equal(fs.existsSync(broker.socketPath), true, "a refused shutdown must leave the endpoint in place");

    client.end();
    client = null;
    await delay(200); // let the broker observe the close
    assert.equal(await sendBrokerShutdown(broker.endpoint, { timeoutMs: 2000, ifIdle: true }), true, "an idle broker honours it");
    assert.ok(await until(() => broker.exit.done, EXIT_BUDGET_MS), `broker never exited after the if-idle shutdown\n${broker.stderr()}`);
    assert.equal(broker.exit.code, 0);
  } finally {
    client?.destroy();
    await reapBroker(broker);
  }
});

test("a broker with another client merely attached refuses an if-idle shutdown", POSIX_ONLY, async () => {
  // A `/codex:task` holds ONE connection across `thread/start` → prompt build
  // → `turn/start`, and in that gap no request or stream is active on it. An
  // if-idle test keyed on the active sockets alone honoured a sweep's shutdown
  // inside that gap and `shutdown()` ended every client — the job with it.
  // "Busy" for the sweep must be what it is for the watchdog: any other
  // socket attached, silent or not.
  const cwd = makeTempDir("codex-broker-cwd-");
  let broker = null;
  let client = null;
  try {
    broker = await startBroker({ cwd, idleMs: NEVER_IDLE_MS });
    client = await connectClient(broker.endpoint);

    assert.equal(
      await sendBrokerShutdown(broker.endpoint, { timeoutMs: 2000, ifIdle: true }),
      "BUSY",
      "a broker with a client attached must refuse an if-idle shutdown"
    );
    await delay(300);
    assert.equal(broker.exit.done, false, `broker exited on a refused if-idle shutdown\n${broker.stderr()}`);
    assert.equal(client.destroyed, false, "the attached client must not have been ended");
    assert.equal(fs.existsSync(broker.socketPath), true, "a refused shutdown must leave the endpoint in place");

    client.end();
    client = null;
    await delay(200); // let the broker observe the close
    assert.equal(
      await sendBrokerShutdown(broker.endpoint, { timeoutMs: 2000, ifIdle: true }),
      true,
      "with only the requesting socket attached the shutdown is honoured"
    );
    assert.ok(await until(() => broker.exit.done, EXIT_BUDGET_MS), `broker never exited after the if-idle shutdown\n${broker.stderr()}`);
    assert.equal(broker.exit.code, 0);
  } finally {
    client?.destroy();
    await reapBroker(broker);
  }
});

test("SessionEnd's forced shutdown still ends a broker mid-turn", POSIX_ONLY, async () => {
  const cwd = makeTempDir("codex-broker-cwd-");
  let broker = null;
  let client = null;
  try {
    broker = await startBroker({ cwd, idleMs: NEVER_IDLE_MS, behavior: "interruptible-slow-task" });
    client = await startSlowTurn(broker.endpoint, cwd);
    assert.equal(await sendBrokerShutdown(broker.endpoint, { timeoutMs: 2000 }), true, "the session's own teardown is unconditional");
    assert.ok(await until(() => broker.exit.done, EXIT_BUDGET_MS), `broker survived a forced shutdown\n${broker.stderr()}`);
    assert.equal(broker.exit.code, 0);
  } finally {
    client?.destroy();
    await reapBroker(broker);
  }
});

test("the sweep leaves a broker mid-turn for a deleted workspace alone, record and socket included", POSIX_ONLY, async () => {
  await withPluginData(async () => {
    const binDir = makeTempDir("codex-broker-bin-");
    installFakeCodex(binDir, "interruptible-slow-task");
    const env = { ...buildEnv(binDir), [BROKER_IDLE_MS_ENV]: String(NEVER_IDLE_MS) };
    const goneCwd = makeTempDir("codex-broker-gone-");
    const stateFile = path.join(resolveStateDir(goneCwd), "broker.json");

    let session = null;
    let client = null;
    try {
      session = await ensureBrokerSession(goneCwd, { env });
      assert.ok(session?.endpoint, "the broker did not start");
      client = await startSlowTurn(session.endpoint, goneCwd);
      fs.rmSync(goneCwd, { recursive: true, force: true });

      const result = await sweepOrphanedBrokerSessions({ roots: [resolveStateRoot()] });

      assert.equal(result.swept, 0, `a busy broker is not an orphan yet: ${JSON.stringify(result)}`);
      assert.equal(fs.existsSync(stateFile), true, "the record stays for the next sweep to retry");
      assert.equal(alive(session.pid), true, "the broker PROCESS must survive — its job is still streaming");
      assert.equal(await waitForBrokerEndpoint(session.endpoint, 1500), true, "the endpoint must still be served");
    } finally {
      client?.destroy();
      if (session?.endpoint) {
        await sendBrokerShutdown(session.endpoint, { timeoutMs: 1000 });
        teardownBrokerSession({ ...session, killProcess: terminateProcessTree });
      }
    }
  });
});

test("the sweep skips legacy sessions that recorded no cwd, and a spent budget touches nothing", async () => {
  await withPluginData(async () => {
    const root = resolveStateRoot();
    const legacyFile = plantRecord(root, "legacy-0000000000000000", { endpoint: "unix:/tmp/codex-legacy.sock" });
    // No cwd: written by a build before this field existed. There is no
    // evidence it is orphaned, and a live broker must never be swept.
    const orphanFile = plantRecord(root, "orphan-1111111111111111", {
      endpoint: "unix:/tmp/codex-orphan.sock",
      cwd: path.join(root, "does-not-exist")
    });

    // Only this test's own root: every sweep in this file passes `roots` so a
    // run never walks — or sweeps — the developer's real state roots.
    const budgeted = await sweepOrphanedBrokerSessions({ budgetMs: 0, roots: [root] });
    assert.equal(budgeted.swept, 0, "a spent budget must stop the sweep before it touches anything");
    // The walk itself is budgeted too: ~540 state dirs at a readJsonFile plus a
    // stat each — the latter able to block on a stalled mount — is not free,
    // and the SessionStart hook has 5s in total.
    assert.equal(budgeted.scanned, 0, "a spent budget must stop the walk before it classifies anything");
    assert.equal(fs.existsSync(orphanFile), true, "nothing may be swept once the budget is gone");

    // Nobody listens at the orphan's endpoint: ENOENT proves it dead, so its
    // record goes.
    const result = await sweepOrphanedBrokerSessions({ roots: [root] });
    assert.equal(fs.existsSync(legacyFile), true, "a session with no recorded cwd must be left alone");
    assert.equal(fs.existsSync(orphanFile), false, "a session whose cwd is gone must be swept");
    assert.equal(result.swept, 1);
  });
});

test("the sweep's candidate roots cover the configured root and both tmp fallbacks", () => {
  const roots = sweepCandidateRoots();
  assert.ok(roots.includes(resolveStateRoot()));
  assert.ok(roots.includes(fallbackStateRoot()));
  if (process.platform !== "win32") {
    const uid = process.getuid();
    assert.ok(
      roots.includes(path.join("/tmp", `claude-${uid}`, `codex-companion-${uid}`)),
      "a broker started inside the Claude Code Bash sandbox records under its TMPDIR, which the unsandboxed side never resolves"
    );
  }
});

test("the test-only roots override replaces the candidate roots wholesale", () => {
  // What lets the SessionStart hook test below run the real hook as a
  // subprocess without sweeping this machine: the `/tmp/claude-<uid>` root is
  // hardcoded, so no other variable can move it.
  const a = makeTempDir("codex-sweep-root-a-");
  const b = makeTempDir("codex-sweep-root-b-");
  assert.deepEqual(sweepCandidateRoots({ [SWEEP_ROOTS_ENV]: [a, b].join(path.delimiter) }), [a, b]);
  assert.deepEqual(sweepCandidateRoots({ [SWEEP_ROOTS_ENV]: "" }), sweepCandidateRoots({}), "empty means unset");
});

test("the sweep leaves a wedged broker's record and socket alone instead of stranding it", POSIX_ONLY, async () => {
  // Verified: 18 wedged stand-ins under a 150ms budget lost all 18 records and
  // sockets while every listener stayed alive — brokers with no endpoint left
  // to shut them down through.
  await withPluginData(async () => {
    const sessionDir = makeTempDir("cxc-test-");
    const endpoint = createBrokerEndpoint(sessionDir);
    const socketPath = parseBrokerEndpoint(endpoint).path;
    const wedged = await listenWedged(socketPath);
    try {
      const recordFile = plantRecord(resolveStateRoot(), "wedged-2222222222222222", {
        endpoint,
        sessionDir,
        cwd: path.join(sessionDir, "does-not-exist")
      });

      await sweepOrphanedBrokerSessions({ budgetMs: 600, roots: [resolveStateRoot()] });

      assert.equal(fs.existsSync(recordFile), true, "a timeout is not an acknowledgement: the record stays for the next sweep to retry");
      assert.equal(fs.existsSync(socketPath), true, "the socket stays — unlinking it would strand the live process");
    } finally {
      await wedged.stop();
    }
  });
});

/**
 * A listener that answers every connection with `reply`, written in `chunks`
 * pieces with `gapMs` between them — a broker whose reply reaches the client
 * in more than one `data` event, or one that answers with something that is
 * not a reply at all.
 */
function listenReplying(socketPath, reply, { chunks = 1, gapMs = 0 } = {}) {
  const sockets = new Set();
  const server = net.createServer(async (socket) => {
    sockets.add(socket);
    const size = Math.ceil(reply.length / chunks);
    for (let i = 0; i < reply.length; i += size) {
      if (i > 0 && gapMs > 0) await delay(gapMs);
      if (socket.destroyed) return;
      socket.write(reply.slice(i, i + size));
    }
  });
  const stop = () =>
    new Promise((resolve) => {
      for (const socket of sockets) socket.destroy();
      server.close(() => resolve());
    });
  return new Promise((resolve) => server.listen(socketPath, () => resolve({ server, stop })));
}

test("sendBrokerShutdown reads a whole reply line, and treats anything but an ack or a refusal as neither", POSIX_ONLY, async () => {
  // The reply used to be classified from the first `data` chunk alone: a BUSY
  // refusal split across two writes parsed as garbage, garbage counted as an
  // acknowledgement, and `sweepOrphan` unlinked a live, busy broker's record
  // and socket on the strength of it.
  const sessionDir = makeTempDir("cxc-test-");
  const endpoint = createBrokerEndpoint(sessionDir);
  const socketPath = parseBrokerEndpoint(endpoint).path;
  const busyReply = `${JSON.stringify({ id: 1, error: { code: BROKER_BUSY_RPC_CODE, message: "broker is busy" } })}\n`;

  const split = await listenReplying(socketPath, busyReply, { chunks: 2, gapMs: 100 });
  try {
    assert.equal(
      await sendBrokerShutdown(endpoint, { timeoutMs: 2000, ifIdle: true }),
      "BUSY",
      "a refusal delivered in two chunks is still a refusal"
    );
  } finally {
    await split.stop();
  }

  const garbage = await listenReplying(socketPath, "this is not json\n");
  try {
    const outcome = await sendBrokerShutdown(endpoint, { timeoutMs: 2000, ifIdle: true });
    assert.notEqual(outcome, true, "an unparseable reply is not an acknowledgement");
    assert.equal(outcome, "BADREPLY");
  } finally {
    await garbage.stop();
  }

  const otherError = await listenReplying(socketPath, `${JSON.stringify({ id: 1, error: { code: -32700, message: "Invalid JSON" } })}\n`);
  try {
    assert.equal(
      await sendBrokerShutdown(endpoint, { timeoutMs: 2000, ifIdle: true }),
      "BADREPLY",
      "an error that is not the busy refusal is not an acknowledgement either"
    );
  } finally {
    await otherError.stop();
  }

  const ack = await listenReplying(socketPath, `${JSON.stringify({ id: 1, result: {} })}\n`, { chunks: 3, gapMs: 20 });
  try {
    assert.equal(await sendBrokerShutdown(endpoint, { timeoutMs: 2000 }), true, "the broker's own ack, chunked, still acks");
  } finally {
    await ack.stop();
  }
});

test("the sweep leaves a broker whose shutdown reply it cannot read alone", POSIX_ONLY, async () => {
  await withPluginData(async () => {
    const sessionDir = makeTempDir("cxc-test-");
    const endpoint = createBrokerEndpoint(sessionDir);
    const socketPath = parseBrokerEndpoint(endpoint).path;
    const garbage = await listenReplying(socketPath, "this is not json\n");
    try {
      const recordFile = plantRecord(resolveStateRoot(), "garbled-6666666666666666", {
        endpoint,
        sessionDir,
        cwd: path.join(sessionDir, "does-not-exist")
      });
      const result = await sweepOrphanedBrokerSessions({ roots: [resolveStateRoot()] });
      assert.equal(result.swept, 0, `a reply that is neither ack nor refusal proves nothing: ${JSON.stringify(result)}`);
      assert.equal(fs.existsSync(recordFile), true, "the record stays for the next sweep to retry");
      assert.equal(fs.existsSync(socketPath), true, "the socket stays — something live is listening on it");
    } finally {
      await garbage.stop();
    }
  });
});

test("the sweep never acts on a record under a root someone else could have written", async () => {
  // Verified: a planted record under the (predictable) fallback root made the
  // SessionStart hook unlink whatever paths it named, as the user.
  await withPluginData(async () => {
    const root = resolveStateRoot();
    const recordFile = plantRecord(root, "orphan-3333333333333333", {
      endpoint: "unix:/tmp/codex-orphan-unsafe.sock",
      cwd: path.join(root, "does-not-exist")
    });
    fs.chmodSync(root, 0o777); // group/other-writable — the hostile shape
    try {
      assert.equal(isSafeStateRoot(root), false);
      await sweepOrphanedBrokerSessions({ roots: [root] });
      assert.equal(fs.existsSync(recordFile), true, "a record under an unsafe root must not be acted on");
    } finally {
      fs.chmodSync(root, 0o700);
    }
    assert.equal(isSafeStateRoot(root), true, "0700 and owned by us is the shape the sweep trusts");
    await sweepOrphanedBrokerSessions({ roots: [root] });
    assert.equal(fs.existsSync(recordFile), false, "the same record is swept once the root is trustworthy");
  });
});

test("the sweep walks the tmp fallback root as well as the configured one", async () => {
  // Under a redirected TMPDIR: the fallback root is derived from os.tmpdir(),
  // and planting under the REAL one would create — and sweep — state outside
  // the suite's temp dirs.
  await withTmpDir(() =>
    withPluginData(async () => {
      const fallback = fallbackStateRoot();
      assert.ok(fallback.startsWith(process.env.TMPDIR), "the fallback root must sit under the redirected TMPDIR");
      fs.mkdirSync(fallback, { recursive: true, mode: 0o700 });
      assert.ok(isSafeStateRoot(fallback), `${fallback} is not a root the sweep trusts; state.test.mjs would fail here too`);
      const recordFile = plantRecord(fallback, "sweep-fallback-4444444444444444", {
        endpoint: "unix:/tmp/codex-orphan-fallback.sock",
        cwd: path.join(fallback, "does-not-exist")
      });
      assert.notEqual(resolveStateRoot(), fallback, "the configured root is elsewhere for this test");
      const result = await sweepOrphanedBrokerSessions({ roots: [resolveStateRoot(), fallback] });
      assert.equal(fs.existsSync(recordFile), false, "a record under the fallback root must be swept from a session using the configured root");
      assert.equal(result.swept, 1);
    })
  );
});

test("a rejected symlinked configured root does not shadow the fallback root it points at", async () => {
  // Verified: the sweep marked a root seen BEFORE `isSafeStateRoot` rejected
  // it, so `<plugin-data>/state -> <fallback>` (same realpath) made the
  // fallback root skip as "already walked" while nothing had walked it.
  await withTmpDir(() =>
    withPluginData(async () => {
      const fallback = fallbackStateRoot();
      fs.mkdirSync(fallback, { recursive: true, mode: 0o700 });
      const configured = path.join(process.env.CLAUDE_PLUGIN_DATA, "state");
      fs.symlinkSync(fallback, configured);
      assert.equal(resolveStateRoot(), configured, "the symlink is writable, so it is the configured root");
      assert.equal(isSafeStateRoot(configured), false, "a symlinked root is never trusted");

      const recordFile = plantRecord(fallback, "sweep-shadowed-5555555555555555", {
        endpoint: "unix:/tmp/codex-orphan-shadowed.sock",
        cwd: path.join(fallback, "does-not-exist")
      });
      const result = await sweepOrphanedBrokerSessions({ roots: [configured, fallback] });
      assert.equal(fs.existsSync(recordFile), false, "the fallback root must still be swept through its own path");
      assert.equal(result.swept, 1);
    })
  );
});

test("teardown only ever unlinks paths inside a cxc-* session directory", () => {
  // The record may have been planted by someone else, or simply be stale: the
  // one thing that makes acting on it safe is that nothing outside a broker
  // session dir is ever touched.
  const tmp = os.tmpdir();
  assert.equal(isBrokerSessionPath(path.join(tmp, "cxc-abc123", "broker.sock")), true);
  assert.equal(isBrokerSessionPath(path.join(tmp, "cxc-abc123", "broker.pid")), true);
  assert.equal(isBrokerSessionDir(path.join(tmp, "cxc-abc123")), true);
  assert.equal(isBrokerSessionDir(path.join(tmp, "cxc-abc123", "broker.pid")), false, "a file is not a session dir");
  assert.equal(isBrokerSessionPath(path.join(tmp, "cxc-abc123")), false, "the dir itself is not a session file");
  assert.equal(isBrokerSessionPath(path.join(tmp, "cxc-abc123", "nested", "x")), false, "nothing deeper than one level");
  assert.equal(isBrokerSessionPath(path.join(tmp, "other-abc123", "broker.sock")), false, "wrong prefix");
  assert.equal(isBrokerSessionPath(path.join(tmp, "cxc-abc123", "..", "victim")), false, "no escaping via ..");
  assert.equal(isBrokerSessionPath("/victim/cxc-abc123/broker.sock"), false, "must be under a tmp root");
  assert.equal(isBrokerSessionPath(path.join(tmp, "broker.sock")), false, "tmpdir itself is not a session dir");
  if (process.platform !== "win32") {
    // A record written from inside the Claude Code Bash sandbox names a
    // session dir under ITS tmpdir; confining to ours alone left those files
    // behind while the record was unlinked.
    const sandboxTmp = path.join("/tmp", `claude-${process.getuid()}`);
    assert.equal(isBrokerSessionPath(path.join(sandboxTmp, "cxc-abc123", "broker.sock")), true, "the sandbox tmp root counts too");
    assert.equal(isBrokerSessionDir(path.join(sandboxTmp, "cxc-abc123")), true);
    assert.equal(isBrokerSessionPath(path.join(sandboxTmp, "other-abc123", "broker.sock")), false);
  }
  for (const bad of [null, undefined, "", 42]) {
    assert.equal(isBrokerSessionPath(bad), false);
    assert.equal(isBrokerSessionDir(bad), false);
  }

  const victim = makeTempDir("codex-victim-");
  const victimSock = path.join(victim, "y.sock");
  const victimPid = path.join(victim, "x.pid");
  const victimLog = path.join(victim, "x.log");
  const victimDir = path.join(victim, "emptydir");
  for (const file of [victimSock, victimPid, victimLog]) fs.writeFileSync(file, "1\n", "utf8");
  fs.mkdirSync(victimDir);
  let killed = null;
  teardownBrokerSession({
    endpoint: `unix:${victimSock}`,
    pidFile: victimPid,
    logFile: victimLog,
    sessionDir: victimDir,
    pid: 1,
    killProcess: (pid) => {
      killed = pid;
    }
  });
  for (const file of [victimSock, victimPid, victimLog, victimDir]) {
    assert.equal(fs.existsSync(file), true, `${file} is outside any session dir and must be left alone`);
  }
  assert.equal(killed, null, "a pid whose pid file is not ours is never signalled");
});

test("teardown refuses to unlink through a symlinked cxc-* entry", async () => {
  // The lexical check passes `<tmp>/cxc-link/broker.sock`; without an lstat
  // every unlink lands in whatever `cxc-link` points at. Under a redirected
  // TMPDIR so the link itself is created inside the suite's temp dirs.
  await withTmpDir(async () => {
    const victim = makeTempDir("codex-victim-");
    const link = path.join(os.tmpdir(), "cxc-link");
    fs.symlinkSync(victim, link);
    const files = ["broker.sock", "broker.pid", "broker.log"].map((name) => path.join(victim, name));
    for (const file of files) fs.writeFileSync(file, "4242\n", "utf8");

    assert.equal(isBrokerSessionDir(link), false, "a symlinked session dir is not ours");
    assert.equal(isBrokerSessionPath(path.join(link, "broker.pid")), false, "nor is anything inside it");
    let killed = null;
    teardownBrokerSession({
      endpoint: `unix:${path.join(link, "broker.sock")}`,
      pidFile: path.join(link, "broker.pid"),
      logFile: path.join(link, "broker.log"),
      sessionDir: link,
      pid: 4242,
      killProcess: (pid) => {
        killed = pid;
      }
    });
    for (const file of files) {
      assert.equal(fs.existsSync(file), true, `${file} was reached through a symlink and must be left alone`);
    }
    assert.equal(fs.lstatSync(link).isSymbolicLink(), true, "the link itself stays too");
    assert.equal(killed, null, "a pid file behind a symlink is no evidence for the pid");
  });
});

test("teardown never signals pid 0, even when an empty pid file 'matches' it", () => {
  // `Number("")` is 0: a record with `pid: 0` beside an empty pid file used to
  // pass the pid-file check and `kill(0)` the hook's own process group.
  const sessionDir = makeTempDir("cxc-test-");
  const pidFile = path.join(sessionDir, "broker.pid");
  fs.writeFileSync(pidFile, "", "utf8");
  const calls = [];
  teardownBrokerSession({ pidFile, pid: 0, killProcess: (pid) => calls.push(pid) });
  assert.deepEqual(calls, [], "pid 0 is the caller's own process group, never a broker");
  assert.equal(fs.existsSync(pidFile), false, "the empty pid file is still removed");

  fs.mkdirSync(sessionDir);
  fs.writeFileSync(pidFile, "", "utf8");
  teardownBrokerSession({ pidFile, pid: 4242, killProcess: (pid) => calls.push(pid) });
  assert.deepEqual(calls, [], "an empty pid file names nobody");
  assert.equal(terminateProcessTree(0).attempted, false, "the process-tree twin refuses pid 0 as well");
  assert.equal(terminateProcessTree(-1).attempted, false);
});

test("teardown signals the recorded pid only while its pid file still names it", () => {
  // The broker unlinks its pid file on every clean exit, so a stale record's
  // pid has very likely been reused; SessionEnd used to signal it anyway.
  const sessionDir = makeTempDir("cxc-test-");
  const pidFile = path.join(sessionDir, "broker.pid");
  const calls = [];
  const killProcess = (pid) => calls.push(pid);

  teardownBrokerSession({ pidFile, pid: 4242, killProcess });
  assert.deepEqual(calls, [], "no pid file: the broker exited cleanly, the pid is stale");
  assert.equal(fs.existsSync(sessionDir), false, "an emptied session dir is removed");

  fs.mkdirSync(sessionDir);
  fs.writeFileSync(pidFile, "9999\n", "utf8");
  teardownBrokerSession({ pidFile, pid: 4242, killProcess });
  assert.deepEqual(calls, [], "a pid file naming another process is not evidence for this pid");
  assert.equal(fs.existsSync(pidFile), false, "the stale pid file is still removed");

  fs.mkdirSync(sessionDir);
  fs.writeFileSync(pidFile, "4242\n", "utf8");
  teardownBrokerSession({ pidFile, pid: 4242, killProcess });
  assert.deepEqual(calls, [4242], "a pid file that still names the pid is the crashed-broker case the kill exists for");
  assert.equal(fs.existsSync(sessionDir), false);
});

test("a broker that exits on its own clears its own record", POSIX_ONLY, async () => {
  // Verified: after an idle exit `broker.json` kept `pid` + `endpoint`, so
  // `getCodexAuthStatus` dialled a dead socket and SessionEnd signalled a
  // stale pid.
  await withPluginData(async () => {
    const binDir = makeTempDir("codex-broker-bin-");
    installFakeCodex(binDir);
    const cwd = makeTempDir("codex-broker-cwd-");
    const env = { ...buildEnv(binDir), [BROKER_IDLE_MS_ENV]: String(TEST_IDLE_MS) };

    const session = await ensureBrokerSession(cwd, { env });
    try {
      assert.ok(session?.pid, "the broker did not start");
      assert.ok(loadBrokerSession(cwd), "the record exists while the broker runs");
      assert.ok(await until(() => !alive(session.pid), EXIT_BUDGET_MS), "the broker never idled out");
      assert.equal(loadBrokerSession(cwd), null, "an exited broker must not leave its record behind");
      assert.equal(fs.existsSync(session.pidFile), false);
    } finally {
      if (session) teardownBrokerSession({ ...session, killProcess: terminateProcessTree });
    }
  });
});

test("connect with reuseExistingBroker never dials a recorded endpoint nobody listens on", POSIX_ONLY, async () => {
  await withPluginData(async () => {
    const binDir = makeTempDir("codex-broker-bin-");
    installFakeCodex(binDir);
    const cwd = makeTempDir("codex-broker-cwd-");
    const sessionDir = makeTempDir("cxc-test-");
    // A record whose broker is gone — the shape a self-exit used to leave.
    saveBrokerSession(cwd, { endpoint: createBrokerEndpoint(sessionDir), sessionDir, pid: 1 });

    const client = await CodexAppServerClient.connect(cwd, { reuseExistingBroker: true, env: buildEnv(binDir) });
    try {
      assert.equal(client.transport, "direct", "a dead record means no broker: the direct path, not a connect ENOENT");
    } finally {
      await client.close();
    }
  });
});

test("the SessionStart hook runs the sweep and still exports its session vars", async () => {
  const pluginData = makeTempDir("codex-plugin-data-");
  const root = path.join(pluginData, "state");
  const liveCwd = makeTempDir("codex-broker-live-");
  // Endpoints that point at nothing: `sendBrokerShutdown` must survive a stale
  // socket path, and the hook must never fail because of one.
  const orphanFile = plantRecord(root, "snap-gateb-orphan-2222222222222222", {
    endpoint: "unix:/tmp/codex-orphan-hook.sock",
    cwd: path.join(root, "snapshot-that-was-deleted")
  });
  const liveFile = plantRecord(root, "live-3333333333333333", { endpoint: "unix:/tmp/codex-live-hook.sock", cwd: liveCwd });

  const envFile = path.join(makeTempDir("codex-hook-env-"), "env.sh");
  fs.writeFileSync(envFile, "", "utf8");

  const started = Date.now();
  // The real hook, as a subprocess — so its candidate roots are pinned to this
  // test's own via the test-only override: the default list includes the
  // hardcoded `/tmp/claude-<uid>` root, and no TMPDIR redirect moves that one.
  const result = run("node", [HOOK_SCRIPT, "SessionStart"], {
    cwd: liveCwd,
    env: { ...process.env, CLAUDE_PLUGIN_DATA: pluginData, CLAUDE_ENV_FILE: envFile, [SWEEP_ROOTS_ENV]: root },
    input: JSON.stringify({
      hook_event_name: "SessionStart",
      session_id: "session-abc",
      transcript_path: "/tmp/transcript.jsonl",
      cwd: liveCwd
    })
  });

  assert.equal(result.status, 0, `SessionStart must never fail on the sweep: ${result.stderr}`);
  // hooks.json gives this hook 5s; the sweep's budget has to leave room.
  assert.ok(Date.now() - started < 5000, "the SessionStart hook must stay inside its 5s hook timeout");
  assert.equal(fs.existsSync(orphanFile), false, "SessionStart must sweep a session whose cwd is gone");
  assert.equal(fs.existsSync(liveFile), true, "SessionStart must not sweep a session whose cwd still exists");

  const exported = fs.readFileSync(envFile, "utf8");
  assert.match(exported, /CODEX_COMPANION_SESSION_ID='session-abc'/, "SessionStart's existing exports must survive");
  assert.match(exported, /CODEX_COMPANION_TRANSCRIPT_PATH='\/tmp\/transcript\.jsonl'/);
});

test("the broker idle limit is env-overridable and defaults on anything but a plain integer at or above the floor", () => {
  assert.equal(resolveBrokerIdleMs({}), DEFAULT_BROKER_IDLE_MS);
  assert.equal(resolveBrokerIdleMs({ [BROKER_IDLE_MS_ENV]: "1500" }), 1500);
  assert.equal(resolveBrokerIdleMs({ [BROKER_IDLE_MS_ENV]: String(MIN_BROKER_IDLE_MS) }), MIN_BROKER_IDLE_MS);
  for (const bad of ["", "0", "-1", "1.5", "abc", "Infinity", "NaN", "999", "+1500", " 1500", "1e3", "0x600"]) {
    assert.equal(
      resolveBrokerIdleMs({ [BROKER_IDLE_MS_ENV]: bad }),
      DEFAULT_BROKER_IDLE_MS,
      `${JSON.stringify(bad)} must fall back to the default rather than disable or shorten the watchdog`
    );
  }
});
