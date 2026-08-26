/**
 * The `codex app-server` orphan leak.
 *
 * Cleanup used to live only in `close()`, on the graceful path. A dispatcher
 * killed by its harness's foreground timeout never reached it, and because the
 * child was not detached it outlived the parent rather than dying with it. They
 * accumulated one per killed dispatch until reboot — observed on a developer
 * machine as 141 orphaned app-servers holding 6.1 GB, the oldest over two days
 * old, each pinning a broker socket that `/codex:cancel` could no longer reach.
 *
 * These tests drive the real shapes from `lib/app-server.mjs` against a stand-in
 * child, because spawning a real `codex app-server` in a unit test is neither
 * hermetic nor fast. What is under test is the LIFECYCLE, which is ours.
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";

const alive = (pid) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

/** Run `source` as a detached node process; return its pid and the child's. */
async function runParent(source) {
  const parent = spawn(process.execPath, ["-e", source], {
    stdio: ["ignore", "pipe", "ignore"]
  });
  let out = "";
  parent.stdout.setEncoding("utf8");
  for await (const chunk of parent.stdout) {
    out += chunk;
    if (out.includes("\n")) break;
  }
  const childPid = Number(out.trim());
  assert.ok(childPid > 0, `parent did not report a child pid, got ${JSON.stringify(out)}`);
  return { parentPid: parent.pid, childPid, parent };
}

const REAPER = `
const { spawn } = require("node:child_process");
const live = new Set();
function reap() {
  for (const p of live) {
    try { process.kill(-p.pid, "SIGKILL"); } catch { try { p.kill("SIGKILL"); } catch {} }
  }
  live.clear();
}
process.on("exit", reap);
for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  process.on(sig, () => { reap(); process.removeAllListeners(sig); process.kill(process.pid, sig); });
}
const child = spawn("sleep", ["300"], { stdio: ["pipe","pipe","pipe"], detached: true });
live.add(child);
`;

const WATCHDOG = `
const guard = spawn("sh", ["-c",
  "while kill -0 " + process.pid + " 2>/dev/null; do sleep 1; done; kill -9 -" + child.pid + " 2>/dev/null"],
  { stdio: "ignore", detached: true });
guard.unref();
`;

const REPORT = `
console.log(String(child.pid));
setTimeout(() => {}, 30000);
`;

test("a SIGTERM'd dispatcher does not leak its app-server", async () => {
  const { parentPid, childPid } = await runParent(REAPER + REPORT);
  assert.ok(alive(childPid), "the stand-in child should be running");
  process.kill(parentPid, "SIGTERM");
  await delay(1500);
  assert.equal(alive(childPid), false,
    "the signal handlers must reap the app-server before exiting");
});

test("a SIGKILL'd dispatcher does not leak its app-server", async () => {
  // THE case that produced the orphans: the harness's foreground timeout does
  // not send SIGTERM, and no in-process handler can run on SIGKILL. Only the
  // out-of-process watchdog closes this one.
  const { parentPid, childPid } = await runParent(REAPER + WATCHDOG + REPORT);
  assert.ok(alive(childPid), "the stand-in child should be running");
  process.kill(parentPid, "SIGKILL");
  await delay(3000);
  assert.equal(alive(childPid), false,
    "the parent-death watchdog must kill the app-server when the dispatcher is SIGKILLed");
});

test("without the watchdog, a SIGKILL DOES leak — the bug this pins", async () => {
  const { parentPid, childPid } = await runParent(REAPER + REPORT);
  process.kill(parentPid, "SIGKILL");
  await delay(1500);
  const leaked = alive(childPid);
  if (leaked) process.kill(childPid, "SIGKILL");
  assert.equal(leaked, true,
    "if this fails, signal handlers alone now suffice and the watchdog's " +
    "justification needs revisiting");
});
