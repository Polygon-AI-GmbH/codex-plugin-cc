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

// Churns the heap so an unreferenced guard is actually collected rather than
// merely collectABLE — without this the retention could be deleted and this
// suite would still pass, which is exactly what happened.
const REPORT = `
console.log(String(child.pid));
setTimeout(() => {}, 30000);
`;

// The REAL watchdog, imported and called — not a copy of it. A hand-written
// duplicate of the shell program is what this file used to carry, and it had
// silently diverged from production on three axes (bare `sh`, no pinned env, no
// single-pid fallback), so the test kept passing against a program the plugin
// no longer ships. Importing means the SIGKILL case below pins whatever
// `spawnParentDeathWatchdog` actually does today.
const APP_SERVER_URL =
  new URL("../plugins/codex/scripts/lib/app-server.mjs", import.meta.url).href;

const REAL_WATCHDOG_AND_REPORT = `
import(${JSON.stringify(APP_SERVER_URL)}).then((mod) => {
  // Deliberately NOT held here. Production retention is the module's own job,
  // and a global in the fixture would mask its absence — it did: dropping that
  // retention passed this suite green until the reference came out. The guard's
  // stdin pipe is its lifetime signal, so a collected handle fires it early and
  // kills a live app-server.
  if (!mod.spawnParentDeathWatchdog(child.pid)) throw new Error("watchdog refused to start");
  console.log(String(child.pid));
  setTimeout(() => {}, 30000);
}).catch((error) => { console.error(error); process.exit(1); });
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
  const { parentPid, childPid } = await runParent(REAPER + REAL_WATCHDOG_AND_REPORT);
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


test("the watchdog refuses a pid it must never signal", { skip: process.platform === "win32" ? "no process groups" : false }, async () => {
  // Two separate hazards. `undefined`/`NaN`: a failed `spawn("codex")` leaves
  // pid undefined, and on that path the child's `exit` never fires, so nothing
  // would ever kill a guard started for it — it would hold its pipe for the
  // whole companion lifetime. `1`, `0` and negatives: `kill -9 -1` is POSIX for
  // "every process this uid may signal", so a guard built for pid 1 would take
  // the developer's whole session down when its parent died.
  //
  // Skipped on win32 rather than run: the function returns null for EVERY input
  // there, so the assertions would pass without exercising the gate at all and
  // would stay green if it were deleted outright.
  const { spawnParentDeathWatchdog } = await import(APP_SERVER_URL);
  for (const bad of [undefined, NaN, null, "123", 1.5, 1, 0, -1]) {
    assert.equal(spawnParentDeathWatchdog(bad), null,
      `pid ${String(bad)} must be refused`);
  }
});

test("no uncaughtException handler is installed — it would suppress the reaper", async () => {
  // The rethrowing handler this module used to carry DID reap — its first
  // statement ran — but installing any uncaughtException listener suppresses
  // every `exit` listener, and the rethrow turned exit 1 into exit 7. This pins
  // the deletion: `exit` already runs the reaper on an uncaught exception, so
  // the handler bought nothing and cost both of those.
  const source = [
    `import(${JSON.stringify(APP_SERVER_URL)}).then((mod) => {`,
    `  mod.installReaper();`,
    `  process.on("exit", (code) => process.stderr.write("EXIT-RAN code=" + code + "\\n"));`,
    `  console.log(String(process.listenerCount("uncaughtException")));`,
    `  setTimeout(() => { throw new Error("boom"); }, 10);`,
    `}).catch((error) => { console.error(error); process.exit(3); });`
  ].join("\n");
  const proc = spawn(process.execPath, ["-e", source], { stdio: ["ignore", "pipe", "pipe"] });
  let out = "";
  let err = "";
  proc.stdout.setEncoding("utf8");
  proc.stderr.setEncoding("utf8");
  proc.stdout.on("data", (chunk) => { out += chunk; });
  proc.stderr.on("data", (chunk) => { err += chunk; });
  const code = await new Promise((resolve) => proc.once("exit", resolve));
  assert.equal(out.trim(), "0",
    `app-server.mjs must install no uncaughtException listener (stderr: ${err})`);
  assert.match(err, /EXIT-RAN code=1/,
    "the exit listener must still run on an uncaught exception, and the code stay 1");
  assert.equal(code, 1, "exit 7 means an uncaughtException handler rethrew");
});


test("the watchdog stays SILENT while the dispatcher is alive", async () => {
  // The other half of the pipe-EOF contract, and the dangerous one. Every test
  // above asks "does the guard fire when it should"; none asked "does it stay
  // quiet when it must". Mutation-checked: turning the guard's stdio back into
  // `"ignore"` gives it /dev/null on fd 0, so `read` hits EOF immediately and
  // it SIGKILLs a perfectly healthy app-server group ~100ms after startup —
  // and the whole suite still passed, because the liveness assertion in the
  // SIGKILL test races the guard and loses. Dropping `liveWatchdogs` and the
  // stdin `unref()` together passed too.
  //
  // So: hold the parent alive well past any plausible early-fire window and
  // assert the child is STILL running. That is the assertion those mutations
  // cannot survive.
  const { parentPid, childPid, parent } = await runParent(REAPER + REAL_WATCHDOG_AND_REPORT);
  try {
    assert.ok(alive(childPid), "the stand-in child should be running at startup");
    await delay(2500);
    assert.equal(alive(childPid), true,
      "the app-server must survive while its dispatcher lives — a guard that " +
      "reads EOF from a closed or /dev/null stdin kills it within ~100ms");
    assert.ok(alive(parentPid), "the dispatcher itself must still be alive");
  } finally {
    parent.kill("SIGKILL");
    await delay(1500);
    if (alive(childPid)) process.kill(childPid, "SIGKILL");
  }
});
