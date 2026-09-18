import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { spawnSync } from "node:child_process";
import { after } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

// Claude Code exports CODEX_COMPANION_SESSION_ID / _TRANSCRIPT_PATH into every
// shell it runs, so a developer running this suite from inside a session
// inherits a live session id. Job lookups that default to "the current session"
// then filter out every fixture job, and three status/result tests fail with
// "No finished Codex jobs found" — green in CI, red on the machine that wrote
// the code. Scrubbed at module scope, not inside run(): the tests that build an
// env with `{...process.env, PATH: binDir}` have already copied the ambient
// value in, so by the time run() sees it an inherited value is indistinguishable
// from a deliberate one. Deleting here makes every spread come out clean while
// a test that assigns the variable explicitly still wins (a static import is
// evaluated before the importing module's body).
//
// The whole PREFIX, not the two names that happened to bite: there are five,
// and CODEX_COMPANION_APP_SERVER_ENDPOINT is the dangerous one — `connect()`
// reads it ahead of every fallback, so a developer with a live broker would
// have the suite talk to their REAL broker socket and write fixture jobs into
// their real state tree, nondeterministically.
for (const key of Object.keys(process.env)) {
  if (key.startsWith("CODEX_COMPANION_")) delete process.env[key];
}

// Every directory this hands out, so `cleanupTempDirs` can take them — and
// anything they started — back down. Module state is per FILE: node's test
// runner gives each test file its own process.
const tempDirs = new Set();

export function makeTempDir(prefix = "codex-plugin-test-") {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tempDirs.add(dir);
  return dir;
}

/**
 * Stop what the temp workspaces started, then remove them.
 *
 * Running the companion against a workspace spawns a DETACHED, unref'd broker
 * for it, and a temp dir that still exists is one none of the product's own
 * defences (lib/broker-watchdog.mjs, the SessionStart sweep) can reap — so a
 * test that starts a broker has to stop it. Registered below as a file-level
 * `after`, so it runs even when a test fails partway through.
 */
export async function cleanupTempDirs() {
  if (tempDirs.size === 0) {
    return;
  }
  // Dynamic, so the CODEX_COMPANION_* scrub above runs before anything reads them.
  const { loadBrokerSession } = await import("../plugins/codex/scripts/lib/broker-lifecycle.mjs");
  const { resolveStateDir } = await import("../plugins/codex/scripts/lib/state.mjs");

  // Brokers first, removal second. A tracked plugin-data root holds the
  // records for OTHER tracked workspaces, and it is tracked before them — so
  // removing dirs in order would delete a record before its broker is found.
  for (const dir of tempDirs) {
    await stopBrokersUnderPluginData(dir);
  }

  for (const dir of tempDirs) {
    // One failure must never abort the loop or reject the `after` hook: every
    // directory that follows would be leaked, brokers included.
    try {
      // Resolved BEFORE the workspace disappears: resolveStateDir hashes the
      // realpath, which it cannot read once the directory is gone.
      let stateDir = null;
      try {
        stateDir = resolveStateDir(dir);
      } catch {
        // Not resolvable; there is nothing recorded for it either.
      }

      // resolveStateDir walks UP to the enclosing git root, so a TMPDIR that
      // ever sat inside a repository would resolve to THAT repository's real
      // state — and we would shut down the developer's live broker and delete
      // their job history. The slug is the workspace basename, so requiring it
      // to match this temp dir is what proves the state we are about to
      // destroy is ours.
      const owned = stateDir !== null && path.basename(stateDir).startsWith(`${path.basename(dir)}-`);

      if (owned) {
        let session = null;
        try {
          session = loadBrokerSession(dir);
        } catch {
          // Unreadable or absent — nothing to shut down.
        }
        await stopRecordedBroker(session);
        fs.rmSync(stateDir, { recursive: true, force: true });
      } else if (stateDir !== null) {
        // Not skipped silently: a TMPDIR inside a repository is a suite
        // misconfiguration, and this is the only place it shows.
        console.warn(`[tests] not touching state for ${dir}: ${stateDir} does not look like this temp dir's`);
      }

      fs.rmSync(dir, { recursive: true, force: true });
    } catch (error) {
      console.warn(`[tests] cleanup of ${dir} failed: ${error?.message ?? error}`);
    }
  }

  tempDirs.clear();
}

/**
 * Shut down and tear down one recorded broker session. The teardown signals
 * the recorded pid only while its pid file still names it, so a broker that
 * already exited on its own is never mistaken for a reused pid.
 */
async function stopRecordedBroker(session) {
  if (!session) {
    return;
  }
  const { sendBrokerShutdown, teardownBrokerSession } = await import("../plugins/codex/scripts/lib/broker-lifecycle.mjs");
  const { terminateProcessTree } = await import("../plugins/codex/scripts/lib/process.mjs");
  try {
    if (session.endpoint) {
      await sendBrokerShutdown(session.endpoint, { timeoutMs: 500 });
    }
    teardownBrokerSession({ ...session, killProcess: terminateProcessTree });
  } catch {
    // Best effort: one stuck broker must not skip the rest of the cleanup.
  }
}

/**
 * Stop every broker recorded under a plugin-data root's `state/`. The sweep
 * tests start real brokers keyed under a temporary CLAUDE_PLUGIN_DATA; once
 * the env is restored their records are unreachable from `cleanupTempDirs`.
 */
async function stopBrokersUnderPluginData(pluginDataDir) {
  const root = path.join(pluginDataDir, "state");
  let entries = [];
  try {
    entries = fs.readdirSync(root, { withFileTypes: true });
  } catch {
    return; // Never created — nothing ran under it.
  }
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const recordFile = path.join(root, entry.name, "broker.json");
    let session = null;
    try {
      session = JSON.parse(fs.readFileSync(recordFile, "utf8"));
    } catch {
      continue;
    }
    await stopRecordedBroker(session);
    fs.rmSync(recordFile, { force: true });
  }
}

// Once per test file: node's test runner gives each file its own process, so
// this module — and this hook — is evaluated once per file.
after(cleanupTempDirs);

/** `kill -0`: is there a process with this pid we may signal? */
export function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Poll `predicate` until it holds or `timeoutMs` passes; never throws. */
export async function until(predicate, timeoutMs, stepMs = 50) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await delay(stepMs);
  }
}

/**
 * Run `fn` with CLAUDE_PLUGIN_DATA pointed at a fresh temp dir, then restore it.
 * Every broker recorded under that root is stopped BEFORE the env is restored:
 * the root is a tracked temp dir, but once the variable changes nothing can
 * find the records under it, so the brokers would outlive the suite.
 */
export function withPluginData(fn) {
  const previous = process.env.CLAUDE_PLUGIN_DATA;
  const pluginDataDir = makeTempDir("codex-plugin-data-");
  process.env.CLAUDE_PLUGIN_DATA = pluginDataDir;
  return Promise.resolve()
    .then(fn)
    .finally(async () => {
      try {
        await stopBrokersUnderPluginData(pluginDataDir);
      } finally {
        if (previous == null) delete process.env.CLAUDE_PLUGIN_DATA;
        else process.env.CLAUDE_PLUGIN_DATA = previous;
      }
    });
}

export function writeExecutable(filePath, source) {
  fs.writeFileSync(filePath, source, { encoding: "utf8", mode: 0o755 });
}

export function run(command, args, options = {}) {
  return spawnSync(command, args, {
    cwd: options.cwd,
    env: options.env,
    encoding: "utf8",
    input: options.input,
    shell: options.shell ?? (process.platform === "win32" && !path.isAbsolute(command)),
    windowsHide: true
  });
}

export function initGitRepo(cwd) {
  run("git", ["init", "-b", "main"], { cwd });
  run("git", ["config", "user.name", "Codex Plugin Tests"], { cwd });
  run("git", ["config", "user.email", "tests@example.com"], { cwd });
  run("git", ["config", "commit.gpgsign", "false"], { cwd });
  run("git", ["config", "tag.gpgsign", "false"], { cwd });
}
