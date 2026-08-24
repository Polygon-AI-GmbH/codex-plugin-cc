import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";

import { makeTempDir } from "./helpers.mjs";
import { ensureStateDir, resolveJobFile, resolveJobLogFile, resolveStateDir, resolveStateFile, saveState } from "../plugins/codex/scripts/lib/state.mjs";

test("resolveStateDir uses a temp-backed per-workspace directory", () => {
  const workspace = makeTempDir();
  const stateDir = resolveStateDir(workspace);

  assert.equal(stateDir.startsWith(os.tmpdir()), true);
  assert.match(path.basename(stateDir), /.+-[a-f0-9]{16}$/);
  assert.match(stateDir, new RegExp(`^${os.tmpdir().replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`));
});

test("resolveStateDir uses CLAUDE_PLUGIN_DATA when it is provided", () => {
  const workspace = makeTempDir();
  const pluginDataDir = makeTempDir();
  const previousPluginDataDir = process.env.CLAUDE_PLUGIN_DATA;
  process.env.CLAUDE_PLUGIN_DATA = pluginDataDir;

  try {
    const stateDir = resolveStateDir(workspace);

    assert.equal(stateDir.startsWith(path.join(pluginDataDir, "state")), true);
    assert.match(path.basename(stateDir), /.+-[a-f0-9]{16}$/);
    assert.match(
      stateDir,
      new RegExp(`^${path.join(pluginDataDir, "state").replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`)
    );
  } finally {
    if (previousPluginDataDir == null) {
      delete process.env.CLAUDE_PLUGIN_DATA;
    } else {
      process.env.CLAUDE_PLUGIN_DATA = previousPluginDataDir;
    }
  }
});

test("resolveStateDir falls back to the temp root when CLAUDE_PLUGIN_DATA is not writable", {
  // chmod 0o500 is a no-op for uid 0 — access(W_OK) always succeeds for root —
  // so this would fail in a root container. CI runs non-root; skip elsewhere.
  skip:
    process.platform === "win32"
      ? "chmod cannot make a directory unwritable on Windows"
      : process.getuid?.() === 0
        ? "chmod cannot make a dir unwritable for root"
        : false
}, () => {
  // 0o500 reproduces the sandbox shape: readable, not writable. The mechanism
  // and why it cannot be fixed from settings live in state.mjs's own comment.
  const workspace = makeTempDir();
  const pluginDataDir = makeTempDir();
  const previousPluginDataDir = process.env.CLAUDE_PLUGIN_DATA;
  fs.chmodSync(pluginDataDir, 0o500); // readable, not writable — the sandbox shape
  process.env.CLAUDE_PLUGIN_DATA = pluginDataDir;

  try {
    const stateDir = resolveStateDir(workspace);

    assert.equal(
      stateDir.startsWith(pluginDataDir),
      false,
      "state dir must not stay under an unwritable CLAUDE_PLUGIN_DATA"
    );
    // Full root, not a prefix: `codex-companion` alone also matches the old
    // non-namespaced root, so a revert of the per-uid split would go unnoticed.
    assert.equal(
      stateDir.startsWith(path.join(os.tmpdir(), `codex-companion-${process.getuid()}`)),
      true
    );
    assert.match(path.basename(stateDir), /.+-[a-f0-9]{16}$/);

    // The whole point: the directory can actually be created now.
    ensureStateDir(workspace);
    assert.equal(fs.existsSync(path.join(stateDir, "jobs")), true);

    // And it is created 0700 — the fallback root may sit in a shared /tmp, and
    // job files carry prompts, diffs and Codex output. Without this assertion
    // the mode silently reverts to umask default the next time anyone edits
    // ensureStateDir.
    assert.equal(fs.statSync(path.join(stateDir, "jobs")).mode & 0o777, 0o700);
  } finally {
    fs.chmodSync(pluginDataDir, 0o700);
    if (previousPluginDataDir == null) {
      delete process.env.CLAUDE_PLUGIN_DATA;
    } else {
      process.env.CLAUDE_PLUGIN_DATA = previousPluginDataDir;
    }
  }
});

test("resolveStateDir keeps a writable CLAUDE_PLUGIN_DATA rather than falling back", () => {
  // Guards the other direction: the fallback must be reached only on a real
  // permission failure, or every unsandboxed run silently loses its job history
  // to a temp dir that the OS may clear between sessions.
  const workspace = makeTempDir();
  const pluginDataDir = makeTempDir();
  const previousPluginDataDir = process.env.CLAUDE_PLUGIN_DATA;
  process.env.CLAUDE_PLUGIN_DATA = pluginDataDir;

  try {
    const stateDir = resolveStateDir(workspace);
    assert.equal(stateDir.startsWith(path.join(pluginDataDir, "state")), true);
    ensureStateDir(workspace);
    assert.equal(fs.existsSync(path.join(stateDir, "jobs")), true);
  } finally {
    if (previousPluginDataDir == null) {
      delete process.env.CLAUDE_PLUGIN_DATA;
    } else {
      process.env.CLAUDE_PLUGIN_DATA = previousPluginDataDir;
    }
  }
});

test("resolveStateDir falls back when the state root is writable but not searchable", {
  skip:
    process.platform === "win32"
      ? "POSIX permission bits"
      : process.getuid?.() === 0
        ? "root bypasses X_OK"
        : false
}, () => {
  // Mode 0200 passes W_OK but not X_OK: writable, yet its children cannot be
  // reached. Probing W_OK alone selected this unusable root, and the read-only
  // consumers (status, the stop hook, loadBrokerSession) never reach
  // ensureStateDir's retry — they would report empty state, and a later write
  // could clobber real jobs recorded in the fallback.
  const workspace = makeTempDir();
  const pluginDataDir = makeTempDir();
  const previousPluginDataDir = process.env.CLAUDE_PLUGIN_DATA;
  fs.chmodSync(pluginDataDir, 0o200);
  process.env.CLAUDE_PLUGIN_DATA = pluginDataDir;

  try {
    const stateDir = resolveStateDir(workspace);
    assert.equal(
      stateDir.startsWith(pluginDataDir),
      false,
      "a write-only, non-searchable root must not be selected"
    );
    assert.equal(
      stateDir.startsWith(path.join(os.tmpdir(), `codex-companion-${process.getuid()}`)),
      true
    );
  } finally {
    fs.chmodSync(pluginDataDir, 0o700);
    if (previousPluginDataDir == null) delete process.env.CLAUDE_PLUGIN_DATA;
    else process.env.CLAUDE_PLUGIN_DATA = previousPluginDataDir;
  }
});

test("ensureStateDir refuses a fallback root this user does not privately own", {
  skip:
    process.platform === "win32"
      ? "POSIX ownership/mode semantics"
      : process.getuid?.() === 0
        ? "root passes every ownership check"
        : false
}, () => {
  // The fallback root can live in a shared /tmp. `mode:` on mkdirSync does NOT
  // cover this: it is ignored for an existing directory and mkdirSync follows
  // symlinks (both verified), so a pre-created 0777 dir or symlink would take
  // every prompt, diff and Codex output — and let its owner rewrite a stored
  // job's request to run their own prompt as us.
  //
  // TMPDIR is redirected so this never touches the real machine-shared root:
  // `node --test tests/*.test.mjs` runs files in parallel, and the companion
  // subprocesses that sibling files spawn resolve that same root.
  const workspace = makeTempDir();
  const pluginDataDir = makeTempDir();
  const tmpHome = makeTempDir();
  const previousPluginDataDir = process.env.CLAUDE_PLUGIN_DATA;
  const previousTmpDir = process.env.TMPDIR;

  process.env.TMPDIR = tmpHome;
  const fallbackRoot = path.join(tmpHome, `codex-companion-${process.getuid()}`);
  fs.mkdirSync(fallbackRoot, { recursive: true, mode: 0o700 });
  fs.chmodSync(fallbackRoot, 0o777); // group/other access — the hostile shape
  fs.chmodSync(pluginDataDir, 0o500); // force the fallback branch
  process.env.CLAUDE_PLUGIN_DATA = pluginDataDir;

  try {
    assert.throws(() => ensureStateDir(workspace), /not a private directory/);
  } finally {
    fs.chmodSync(pluginDataDir, 0o700);
    if (previousTmpDir == null) delete process.env.TMPDIR;
    else process.env.TMPDIR = previousTmpDir;
    if (previousPluginDataDir == null) delete process.env.CLAUDE_PLUGIN_DATA;
    else process.env.CLAUDE_PLUGIN_DATA = previousPluginDataDir;
  }
});

test("ensureStateDir validates the fallback root even when CLAUDE_PLUGIN_DATA is unset", {
  skip:
    process.platform === "win32"
      ? "POSIX ownership/mode semantics"
      : process.getuid?.() === 0
        ? "root passes every ownership check"
        : false
}, () => {
  // The unset branch is where the fallback is the DEFAULT, not the exception —
  // and it early-returned without validating, so the hostile-root check was
  // bypassed on exactly the path that uses it most. A plain shell invocation
  // (the wait-loop the docs prescribe) has no CLAUDE_PLUGIN_DATA.
  const workspace = makeTempDir();
  const tmpHome = makeTempDir();
  const previousPluginDataDir = process.env.CLAUDE_PLUGIN_DATA;
  const previousTmpDir = process.env.TMPDIR;

  process.env.TMPDIR = tmpHome;
  delete process.env.CLAUDE_PLUGIN_DATA;
  const fallbackRoot = path.join(tmpHome, `codex-companion-${process.getuid()}`);
  fs.mkdirSync(fallbackRoot, { recursive: true, mode: 0o700 });
  fs.chmodSync(fallbackRoot, 0o777);

  try {
    assert.throws(() => ensureStateDir(workspace), /not a private directory/);
  } finally {
    if (previousTmpDir == null) delete process.env.TMPDIR;
    else process.env.TMPDIR = previousTmpDir;
    if (previousPluginDataDir != null) process.env.CLAUDE_PLUGIN_DATA = previousPluginDataDir;
  }
});

test("ensureStateDir reports a hostile fallback root planted as a file, not a raw mkdir errno", {
  skip: process.platform === "win32" ? "POSIX semantics" : false
}, () => {
  // A regular file (or dangling symlink) at the root makes mkdir throw
  // EEXIST/ENOENT before any check runs. Uncaught, that exits 1 with no
  // sentinel — the opaque "Codex is missing" failure this change exists to end.
  const workspace = makeTempDir();
  const tmpHome = makeTempDir();
  const previousTmpDir = process.env.TMPDIR;
  const previousPluginDataDir = process.env.CLAUDE_PLUGIN_DATA;

  process.env.TMPDIR = tmpHome;
  delete process.env.CLAUDE_PLUGIN_DATA;
  fs.writeFileSync(path.join(tmpHome, `codex-companion-${process.getuid?.() ?? "shared"}`), "x");

  try {
    assert.throws(() => ensureStateDir(workspace), /not a private directory/);
  } finally {
    if (previousTmpDir == null) delete process.env.TMPDIR;
    else process.env.TMPDIR = previousTmpDir;
    if (previousPluginDataDir != null) process.env.CLAUDE_PLUGIN_DATA = previousPluginDataDir;
  }
});

test("saveState prunes dropped job artifacts when indexed jobs exceed the cap", () => {
  const workspace = makeTempDir();
  const stateFile = resolveStateFile(workspace);
  fs.mkdirSync(path.dirname(stateFile), { recursive: true });

  const jobs = Array.from({ length: 51 }, (_, index) => {
    const jobId = `job-${index}`;
    const updatedAt = new Date(Date.UTC(2026, 0, 1, 0, index, 0)).toISOString();
    const logFile = resolveJobLogFile(workspace, jobId);
    const jobFile = resolveJobFile(workspace, jobId);
    fs.writeFileSync(logFile, `log ${jobId}\n`, "utf8");
    fs.writeFileSync(jobFile, JSON.stringify({ id: jobId, status: "completed" }, null, 2), "utf8");
    return {
      id: jobId,
      status: "completed",
      logFile,
      updatedAt,
      createdAt: updatedAt
    };
  });

  fs.writeFileSync(
    stateFile,
    `${JSON.stringify(
      {
        version: 1,
        config: { stopReviewGate: false },
        jobs
      },
      null,
      2
    )}\n`,
    "utf8"
  );

  saveState(workspace, {
    version: 1,
    config: { stopReviewGate: false },
    jobs
  });

  const prunedJobFile = resolveJobFile(workspace, "job-0");
  const prunedLogFile = resolveJobLogFile(workspace, "job-0");
  const retainedJobFile = resolveJobFile(workspace, "job-50");
  const retainedLogFile = resolveJobLogFile(workspace, "job-50");
  const jobsDir = path.dirname(prunedJobFile);

  assert.equal(fs.existsSync(retainedJobFile), true);
  assert.equal(fs.existsSync(retainedLogFile), true);

  const savedState = JSON.parse(fs.readFileSync(stateFile, "utf8"));
  assert.equal(savedState.jobs.length, 50);
  assert.deepEqual(
    savedState.jobs.map((job) => job.id),
    Array.from({ length: 50 }, (_, index) => `job-${50 - index}`)
  );
  assert.deepEqual(
    fs.readdirSync(jobsDir).sort(),
    Array.from({ length: 50 }, (_, index) => `job-${index + 1}`)
      .flatMap((jobId) => [`${jobId}.json`, `${jobId}.log`])
      .sort()
  );
});
