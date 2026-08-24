import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { resolveWorkspaceRoot } from "./workspace.mjs";

const STATE_VERSION = 1;
const PLUGIN_DATA_ENV = "CLAUDE_PLUGIN_DATA";
// Resolved lazily, not captured at module load, so a test can point TMPDIR at
// its own directory instead of mutating the real machine-shared root — doing
// that raced the companion subprocesses that sibling test files spawn.
//
// Namespaced per-uid because os.tmpdir() is the shared /tmp on Linux, and the
// fallback turned this from a rare path into the common one for every sandboxed
// session. Job files carry prompts, diffs and Codex output.
//
// `mode:` on mkdirSync is NOT sufficient alone: it is silently ignored for a
// directory that already exists, and mkdirSync follows symlinks (both verified).
// assertSafeFallbackRoot is what actually closes that, and ensureStateDir is the
// single place that calls it.
function fallbackStateRoot() {
  return path.join(os.tmpdir(), `codex-companion-${process.getuid?.() ?? "shared"}`);
}

const STATE_FILE_NAME = "state.json";
const JOBS_DIR_NAME = "jobs";
const MAX_JOBS = 50;

// Claude Code points CLAUDE_PLUGIN_DATA at a directory under ~/.claude/plugins,
// which its own sandbox denies writes to. That deny cannot be lifted from
// settings: a child `allowWrite` entry loses to the built-in parent deny (the
// permission structure is `denyWithinAllow`), so an operator has no way to opt
// in. Without the probe below, mkdir threw straight out of ensureStateDir and
// the companion exited 1 with no sentinel — surfacing as GATE-B-UNAVAILABLE,
// which reads as "Codex is missing" rather than "this path is not writable".
// Candidate root -> root actually used. One map, not two: the memo IS the
// warn-once, because the warning fires only on a cache miss. resolveStateDir
// runs several times per save (once per pruned job file), so an unmemoized
// warning would print the same line repeatedly.
const resolvedStateRoots = new Map();
const warnedRoots = new Set();

function nearestExistingAncestor(target) {
  let current = path.resolve(target);
  for (;;) {
    if (fs.existsSync(current)) return current;
    const parent = path.dirname(current);
    if (parent === current) return current;
    current = parent;
  }
}

function isWritableDir(target) {
  try {
    // Probes the nearest EXISTING ancestor because `state/` usually does not
    // exist yet, and accessSync on a missing path throws ENOENT — which would
    // read as "unwritable" and send every legitimate run to the fallback.
    //
    // The probe does not touch the filesystem, which is the point: when the
    // state dir already exists, `mkdir -p` succeeds as a no-op under the
    // sandbox while writing a file INSIDE it still fails, so a mkdir-based
    // probe would pass and then fail later mid-run. Verified that accessSync
    // reports the sandbox deny as EPERM, exactly as a real write does.
    // X_OK as well as W_OK: a write-only directory (mode 0200) passes W_OK
    // while its children cannot be reached, and read-only consumers (status,
    // the stop hook, loadBrokerSession) never reach ensureStateDir's retry —
    // they would silently read default state and a later write could clobber
    // real fallback jobs.
    fs.accessSync(
      nearestExistingAncestor(target),
      fs.constants.W_OK | fs.constants.X_OK
    );
    return true;
  } catch {
    return false;
  }
}

// "This location cannot hold our state", as opposed to a real bug. EEXIST /
// ENOTDIR / ENOENT are in here too: a botched install leaving `<data>/state` as
// a regular file, or a dangling symlink, is equally survivable by degrading —
// rethrowing reproduced the original exit-1-no-sentinel symptom on a condition
// the fallback handles trivially.
const UNWRITABLE_ERRNOS = new Set([
  "EPERM",
  "EACCES",
  "EROFS",
  "EEXIST",
  "ENOTDIR",
  "ENOENT"
]);

function hostileRootError(root, detail) {
  return new Error(
    `${root} is not a private directory owned by this user (${detail}). Refusing ` +
      `to write job state there — it would expose prompts, diffs and Codex ` +
      `output, and let its owner rewrite a stored job's request. Remove or fix ` +
      `that path, then retry.`
  );
}

function assertSafeFallbackRoot() {
  const root = fallbackStateRoot();

  // Create BEFORE validating, then validate unconditionally. Validating only on
  // the already-exists path left a race: between an ENOENT and our mkdir, an
  // attacker could create the predictable `codex-companion-<uid>` (or a symlink
  // to a directory they own), and recursive mkdir accepts an existing directory
  // silently. The worker later reads `storedJob.request` back out of this root,
  // so losing that race means an attacker-supplied prompt runs as us.
  try {
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  } catch (error) {
    // The two shapes an attacker actually plants — a regular file, or a symlink
    // to a missing target — make mkdir throw EEXIST/ENOENT/ENOTDIR before any
    // check runs. Uncaught, those exit 1 with no sentinel: exactly the opaque
    // "Codex is missing" failure this whole change exists to remove.
    if (["EEXIST", "ENOTDIR", "ENOENT"].includes(error.code)) {
      throw hostileRootError(root, `cannot be created as a directory (${error.code})`);
    }
    throw error;
  }
  const stats = fs.lstatSync(root);

  // Windows has no POSIX modes or uids: Node reports synthesized bits such as
  // 0777 for an ordinary directory, so the mode test would reject the plugin's
  // own fallback on every command after the one that created it. There
  // os.tmpdir() is the per-user %TEMP%, which is what provides the isolation.
  const posix = process.platform !== "win32" && process.getuid !== undefined;
  const hostile =
    stats.isSymbolicLink() ||
    !stats.isDirectory() ||
    (posix && stats.uid !== process.getuid()) ||
    (posix && (stats.mode & 0o077) !== 0);
  // Loud on purpose: there is no safe way to continue automatically.
  if (hostile) throw hostileRootError(root, "symlink, wrong owner, or group/other permissions");
}

// stderr, never stdout: callers parse stdout. Named cause — the whole defect was
// that an operator could not tell a permission problem from a broken install.
// Every path that relocates state routes through here, including the
// ensureStateDir backstop, which otherwise relocated silently.
function warnRelocated(candidate, resolved) {
  if (warnedRoots.has(candidate)) return;
  warnedRoots.add(candidate);
  process.stderr.write(
    `[codex] ${candidate} is not writable (sandbox or permissions); using ` +
      `${resolved} instead. Job history AND setup toggles (e.g. the stop-time ` +
      `review gate) recorded under the other root are not visible from here, so ` +
      `re-run /codex:setup options if they look unset.\n`
  );
}

// PURE: no mkdir, no throw. It sits on the SessionEnd teardown path
// (loadBrokerSession -> resolveBrokerStateFile -> here), where a throw skipped
// broker shutdown and orphaned the detached app-server process and its socket.
// Creation and validation belong to ensureStateDir, the single writer.
function resolveStateRoot(pluginDataDir) {
  if (!pluginDataDir) return fallbackStateRoot();

  const candidate = path.join(pluginDataDir, "state");
  let resolved = resolvedStateRoots.get(candidate);
  if (resolved === undefined) {
    resolved = isWritableDir(candidate) ? candidate : fallbackStateRoot();
    resolvedStateRoots.set(candidate, resolved);
  }
  if (resolved !== candidate) warnRelocated(candidate, resolved);
  return resolved;
}

function nowIso() {
  return new Date().toISOString();
}

function defaultState() {
  return {
    version: STATE_VERSION,
    config: {
      stopReviewGate: false
    },
    jobs: []
  };
}

export function resolveStateDir(cwd) {
  const workspaceRoot = resolveWorkspaceRoot(cwd);
  let canonicalWorkspaceRoot = workspaceRoot;
  try {
    canonicalWorkspaceRoot = fs.realpathSync.native(workspaceRoot);
  } catch {
    canonicalWorkspaceRoot = workspaceRoot;
  }

  const slugSource = path.basename(workspaceRoot) || "workspace";
  const slug = slugSource.replace(/[^a-zA-Z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "workspace";
  const hash = createHash("sha256").update(canonicalWorkspaceRoot).digest("hex").slice(0, 16);
  const stateRoot = resolveStateRoot(process.env[PLUGIN_DATA_ENV]);
  return path.join(stateRoot, `${slug}-${hash}`);
}

export function resolveStateFile(cwd) {
  return path.join(resolveStateDir(cwd), STATE_FILE_NAME);
}

export function resolveJobsDir(cwd) {
  return path.join(resolveStateDir(cwd), JOBS_DIR_NAME);
}

function forceFallbackRoot() {
  const pluginDataDir = process.env[PLUGIN_DATA_ENV];
  if (!pluginDataDir) return false;
  const candidate = path.join(pluginDataDir, "state");
  const fallback = fallbackStateRoot();
  if (resolvedStateRoots.get(candidate) === fallback) return false;
  assertSafeFallbackRoot();
  resolvedStateRoots.set(candidate, fallback);
  // Announce it here too. Relocating silently is the same "cannot tell a
  // permission problem from a broken install" failure the warning exists for —
  // and readers in other processes re-probe and will NOT follow, so a job would
  // otherwise appear to vanish with no explanation.
  warnRelocated(candidate, fallback);
  return true;
}

export function ensureStateDir(cwd) {
  // The single place that creates, and therefore the single place that
  // validates. Covers the CLAUDE_PLUGIN_DATA-unset branch too, where the
  // fallback is the DEFAULT rather than the exception — validating only inside
  // the unwritable branch left that path completely unguarded.
  if (resolveStateRoot(process.env[PLUGIN_DATA_ENV]) === fallbackStateRoot()) {
    assertSafeFallbackRoot();
  }

  // 0700: the fallback root may sit in a shared /tmp, and job files carry
  // prompts, diffs and Codex output.
  try {
    fs.mkdirSync(resolveJobsDir(cwd), { recursive: true, mode: 0o700 });
    return;
  } catch (error) {
    // The probe in isWritableDir answers about the nearest EXISTING ancestor,
    // not about this path, so it can be wrong: a stale root-owned state dir
    // under a writable parent, a deny that hides a path from existsSync (which
    // reports false for EACCES too, letting the walk climb ABOVE the boundary),
    // write-without-search permission, or EROFS. Degrading to the fallback here
    // is what keeps a probe miss from reproducing the original crash — exit 1
    // with no sentinel, read downstream as "Codex is missing".
    if (!UNWRITABLE_ERRNOS.has(error.code) || !forceFallbackRoot()) throw error;
  }
  fs.mkdirSync(resolveJobsDir(cwd), { recursive: true, mode: 0o700 });
}

export function loadState(cwd) {
  const stateFile = resolveStateFile(cwd);
  if (!fs.existsSync(stateFile)) {
    return defaultState();
  }

  try {
    const parsed = JSON.parse(fs.readFileSync(stateFile, "utf8"));
    return {
      ...defaultState(),
      ...parsed,
      config: {
        ...defaultState().config,
        ...(parsed.config ?? {})
      },
      jobs: Array.isArray(parsed.jobs) ? parsed.jobs : []
    };
  } catch {
    return defaultState();
  }
}

function pruneJobs(jobs) {
  return [...jobs]
    .sort((left, right) => String(right.updatedAt ?? "").localeCompare(String(left.updatedAt ?? "")))
    .slice(0, MAX_JOBS);
}

// Pruning is housekeeping, never the caller's goal, so a file we cannot delete
// must not kill the run. `job.logFile` is an ABSOLUTE path recorded when the job
// was created, so after a fallback relocation it can still point into a root
// that just proved unwritable: existsSync says true (reads are allowed) and the
// unlink then throws EPERM, crashing saveState with exit 1 and no sentinel —
// the very symptom the fallback exists to prevent.
function removeFileIfExists(filePath) {
  if (!filePath) return;
  try {
    fs.unlinkSync(filePath);
  } catch (error) {
    if (error.code !== "ENOENT" && !UNWRITABLE_ERRNOS.has(error.code)) throw error;
  }
}

export function saveState(cwd, state) {
  const previousJobs = loadState(cwd).jobs;
  ensureStateDir(cwd);
  const nextJobs = pruneJobs(state.jobs ?? []);
  const nextState = {
    version: STATE_VERSION,
    config: {
      ...defaultState().config,
      ...(state.config ?? {})
    },
    jobs: nextJobs
  };

  const retainedIds = new Set(nextJobs.map((job) => job.id));
  for (const job of previousJobs) {
    if (retainedIds.has(job.id)) {
      continue;
    }
    removeJobFile(resolveJobFile(cwd, job.id));
    removeFileIfExists(job.logFile);
  }

  fs.writeFileSync(resolveStateFile(cwd), `${JSON.stringify(nextState, null, 2)}\n`, "utf8");
  return nextState;
}

export function updateState(cwd, mutate) {
  const state = loadState(cwd);
  mutate(state);
  return saveState(cwd, state);
}

export function generateJobId(prefix = "job") {
  const random = Math.random().toString(36).slice(2, 8);
  return `${prefix}-${Date.now().toString(36)}-${random}`;
}

export function upsertJob(cwd, jobPatch) {
  return updateState(cwd, (state) => {
    const timestamp = nowIso();
    const existingIndex = state.jobs.findIndex((job) => job.id === jobPatch.id);
    if (existingIndex === -1) {
      state.jobs.unshift({
        createdAt: timestamp,
        updatedAt: timestamp,
        ...jobPatch
      });
      return;
    }
    state.jobs[existingIndex] = {
      ...state.jobs[existingIndex],
      ...jobPatch,
      updatedAt: timestamp
    };
  });
}

export function listJobs(cwd) {
  return loadState(cwd).jobs;
}

export function setConfig(cwd, key, value) {
  return updateState(cwd, (state) => {
    state.config = {
      ...state.config,
      [key]: value
    };
  });
}

export function getConfig(cwd) {
  return loadState(cwd).config;
}

export function writeJobFile(cwd, jobId, payload) {
  ensureStateDir(cwd);
  const jobFile = resolveJobFile(cwd, jobId);
  fs.writeFileSync(jobFile, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return jobFile;
}

export function readJobFile(jobFile) {
  return JSON.parse(fs.readFileSync(jobFile, "utf8"));
}

function removeJobFile(jobFile) {
  removeFileIfExists(jobFile);
}

export function resolveJobLogFile(cwd, jobId) {
  ensureStateDir(cwd);
  return path.join(resolveJobsDir(cwd), `${jobId}.log`);
}

export function resolveJobFile(cwd, jobId) {
  ensureStateDir(cwd);
  return path.join(resolveJobsDir(cwd), `${jobId}.json`);
}
