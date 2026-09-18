/**
 * Self-termination for the detached broker process.
 *
 * `spawnBrokerProcess` starts the broker `detached: true` and `unref()`s it, so
 * nothing supervises it. Until this existed it had exactly two exit paths — the
 * `broker/shutdown` RPC and SIGTERM/SIGINT — and the only caller of either is
 * the SessionEnd hook, which tears down ONLY the broker for the session's own
 * cwd. The commit-pipeline review gate runs the companion from a throwaway
 * snapshot worktree, so every review spawned a broker keyed on a path deleted
 * minutes later, and no session end would ever name it. Observed on one
 * machine: ~80 brokers, 65 pointing at a cwd that no longer existed (oldest 10
 * days), 10 `codex app-server` children 8-9 days old and ~4300 open pipe fds —
 * enough that macOS degraded new pipe allocation until a 513-byte write blocked
 * forever and hung every SessionStart hook.
 *
 * Two bounded conditions, on one timer:
 *   - the broker's `--cwd` is gone and no client is attached, so nothing can
 *     ever reach it again. A client mid-turn for a deleted worktree keeps it —
 *     but only for one idle window: a wedged client (the pipe-exhaustion case
 *     above holds its socket open forever) would otherwise make a gone-cwd
 *     broker immortal, and the SessionStart sweep's if-idle shutdown would get
 *     a busy refusal from it forever;
 *   - nothing has talked to it for `idleMs`.
 *
 * A third self-initiated exit lives in the broker itself, not here: losing its
 * `codex app-server` child (exit code 2 — see app-server-broker.mjs), which is
 * event-driven rather than polled.
 *
 * On win32 a directory with a running process's cwd inside it cannot be
 * deleted, so `cwd-removed` never fires there; only the idle exit applies.
 *
 * Idling out is free: `ensureBrokerSession` transparently respawns a broker in
 * ~2s on the next command, and the SessionEnd hook still handles the graceful
 * path. The cost of NOT idling out is an app-server and its pipes held until
 * reboot.
 */
import process from "node:process";

import { workspaceIsGone } from "./fs.mjs";

export const BROKER_IDLE_MS_ENV = "CODEX_COMPANION_BROKER_IDLE_MS";
export const DEFAULT_BROKER_IDLE_MS = 30 * 60 * 1000;
// Below this a broker could expire between two commands of one job.
export const MIN_BROKER_IDLE_MS = 1000;

// One timer answers both questions, and its period is derived from the idle
// limit rather than fixed: production keeps the documented 30s cwd poll, while
// a test can set a 1s limit and watch both conditions fire in a couple of
// seconds without a second knob to get out of sync.
const MIN_TICK_MS = 250;
const MAX_TICK_MS = 30_000;
// Two consecutive misses, not one: a single stat failure mid-turn must not end
// a broker that is doing work.
const CWD_MISSING_TICKS_TO_EXPIRE = 2;

/**
 * @param {Record<string, string | undefined>} [env]
 * @returns {number}
 */
export function resolveBrokerIdleMs(env = process.env) {
  // Only a plain decimal integer at or above the floor is accepted; anything
  // else takes the default. A malformed value must never be read as "never
  // expire" — that is the leak — nor as a zero/tiny limit, which would kill a
  // broker the instant it starts.
  const raw = env?.[BROKER_IDLE_MS_ENV];
  if (typeof raw !== "string" || !/^\d+$/.test(raw)) {
    return DEFAULT_BROKER_IDLE_MS;
  }
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) && parsed >= MIN_BROKER_IDLE_MS ? parsed : DEFAULT_BROKER_IDLE_MS;
}

/**
 * @param {number} idleMs
 * @returns {number}
 */
export function resolveWatchdogTickMs(idleMs) {
  return Math.min(MAX_TICK_MS, Math.max(MIN_TICK_MS, Math.floor(idleMs / 4)));
}

/**
 * @param {object} options
 * @param {string | null} [options.cwd] Absolute path to poll, or null to skip
 *   the check — only a `--cwd` the caller passed explicitly is meaningful; the
 *   `process.cwd()` default cannot have been removed out from under us.
 * @param {number} [options.idleMs]
 * @param {() => boolean} [options.isBusy] Sockets attached or work in flight.
 * @param {(reason: "cwd-removed" | "cwd-removed (busy past idle window)" | "idle") => void} options.onExpire
 */
export function startBrokerWatchdog({
  cwd = null,
  idleMs = resolveBrokerIdleMs(),
  isBusy = () => false,
  onExpire
}) {
  let lastActivity = Date.now();
  let cwdMissingTicks = 0;
  // When the cwd was first seen missing, busy or not; null while it exists.
  let cwdMissingSince = null;

  // Deliberately NOT unref'd. The listening server already holds the loop open,
  // so this costs nothing, and a ref'd timer cannot be skipped by an exit that
  // races it.
  const timer = setInterval(() => {
    const busy = isBusy();
    if (cwd) {
      const gone = workspaceIsGone(cwd);
      // A missing cwd ends nothing while a client is attached: a detached
      // `codex task` whose worktree is deleted mid-stream (a /bm or /merge-pr
      // teardown, a review-gate snapshot) must finish, not lose the job to a
      // closed socket. The miss count RESETS while busy rather than pausing,
      // so once the client leaves the two consecutive misses are both observed
      // unattended — the same evidence an unattended broker always needed.
      cwdMissingTicks = !busy && gone ? cwdMissingTicks + 1 : 0;
      // ...bounded by one idle window, counted from the first miss and NOT
      // reset while busy: a client that never leaves must not keep a broker
      // for a deleted workspace alive forever. Both reset when the cwd is back.
      cwdMissingSince = gone ? (cwdMissingSince ?? Date.now()) : null;
      if (cwdMissingTicks >= CWD_MISSING_TICKS_TO_EXPIRE) {
        clearInterval(timer);
        onExpire("cwd-removed");
        return;
      }
      if (cwdMissingSince !== null && Date.now() - cwdMissingSince >= idleMs) {
        clearInterval(timer);
        onExpire(busy ? "cwd-removed (busy past idle window)" : "cwd-removed");
        return;
      }
    }
    // A broker with a client attached is never idle, even if that client has
    // been silent: the clock measures abandonment, not chattiness.
    if (busy) {
      lastActivity = Date.now();
      return;
    }
    if (Date.now() - lastActivity >= idleMs) {
      clearInterval(timer);
      onExpire("idle");
    }
  }, resolveWatchdogTickMs(idleMs));

  return {
    touch() {
      lastActivity = Date.now();
    }
  };
}
