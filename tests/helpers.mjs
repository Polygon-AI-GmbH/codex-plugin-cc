import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { spawnSync } from "node:child_process";

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

export function makeTempDir(prefix = "codex-plugin-test-") {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
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
