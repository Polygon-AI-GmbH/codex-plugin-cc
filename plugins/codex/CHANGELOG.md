# Changelog

## 1.0.9

- fix: the detached `codex app-server` broker now terminates itself — when its `--cwd` is removed (polled every 30s; two consecutive misses observed with no client attached, so a transiently unreadable workspace does not count and a job still attached for a deleted worktree keeps the broker — but only for one idle window from the first miss, so a client that never leaves cannot make a broker for a deleted workspace immortal), after 30 minutes with no client attached (`CODEX_COMPANION_BROKER_IDLE_MS` overrides it; a plain integer ≥ 1000, anything else takes the default), or when its `codex app-server` child dies (exit 2). Until now its only exit paths were the SessionEnd hook and SIGTERM, and the commit-pipeline review gate runs the companion from a throwaway snapshot worktree that no session end ever names — observed as ~80 leaked brokers holding ~4300 pipe fds until macOS stopped allocating pipes and every SessionStart hook hung.
- The broker clears its own `broker.json` on every exit, shuts down its app-server under a bound (SIGKILL past 2s), stops accepting connections before it does so, and handles SIGTERM/SIGINT gracefully instead of dying of the signal with its socket and pid file left behind.
- `broker.json` records the workspace root the broker was started for, and the SessionStart hook sweeps records whose workspace no longer exists (time-boxed to 2.5s, the walk included; the shutdown it sends is conditional — a broker with any other client attached (busy by the same definition its watchdog uses: a `/codex:task` between `thread/start` and `turn/start` has no request in flight but is very much alive) refuses it and is left alone, record and socket included, until its own watchdog ends it; files are only removed once the broker acknowledged the shutdown or nothing was listening — a refusal, a timeout, or a reply that is neither ack nor refusal leaves them for the next sweep — and only paths inside a `cxc-*` session directory are ever unlinked). SessionEnd's shutdown of the session's own broker stays unconditional.
- Teardown signals a recorded pid only while its pid file still names it — the broker unlinks that file on every clean exit, so a stale pid is never sent a signal.
- Migration note: records written by ≤ 1.0.8 carry no `cwd`, so the sweep skips them and their brokers have no watchdog. Any that survive the upgrade need a one-time manual cleanup — `pkill -f app-server-broker.mjs` while no Codex job is running; the next command respawns a current broker.

## 1.0.8

- fix: job state falls back to a temp directory when `CLAUDE_PLUGIN_DATA` is not writable. Claude Code points that variable under `~/.claude/plugins`, which its own sandbox denies — and the deny cannot be lifted from settings, because a child `allowWrite` loses to the built-in parent deny. The companion died with `EPERM ... mkdir '.../jobs'` and exited 1 with no sentinel, which read downstream as a missing Codex install. A temp fallback existed but only fired when the variable was unset, never when it was set-but-unwritable.
- The fallback root is namespaced per-uid and created `0700`, and is rejected outright if it already exists as a symlink, with another owner, or with group/other permissions — `os.tmpdir()` is the shared `/tmp` on Linux, and job files carry prompts, diffs and Codex output.
- `saveBrokerSession` now creates the state tree through `ensureStateDir`. It runs before any job on the first-run path, and its previous modeless `mkdirSync` left the whole tree `0755` (and `state.json`, which holds job prompts, `0644`).
- A failed first write now degrades to the fallback instead of throwing, so a case the writability probe gets wrong no longer reproduces the original crash.
- Migration note: the fallback root is renamed (`<tmp>/codex-companion` → `<tmp>/codex-companion-<uid>`), so installs that ran with `CLAUDE_PLUGIN_DATA` unset lose visibility of jobs recorded by earlier versions. Let in-flight background jobs finish before upgrading.
- Known limitation: a sandboxed session and an unsandboxed one resolve different roots, so neither sees the other's jobs **or setup toggles** — re-run `/codex:setup` options if they look unset.

## 1.0.7

- Forked as `polygon-codex` (Polygon-AI GmbH, `Polygon-AI-GmbH/codex-plugin-cc`); plugin name and commands are unchanged.
- fix: `review --background` and `adversarial-review --background` now detach into the shared background job worker (the same path `task --background` uses) instead of being parsed but silently ignored and running in the foreground, where the harness's 600s foreground cap killed real reviews (SIGTERM/exit 143).
- The `/codex:review` and `/codex:adversarial-review` command docs no longer claim `Bash(run_in_background)` is what detaches the run.
- Migration note: the marketplace rename re-keys the plugin state directory (`~/.claude/plugins/data/codex-openai-codex` → `codex-polygon-codex`), so setup toggles (e.g. the stop-time review gate) and job history reset under the new key. Let any in-flight background jobs finish before switching, then re-run `/codex:setup` options after.

## 1.0.0

- Initial version of the Codex plugin for Claude Code
