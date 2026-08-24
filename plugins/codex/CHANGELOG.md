# Changelog

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
