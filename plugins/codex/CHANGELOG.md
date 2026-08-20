# Changelog

## 1.0.7

- Forked as `polygon-codex` (Polygon-AI GmbH, `Polygon-AI-GmbH/codex-plugin-cc`); plugin name and commands are unchanged.
- fix: `review --background` and `adversarial-review --background` now detach into the shared background job worker (the same path `task --background` uses) instead of being parsed but silently ignored and running in the foreground, where the harness's 600s foreground cap killed real reviews (SIGTERM/exit 143).
- The `/codex:review` and `/codex:adversarial-review` command docs no longer claim `Bash(run_in_background)` is what detaches the run.
- Migration note: the marketplace rename re-keys the plugin state directory (`~/.claude/plugins/data/codex-openai-codex` → `codex-polygon-codex`), so setup toggles (e.g. the stop-time review gate) and job history reset under the new key. Let any in-flight background jobs finish before switching, then re-run `/codex:setup` options after.

## 1.0.0

- Initial version of the Codex plugin for Claude Code
