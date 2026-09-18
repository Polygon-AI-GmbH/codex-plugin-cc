/**
 * The test suite's own orphan leak: `makeTempDir` handed out directories nobody
 * removed, and a workspace that still exists is one the product's own defences
 * (see lib/broker-watchdog.mjs) cannot reap — so `cleanupTempDirs` must stop
 * every broker a test started.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import { buildEnv, installFakeCodex } from "./fake-codex-fixture.mjs";
import { alive, cleanupTempDirs, makeTempDir, until, withPluginData } from "./helpers.mjs";
import {
  ensureBrokerSession,
  loadBrokerSession,
  waitForBrokerEndpoint
} from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";

test("cleanupTempDirs removes every directory makeTempDir handed out", async () => {
  const dirs = [makeTempDir(), makeTempDir("codex-plugin-test-nested-")];
  fs.writeFileSync(path.join(dirs[0], "leftover.txt"), "x", "utf8");
  fs.mkdirSync(path.join(dirs[1], "sub", "deeper"), { recursive: true });

  await cleanupTempDirs();

  for (const dir of dirs) {
    assert.equal(fs.existsSync(dir), false, `${dir} survived cleanup — this is how 136 of them accumulated`);
  }
});

test(
  "cleanupTempDirs stops a broker a test started for a tracked temp dir",
  { skip: process.platform === "win32" ? "unix sockets and process groups" : false },
  async () => {
    // Under its own plugin-data root, like every other test that starts a real
    // broker, so the record never lands in the developer's real state tree.
    await withPluginData(async () => {
      const binDir = makeTempDir();
      installFakeCodex(binDir);
      const workspace = makeTempDir();

      const session = await ensureBrokerSession(workspace, { env: buildEnv(binDir) });
      assert.ok(session?.endpoint, "the broker under test did not start");
      assert.ok(alive(session.pid), "the broker process should be running before cleanup");
      assert.ok(loadBrokerSession(workspace), "the broker session should be recorded before cleanup");

      await cleanupTempDirs();

      assert.ok(
        await until(() => !alive(session.pid), 2000),
        "the broker process must be gone — a test that starts one has to stop it"
      );
      assert.equal(
        await waitForBrokerEndpoint(session.endpoint, 500),
        false,
        "the broker endpoint must be dead after cleanup"
      );
      assert.equal(fs.existsSync(workspace), false, "the workspace temp dir must be removed");
    });
  }
);
