import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

import {
  chooseAvailableStore,
  creationAttemptKeys,
  enginePauseSeconds,
  localDateKey,
  primaryStores,
  quotaBlockForDay,
  quotaDayKey,
  readJson,
  readJsonLines,
  shardStaggerSeconds,
  shouldRotateStore,
  standbyStores,
  workerSummary,
  writeJsonAtomic,
} from "../src/lib/core.mjs";

const execFileAsync = promisify(execFile);

const config = {
  runtime: {
    store_concurrency: 2,
    primary_store_ids: ["p1", "p2"],
    standby_store_ids: ["s1"],
  },
  stores: [
    { store_id: "p1", enabled: true },
    { store_id: "p2", enabled: true },
    { store_id: "s1", enabled: true },
    { store_id: "disabled", enabled: false },
  ],
};

test("primary and standby pools are explicit and disjoint", () => {
  assert.deepEqual(primaryStores(config).map((row) => row.store_id), ["p1", "p2"]);
  assert.deepEqual(standbyStores(config).map((row) => row.store_id), ["s1"]);
});
test("quota resets at Asia/Shanghai midnight and counts unique attempts", () => {
  assert.equal(localDateKey("2026-08-23T15:59:59.000Z", "Asia/Shanghai"), "2026-08-23");
  assert.equal(localDateKey("2026-08-23T16:00:00.000Z", "Asia/Shanghai"), "2026-08-24");
  const keys = creationAttemptKeys([
    { at: "2026-08-23T15:50:00.000Z", store_id: "p1", item_id: "a", state: "submitted" },
    { at: "2026-08-23T15:51:00.000Z", store_id: "p1", item_id: "a", state: "error" },
    { at: "2026-08-23T15:52:00.000Z", store_id: "p1", item_id: "b", state: "failed" },
    { at: "2026-08-23T16:01:00.000Z", store_id: "p1", item_id: "next-day", state: "submitted" },
  ], {
    exactStoreId: "p1",
    dayKey: "2026-08-23",
    timeZone: "Asia/Shanghai",
  });
  assert.deepEqual([...keys].sort(), ["a", "b"]);
});

test("quota day supports a non-midnight platform reset", () => {
  assert.equal(
    quotaDayKey("2026-08-25T23:59:59.000Z", "Europe/Moscow", "03:00"),
    "2026-08-25",
  );
  assert.equal(
    quotaDayKey("2026-08-26T00:00:00.000Z", "Europe/Moscow", "03:00"),
    "2026-08-26",
  );
  const keys = creationAttemptKeys([
    { at: "2026-08-25T23:59:59.000Z", store_id: "p1", item_id: "old", state: "submitted" },
    { at: "2026-08-26T00:00:00.000Z", store_id: "p1", item_id: "new", state: "submitted" },
  ], {
    exactStoreId: "p1",
    dayKey: "2026-08-26",
    timeZone: "Europe/Moscow",
    resetLocal: "03:00",
  });
  assert.deepEqual([...keys], ["new"]);
});

test("an explicit quota block expires when the configured quota day changes", () => {
  const block = {
    blocked: true,
    detected_at: "2026-08-25T23:50:00.000Z",
    reason: "platform-daily-creation-limit",
  };
  assert.equal(quotaBlockForDay(
    block,
    "2026-08-25",
    "Europe/Moscow",
    "03:00",
  ), block);
  assert.equal(quotaBlockForDay(
    block,
    "2026-08-26",
    "Europe/Moscow",
    "03:00",
  ), null);
});

test("standby is selected only when the primary is blocked", () => {
  const primary = { store_id: "p1" };
  const standby = { store_id: "s1" };
  assert.equal(chooseAvailableStore({
    primary,
    standbys: [standby],
    quotaByStore: { p1: { blocked: false }, s1: { blocked: false } },
  }).store_id, "p1");
  assert.equal(chooseAvailableStore({
    primary,
    standbys: [standby],
    quotaByStore: { p1: { blocked: true }, s1: { blocked: false } },
  }).store_id, "s1");
  assert.equal(chooseAvailableStore({
    primary,
    standbys: [standby],
    quotaByStore: { p1: { blocked: true }, s1: { blocked: false } },
    assignedStoreIds: new Set(["s1"]),
  }), null);
});

test("an empty candidate queue uses the slower independent retry interval", () => {
  const runtime = {
    cycle_pause_seconds: 5,
    empty_queue_pause_seconds: 300,
    error_pause_seconds: 30,
  };
  assert.equal(enginePauseSeconds({
    stop_reason: "candidate-queue-complete",
    counts: { scanned: 0 },
  }, runtime, 0), 300);
  assert.equal(enginePauseSeconds({ stop_reason: "cycle-complete" }, runtime, 0), 5);
  assert.equal(enginePauseSeconds({}, runtime, 1), 30);
});

test("platform quota and API rate-limit signals match the running Flow E/F contract", () => {
  assert.equal(shouldRotateStore({
    stop_reason: "submission-blocked-ozon-daily-limit",
    submission_blocker: { type: "ozon-daily-product-creation-limit" },
  }), true);
  const nowMs = Date.parse("2026-08-26T09:00:00.000Z");
  assert.equal(enginePauseSeconds({
    stop_reason: "maozi-api-rate-limit",
    runtime_blocker: {
      type: "maozi-api-rate-limit",
      retry_after_at: "2026-08-26T09:06:00.000Z",
    },
  }, {
    rate_limit_pause_seconds: 300,
    rate_limit_retry_stagger_seconds: 5,
  }, 0, { nowMs, shardIndex: 3 }), 375);
  assert.equal(enginePauseSeconds({
    stop_reason: "api-rate-limit",
  }, {
    rate_limit_pause_seconds: 300,
  }, 1, { nowMs, shardIndex: 0 }), 300);
  assert.equal(shardStaggerSeconds(30, 5, 120), 120);
});

test("multi-store summary distinguishes running, quota-blocked, and queue-waiting", () => {
  assert.deepEqual(workerSummary([
    { active: true, phase: "engine-active" },
    { active: false, phase: "daily-quota-blocked", daily_quota: { blocked: true } },
    { active: false, phase: "candidate-queue-complete", engine_stop_reason: "candidate-queue-complete" },
  ]), {
    total_workers: 3,
    active_workers: 1,
    quota_blocked_workers: 1,
    queue_complete_workers: 1,
    rate_limited_workers: 0,
    phase: "running",
  });
  assert.equal(workerSummary([
    { active: false, phase: "candidate-queue-complete" },
  ]).phase, "queue-waiting");
  assert.equal(workerSummary([
    { active: false, phase: "rate-limited", engine_stop_reason: "api-rate-limit" },
  ]).phase, "rate-limited");
});

test("concurrent atomic status writes leave one valid JSON document", async () => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "flowde-atomic-"));
  const filename = path.join(temporary, "status.json");
  try {
    await Promise.all(Array.from({ length: 20 }, (_, index) => (
      writeJsonAtomic(filename, { index })
    )));
    const status = await readJson(filename);
    assert.equal(Number.isInteger(status.index), true);
    assert.deepEqual((await fs.readdir(temporary)).filter((name) => name.includes(".tmp-")), []);
  } finally {
    await fs.rm(temporary, { recursive: true, force: true });
  }
});

test("local attempt counts never impersonate an explicit platform quota block", async () => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "flowde-quota-"));
  const stateDir = path.join(temporary, "state");
  const engineDir = path.join(stateDir, "engines", "p1");
  const configFile = path.join(temporary, "config.json");
  try {
    await fs.mkdir(engineDir, { recursive: true });
    await fs.writeFile(configFile, `${JSON.stringify({
      contract: "flowde-test-v1",
      engine: path.resolve("adapters/example-engine.mjs"),
      runtime: {
        store_concurrency: 1,
        primary_store_ids: ["p1"],
        daily_store_creation_limit: 1,
        daily_quota_timezone: "UTC",
        daily_quota_reset_local: "00:00",
      },
      stores: [{ store_id: "p1", store_name: "P1", enabled: true }],
    }, null, 2)}\n`, "utf8");
    const now = new Date().toISOString();
    await fs.writeFile(path.join(engineDir, "audit.jsonl"), [
      { at: now, store_id: "p1", item_id: "a", state: "submitted" },
      { at: now, store_id: "p1", item_id: "b", state: "submitted" },
    ].map((row) => JSON.stringify(row)).join("\n") + "\n", "utf8");
    const { stdout } = await execFileAsync(process.execPath, [
      path.resolve("src/flowde.mjs"),
      "status",
    ], {
      cwd: path.resolve("."),
      env: {
        ...process.env,
        FLOWDE_CONFIG: configFile,
        FLOWDE_STATE_DIR: stateDir,
      },
    });
    const quota = JSON.parse(stdout).daily_quota.stores.p1;
    assert.equal(quota.used, 2);
    assert.equal(quota.remaining, 0);
    assert.equal(quota.blocked, false);
    assert.equal(quota.reason, null);
  } finally {
    await fs.rm(temporary, { recursive: true, force: true });
  }
});

test("example adapter writes only local runtime state", async () => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "flowde-test-"));
  try {
    await execFileAsync(process.execPath, [path.resolve("adapters/example-engine.mjs"), "run"], {
      cwd: path.resolve("."),
      env: {
        ...process.env,
        FLOWDE_RUNTIME_STATE_DIR: temporary,
        FLOWDE_STORE_ID: "test-store",
        FLOWDE_STORE_NAME: "Test Store",
        FLOWDE_MAX_CREATIONS: "2",
      },
    });
    const status = await readJson(path.join(temporary, "status.json"));
    const audit = await readJsonLines(path.join(temporary, "audit.jsonl"));
    assert.equal(status.stop_reason, "cycle-complete");
    assert.equal(status.counts.submitted, 2);
    assert.equal(audit.length, 2);
  } finally {
    await fs.rm(temporary, { recursive: true, force: true });
  }
});
