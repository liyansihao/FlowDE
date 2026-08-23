#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";

import { appendJsonLine, number, sleep, writeJsonAtomic } from "../src/lib/core.mjs";

const stateDir = path.resolve(process.env.FLOWDE_RUNTIME_STATE_DIR || "./state/example-engine");
const statusFile = path.join(stateDir, "status.json");
const stopFile = path.join(stateDir, "stop.requested");
let stopping = false;

process.on("SIGINT", () => { stopping = true; });
process.on("SIGTERM", () => { stopping = true; });

async function stopRequested() {
  if (stopping) return true;
  return fs.stat(stopFile).then(() => true).catch(() => false);
}
async function run() {
  const storeId = String(process.env.FLOWDE_STORE_ID || "example");
  const storeName = String(process.env.FLOWDE_STORE_NAME || storeId);
  const maxCreations = Math.max(0, number(process.env.FLOWDE_MAX_CREATIONS));
  const forceLimit = process.env.FLOWDE_DEMO_FORCE_LIMIT === "1";
  const status = {
    contract: "flowde-engine-status-v1",
    active: true,
    pid: process.pid,
    store_id: storeId,
    store_name: storeName,
    phase: "creating",
    counts: { submission_attempts: 0, submitted: 0 },
    started_at: new Date().toISOString(),
  };
  await writeJsonAtomic(statusFile, status);
  if (forceLimit) {
    status.active = false;
    status.phase = "stopped";
    status.stop_reason = "platform-daily-creation-limit";
    status.submission_blocker = {
      type: "platform-daily-creation-limit",
      message: "Example platform quota response",
    };
    status.updated_at = new Date().toISOString();
    await writeJsonAtomic(statusFile, status);
    return;
  }
  for (let index = 0; index < maxCreations; index += 1) {
    if (await stopRequested()) break;
    const itemId = `${storeId}-${Date.now()}-${index}`;
    await appendJsonLine(path.join(stateDir, "audit.jsonl"), {
      at: new Date().toISOString(),
      store_id: storeId,
      item_id: itemId,
      state: "submitted",
      submission_attempted: true,
    });
    status.counts.submission_attempts += 1;
    status.counts.submitted += 1;
    status.updated_at = new Date().toISOString();
    await writeJsonAtomic(statusFile, status);
    await sleep(50);
  }
  status.active = false;
  status.pid = null;
  status.phase = "cycle-complete";
  status.stop_reason = stopping ? "signal-received" : "cycle-complete";
  status.updated_at = new Date().toISOString();
  await writeJsonAtomic(statusFile, status);
}

if ((process.argv[2] || "run") !== "run") {
  process.stderr.write("usage: example-engine.mjs run\n");
  process.exitCode = 2;
} else {
  run().catch(async (error) => {
    await writeJsonAtomic(statusFile, {
      contract: "flowde-engine-status-v1",
      active: false,
      pid: null,
      phase: "fatal-error",
      stop_reason: "fatal-error",
      error: String(error?.message || error),
      updated_at: new Date().toISOString(),
    }).catch(() => null);
    process.stderr.write(`${String(error?.stack || error)}\n`);
    process.exitCode = 1;
  });
}
