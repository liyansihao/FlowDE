#!/usr/bin/env node

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import {
  appendJsonLine,
  chooseAvailableStore,
  creationAttemptKeys,
  emptyCandidateQueue,
  enginePauseSeconds,
  localDateKey,
  number,
  primaryStores,
  processActive,
  readJson,
  readJsonLines,
  runtimeStores,
  shouldRotateStore,
  sleep,
  standbyStores,
  storeId,
  workerSummary,
  writeJsonAtomic,
} from "./lib/core.mjs";

const PROJECT_ROOT = path.resolve(import.meta.dirname, "..");
const CONFIG_FILE = path.resolve(process.env.FLOWDE_CONFIG || path.join(PROJECT_ROOT, "config.json"));
const STATE_DIR = path.resolve(process.env.FLOWDE_STATE_DIR || path.join(PROJECT_ROOT, "state"));
const STATUS_FILE = path.join(STATE_DIR, "status.json");
const PID_FILE = path.join(STATE_DIR, "supervisor.pid");
const STOP_FILE = path.join(STATE_DIR, "stop.requested");
const LOG_FILE = path.join(STATE_DIR, "supervisor.jsonl");
const QUOTA_FILE = path.join(STATE_DIR, "daily-store-quota.json");
const ENGINE_ROOT = path.join(STATE_DIR, "engines");

function storeDirectory(id) {
  return path.join(ENGINE_ROOT, encodeURIComponent(storeId(id)));
}
function engineStatusFile(id) {
  return path.join(storeDirectory(id), "status.json");
}

function engineAuditFile(id) {
  return path.join(storeDirectory(id), "audit.jsonl");
}

async function ensureState() {
  await fsp.mkdir(ENGINE_ROOT, { recursive: true });
}

async function stopRequested() {
  return fsp.stat(STOP_FILE).then(() => true).catch(() => false);
}

async function readRuntimeJson(filename, fallback = {}) {
  let lastError = null;
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      return await readJson(filename, fallback);
    } catch (error) {
      lastError = error;
      if (attempt < 4) await sleep(100 * (attempt + 1));
    }
  }
  return {
    ...fallback,
    runtime_read_error: String(lastError?.message || lastError || "status read failed"),
  };
}

async function loadConfig() {
  const config = await readJson(CONFIG_FILE, null);
  if (!config) {
    throw new Error(`missing private config: copy config.example.json to ${CONFIG_FILE}`);
  }
  if (String(config?.runtime?.daily_quota_reset_local || "00:00") !== "00:00") {
    throw new Error("this release supports a daily quota reset at 00:00 only");
  }
  const allStores = runtimeStores(config);
  const ids = allStores.map((store) => storeId(store.store_id));
  if (ids.length !== new Set(ids).size) throw new Error("store_id values must be unique");
  if (primaryStores(config).length === 0) throw new Error("at least one enabled primary store is required");
  if (!String(config.engine || "").trim()) throw new Error("config.engine is required");
  const enginePath = path.resolve(path.dirname(CONFIG_FILE), config.engine);
  await fsp.access(enginePath);
  return { ...config, engine_path: enginePath };
}

export async function dailyQuotaSnapshot(config, stores = runtimeStores(config), now = new Date()) {
  const timeZone = String(config?.runtime?.daily_quota_timezone || "UTC");
  const limit = Math.max(1, number(config?.runtime?.daily_store_creation_limit, 100));
  const dayKey = localDateKey(now, timeZone);
  const [persisted, ...auditRows] = await Promise.all([
    readJson(QUOTA_FILE, {}),
    ...stores.map((store) => readJsonLines(engineAuditFile(store.store_id))),
  ]);
  const persistedStores = persisted?.day_key === dayKey ? persisted?.stores || {} : {};
  const result = {};
  stores.forEach((store, index) => {
    const id = storeId(store.store_id);
    const used = creationAttemptKeys(auditRows[index], {
      exactStoreId: id,
      dayKey,
      timeZone,
      scopedToStore: true,
    }).size;
    const externalBlock = persistedStores[id] || null;
    const blocked = used >= limit || externalBlock?.blocked === true;
    result[id] = {
      day_key: dayKey,
      time_zone: timeZone,
      reset_local: "00:00",
      limit,
      used,
      remaining: Math.max(0, limit - used),
      blocked,
      reason: used >= limit ? "local-daily-creation-limit" : externalBlock?.reason || null,
      detected_at: externalBlock?.detected_at || null,
    };
  });
  return { day_key: dayKey, time_zone: timeZone, limit, stores: result };
}

async function markStoreBlocked(config, store, reason, details = {}) {
  const timeZone = String(config?.runtime?.daily_quota_timezone || "UTC");
  const dayKey = localDateKey(new Date(), timeZone);
  const existing = await readJson(QUOTA_FILE, {});
  const value = existing?.day_key === dayKey ? existing : {
    contract: "flowde-daily-store-quota-v1",
    day_key: dayKey,
    time_zone: timeZone,
    reset_local: "00:00",
    stores: {},
  };
  value.stores ||= {};
  value.stores[storeId(store.store_id)] = {
    blocked: true,
    reason,
    detected_at: new Date().toISOString(),
    ...details,
  };
  value.updated_at = new Date().toISOString();
  await writeJsonAtomic(QUOTA_FILE, value);
}

function initialWorkerStates(config) {
  const primaries = primaryStores(config);
  const standbyIds = new Set(standbyStores(config).map((store) => storeId(store.store_id)));
  const primaryIndex = new Map(primaries.map((store, index) => [storeId(store.store_id), index]));
  return new Map(runtimeStores(config).map((store) => {
    const id = storeId(store.store_id);
    return [id, {
      store_id: id,
      store_name: store.store_name,
      role: standbyIds.has(id) ? "standby" : "primary",
      assigned_slot: primaryIndex.get(id) ?? null,
      shard_index: primaryIndex.get(id) ?? null,
      shard_count: primaries.length,
      replacing_store_id: null,
      covered_by_store_id: null,
      active: false,
      child_pid: null,
      phase: "starting",
      cycle: 0,
      last_error: null,
    }];
  }));
}

async function refreshStatus(config, base, workerStates) {
  const stores = runtimeStores(config);
  const quota = await dailyQuotaSnapshot(config, stores);
  const workers = [];
  for (const store of stores) {
    const id = storeId(store.store_id);
    const prior = workerStates.get(id) || {};
    const engine = await readRuntimeJson(engineStatusFile(id), {});
    const childPid = number(prior.child_pid || engine.pid);
    const active = processActive(childPid);
    const row = {
      ...prior,
      store_id: id,
      store_name: store.store_name,
      active,
      child_pid: active ? childPid : null,
      phase: active ? engine.phase || prior.phase || "engine-active" : prior.phase || engine.phase || "idle",
      daily_quota: quota.stores[id],
      engine_stop_reason: active ? null : engine.stop_reason || prior.engine_stop_reason || null,
      updated_at: engine.updated_at || prior.updated_at || null,
    };
    workerStates.set(id, row);
    workers.push(row);
  }
  const summary = workerSummary(workers);
  return {
    ...base,
    contract: config.contract || "flowde-multistore-v1",
    active: base.active !== false,
    pid: base.active === false ? null : process.pid,
    phase: base.active === false ? "stopped" : summary.phase,
    updated_at: new Date().toISOString(),
    daily_quota: quota,
    parallel: {
      configured_slots: primaryStores(config).length,
      ...summary,
      active_standby_workers: workers.filter((worker) => worker.active && worker.role === "standby").length,
      standby_stores: standbyStores(config).length,
    },
    store_workers: workers,
  };
}

async function supervise() {
  await ensureState();
  const oldPid = number(String(await fsp.readFile(PID_FILE, "utf8").catch(() => "")).trim());
  if (processActive(oldPid)) throw new Error(`FlowDE supervisor already active with PID ${oldPid}`);
  const config = await loadConfig();
  const runtime = config.runtime || {};
  const primaries = primaryStores(config);
  const standbys = standbyStores(config);
  const standbyIds = new Set(standbys.map((store) => storeId(store.store_id)));
  const workerStates = initialWorkerStates(config);
  const children = new Map();
  const slotAssignments = new Map();
  let terminating = false;
  let assignmentTail = Promise.resolve();
  let quotaTail = Promise.resolve();

  await fsp.rm(STOP_FILE, { force: true });
  await fsp.writeFile(PID_FILE, `${process.pid}\n`, "utf8");

  const requestStop = () => {
    terminating = true;
    for (const child of children.values()) {
      if (processActive(child.pid)) {
        try { process.kill(child.pid, "SIGTERM"); } catch {}
      }
    }
  };
  process.on("SIGINT", requestStop);
  process.on("SIGTERM", requestStop);

  const stopped = async () => terminating || await stopRequested();
  const pauseInterruptibly = async (seconds) => {
    const total = Math.max(0, number(seconds)) * 1_000;
    for (let waited = 0; waited < total && !(await stopped()); waited += 1_000) {
      await sleep(Math.min(1_000, total - waited));
    }
  };
  const withAssignmentLock = async (task) => {
    const previous = assignmentTail;
    let release;
    assignmentTail = new Promise((resolve) => { release = resolve; });
    await previous;
    try {
      return await task();
    } finally {
      release();
    }
  };
  const persistBlock = async (store, reason, details = {}) => {
    quotaTail = quotaTail.catch(() => null).then(() => markStoreBlocked(config, store, reason, details));
    await quotaTail;
  };

  function releaseSlotUnsafe(shardIndex) {
    const previousId = slotAssignments.get(shardIndex);
    slotAssignments.delete(shardIndex);
    if (!previousId) return;
    const previous = workerStates.get(previousId) || {};
    const replacedId = storeId(previous.replacing_store_id);
    workerStates.set(previousId, {
      ...previous,
      assigned_slot: null,
      replacing_store_id: null,
    });
    if (replacedId) {
      workerStates.set(replacedId, {
        ...workerStates.get(replacedId),
        covered_by_store_id: null,
      });
    }
  }

  async function chooseAssignment(primary, shardIndex) {
    return withAssignmentLock(async () => {
      const candidates = [primary, ...standbys];
      const quota = await dailyQuotaSnapshot(config, candidates);
      const assignedElsewhere = new Set(
        [...slotAssignments.entries()]
          .filter(([slot]) => slot !== shardIndex)
          .map(([, id]) => id),
      );
      const candidate = chooseAvailableStore({
        primary,
        standbys,
        quotaByStore: quota.stores,
        assignedStoreIds: assignedElsewhere,
      });
      for (const store of candidates) {
        const id = storeId(store.store_id);
        const prior = workerStates.get(id) || {};
        workerStates.set(id, {
          ...prior,
          daily_quota: quota.stores[id],
          phase: quota.stores[id]?.blocked ? "daily-quota-blocked" : prior.phase,
        });
      }
      if (!candidate) {
        releaseSlotUnsafe(shardIndex);
        return null;
      }
      const id = storeId(candidate.store_id);
      const priorId = slotAssignments.get(shardIndex);
      if (priorId !== id) releaseSlotUnsafe(shardIndex);
      slotAssignments.set(shardIndex, id);
      const isStandby = standbyIds.has(id);
      const primaryId = storeId(primary.store_id);
      workerStates.set(id, {
        ...workerStates.get(id),
        assigned_slot: shardIndex,
        shard_index: shardIndex,
        replacing_store_id: isStandby ? primaryId : null,
      });
      workerStates.set(primaryId, {
        ...workerStates.get(primaryId),
        covered_by_store_id: isStandby ? id : null,
      });
      return { store: candidate, quota: quota.stores[id], is_standby: isStandby };
    });
  }

  async function runEngineCycle(store, primary, shardIndex, quota, isStandby) {
    const id = storeId(store.store_id);
    const runtimeDir = storeDirectory(id);
    await fsp.mkdir(runtimeDir, { recursive: true });
    const maxCreations = Math.max(0, Math.min(
      number(runtime.max_creations_per_cycle, 10),
      number(quota?.remaining),
    ));
    if (maxCreations <= 0) return { daily_limited: true, pause_seconds: 1 };
    const prior = workerStates.get(id) || {};
    workerStates.set(id, {
      ...prior,
      cycle: number(prior.cycle) + 1,
      phase: "starting-engine",
      last_error: null,
    });
    await fsp.rm(path.join(runtimeDir, "stop.requested"), { force: true });
    const logFd = fs.openSync(path.join(runtimeDir, "engine.log"), "a");
    const child = spawn(process.execPath, [config.engine_path, "run"], {
      cwd: path.dirname(config.engine_path),
      env: {
        ...process.env,
        FLOWDE_RUNTIME_STATE_DIR: runtimeDir,
        FLOWDE_STORE_ID: id,
        FLOWDE_STORE_NAME: String(store.store_name || id),
        FLOWDE_SLOT_INDEX: String(shardIndex),
        FLOWDE_SLOT_COUNT: String(primaries.length),
        FLOWDE_MAX_CREATIONS: String(maxCreations),
        FLOWDE_DAILY_LIMIT: String(quota.limit),
        FLOWDE_DAILY_TIMEZONE: String(quota.time_zone),
        FLOWDE_IS_STANDBY: isStandby ? "1" : "0",
        FLOWDE_REPLACING_STORE_ID: isStandby ? storeId(primary.store_id) : "",
      },
      stdio: ["ignore", logFd, logFd],
    });
    fs.closeSync(logFd);
    children.set(id, child);
    workerStates.set(id, {
      ...workerStates.get(id),
      active: true,
      child_pid: child.pid,
      phase: "engine-active",
    });
    await appendJsonLine(LOG_FILE, {
      at: new Date().toISOString(),
      event: "engine-started",
      store_id: id,
      role: isStandby ? "standby" : "primary",
      replacing_store_id: isStandby ? storeId(primary.store_id) : null,
      shard_index: shardIndex,
      child_pid: child.pid,
      quota_remaining_before_cycle: quota.remaining,
    });
    const exit = await new Promise((resolve) => child.once("exit", (code, signal) => resolve({ code, signal })));
    children.delete(id);
    const engine = await readRuntimeJson(engineStatusFile(id), {});
    if (shouldRotateStore(engine)) {
      await persistBlock(store, "platform-daily-creation-limit", {
        message: engine?.submission_blocker?.message || null,
      });
    }
    const refreshed = (await dailyQuotaSnapshot(config, [store])).stores[id];
    const dailyLimited = shouldRotateStore(engine) || refreshed?.blocked === true;
    const queueComplete = emptyCandidateQueue(engine);
    workerStates.set(id, {
      ...workerStates.get(id),
      active: false,
      child_pid: null,
      phase: dailyLimited
        ? "daily-quota-blocked"
        : queueComplete
          ? "candidate-queue-complete"
          : "cycle-complete",
      daily_quota: refreshed,
      engine_stop_reason: engine.stop_reason || null,
      last_error: exit.code === 0 ? null : { at: new Date().toISOString(), exit },
    });
    await appendJsonLine(LOG_FILE, {
      at: new Date().toISOString(),
      event: "engine-exited",
      store_id: id,
      exit,
      stop_reason: engine.stop_reason || null,
      daily_limited: dailyLimited,
    });
    return {
      daily_limited: dailyLimited,
      pause_seconds: enginePauseSeconds(engine, runtime, exit.code),
    };
  }

  async function runSlot(primary, shardIndex) {
    while (!(await stopped())) {
      const assignment = await chooseAssignment(primary, shardIndex);
      if (!assignment) {
        await pauseInterruptibly(Math.max(5, number(runtime.standby_poll_seconds, 30)));
        continue;
      }
      const outcome = await runEngineCycle(
        assignment.store,
        primary,
        shardIndex,
        assignment.quota,
        assignment.is_standby,
      );
      if (outcome.daily_limited) {
        await withAssignmentLock(async () => releaseSlotUnsafe(shardIndex));
      }
      await pauseInterruptibly(outcome.pause_seconds);
    }
  }

  const startedAt = new Date().toISOString();
  let status = {
    contract: config.contract || "flowde-multistore-v1",
    active: true,
    pid: process.pid,
    started_at: startedAt,
    phase: "starting",
  };
  const workerPromises = primaries.map((store, index) => runSlot(store, index));
  try {
    while (!(await stopped())) {
      status = await refreshStatus(config, status, workerStates);
      await writeJsonAtomic(STATUS_FILE, status);
      await pauseInterruptibly(Math.max(2, number(runtime.supervisor_poll_seconds, 5)));
    }
  } finally {
    requestStop();
    await Promise.race([Promise.allSettled(workerPromises), sleep(10_000)]);
    status = await refreshStatus(config, { ...status, active: false }, workerStates);
    status.stopped_at = new Date().toISOString();
    status.stop_reason = terminating ? "signal-received" : "stop-requested";
    await writeJsonAtomic(STATUS_FILE, status);
    await fsp.rm(PID_FILE, { force: true });
  }
}

async function startCommand() {
  await ensureState();
  await loadConfig();
  const pid = number(String(await fsp.readFile(PID_FILE, "utf8").catch(() => "")).trim());
  if (processActive(pid)) {
    process.stdout.write(`${JSON.stringify({ ok: true, already_active: true, pid }, null, 2)}\n`);
    return;
  }
  await fsp.rm(STOP_FILE, { force: true });
  const logFd = fs.openSync(path.join(STATE_DIR, "supervisor.log"), "a");
  const child = spawn(process.execPath, [fileURLToPath(import.meta.url), "supervise"], {
    cwd: PROJECT_ROOT,
    env: process.env,
    detached: true,
    stdio: ["ignore", logFd, logFd],
  });
  fs.closeSync(logFd);
  child.unref();
  for (let attempt = 0; attempt < 40; attempt += 1) {
    await sleep(250);
    const status = await readJson(STATUS_FILE, {});
    if (status.active === true && processActive(status.pid)) {
      process.stdout.write(`${JSON.stringify(status, null, 2)}\n`);
      return;
    }
  }
  throw new Error("FlowDE supervisor did not become active within 10 seconds");
}

async function stopCommand() {
  await ensureState();
  await fsp.writeFile(STOP_FILE, `${new Date().toISOString()}\n`, "utf8");
  const pid = number(String(await fsp.readFile(PID_FILE, "utf8").catch(() => "")).trim());
  const status = await readJson(STATUS_FILE, {});
  const enginePids = [];
  for (const worker of status.store_workers || []) {
    const id = storeId(worker.store_id);
    if (!id) continue;
    await fsp.mkdir(storeDirectory(id), { recursive: true });
    await fsp.writeFile(path.join(storeDirectory(id), "stop.requested"), `${new Date().toISOString()}\n`, "utf8");
    const childPid = number(worker.child_pid);
    if (processActive(childPid)) {
      try { process.kill(childPid, "SIGTERM"); } catch {}
      enginePids.push(childPid);
    }
  }
  if (processActive(pid)) {
    try { process.kill(pid, "SIGTERM"); } catch {}
  }
  process.stdout.write(`${JSON.stringify({ ok: true, stop_requested: true, pid: pid || null, engine_pids: enginePids }, null, 2)}\n`);
}

async function statusCommand() {
  const config = await loadConfig();
  const stored = await readJson(STATUS_FILE, {
    contract: config.contract || "flowde-multistore-v1",
    active: false,
    phase: "never-started",
    store_workers: [...initialWorkerStates(config).values()],
  });
  const pid = number(String(await fsp.readFile(PID_FILE, "utf8").catch(() => "")).trim());
  const quota = await dailyQuotaSnapshot(config);
  const workers = (stored.store_workers || []).map((worker) => ({
    ...worker,
    active: processActive(worker.child_pid),
    child_pid: processActive(worker.child_pid) ? worker.child_pid : null,
    daily_quota: quota.stores[storeId(worker.store_id)] || null,
  }));
  const summary = workerSummary(workers);
  const supervisorActive = processActive(pid);
  const current = {
    ...stored,
    active: supervisorActive,
    pid: supervisorActive ? pid : null,
    phase: supervisorActive
      ? summary.phase
      : stored.phase === "never-started"
        ? "never-started"
        : "stopped",
    daily_quota: quota,
    parallel: {
      ...(stored.parallel || {}),
      ...summary,
    },
    store_workers: workers,
    updated_at: new Date().toISOString(),
  };
  await writeJsonAtomic(STATUS_FILE, current);
  process.stdout.write(`${JSON.stringify(current, null, 2)}\n`);
}

if (path.resolve(process.argv[1] || "") === path.resolve(fileURLToPath(import.meta.url))) {
  const command = process.argv[2] || "status";
  const action = command === "start" ? startCommand
    : command === "supervise" ? supervise
      : command === "stop" ? stopCommand
        : command === "status" ? statusCommand
          : null;
  if (!action) {
    process.stderr.write("usage: flowde.mjs start|status|stop\n");
    process.exitCode = 2;
  } else {
    action().catch((error) => {
      process.stderr.write(`${JSON.stringify({ ok: false, error: String(error?.message || error) })}\n`);
      process.exitCode = 1;
    });
  }
}
