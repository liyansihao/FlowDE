import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

export const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

export function number(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}
export function storeId(value) {
  return String(value ?? "").trim();
}

export function processActive(pid) {
  if (!(number(pid) > 0)) return false;
  try {
    process.kill(number(pid), 0);
    return true;
  } catch {
    return false;
  }
}

export function enabledStores(config = {}) {
  return (Array.isArray(config.stores) ? config.stores : [])
    .filter((store) => store?.enabled !== false && storeId(store?.store_id));
}

export function primaryStores(config = {}) {
  const enabled = enabledStores(config);
  const configuredIds = new Set(
    (config?.runtime?.primary_store_ids || []).map(storeId).filter(Boolean),
  );
  const selected = configuredIds.size > 0
    ? enabled.filter((store) => configuredIds.has(storeId(store.store_id)))
    : enabled;
  const limit = Math.max(
    1,
    Math.min(selected.length, number(config?.runtime?.store_concurrency, selected.length || 1)),
  );
  return selected.slice(0, limit);
}

export function standbyStores(config = {}) {
  const primaryIds = new Set(primaryStores(config).map((store) => storeId(store.store_id)));
  const enabled = enabledStores(config).filter((store) => !primaryIds.has(storeId(store.store_id)));
  const configuredIds = new Set(
    (config?.runtime?.standby_store_ids || []).map(storeId).filter(Boolean),
  );
  return configuredIds.size > 0
    ? enabled.filter((store) => configuredIds.has(storeId(store.store_id)))
    : enabled;
}

export function runtimeStores(config = {}) {
  const rows = [...primaryStores(config), ...standbyStores(config)];
  return [...new Map(rows.map((store) => [storeId(store.store_id), store])).values()];
}

export function localDateKey(value = new Date(), timeZone = "UTC") {
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) return "";
  const parts = Object.fromEntries(new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date).map((part) => [part.type, part.value]));
  return `${parts.year}-${parts.month}-${parts.day}`;
}

export function quotaDayKey(
  value = new Date(),
  timeZone = "UTC",
  resetLocal = "00:00",
) {
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) return "";
  const match = /^(\d{1,2}):(\d{2})$/u.exec(String(resetLocal || "00:00").trim());
  const resetMinutes = match
    ? Math.min(23, Number(match[1])) * 60 + Math.min(59, Number(match[2]))
    : 0;
  const parts = Object.fromEntries(new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(date).map((part) => [part.type, part.value]));
  const localMinutes = Number(parts.hour) * 60 + Number(parts.minute);
  if (localMinutes >= resetMinutes) return `${parts.year}-${parts.month}-${parts.day}`;
  const prior = new Date(Date.UTC(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
  ) - 86_400_000);
  return [
    prior.getUTCFullYear(),
    String(prior.getUTCMonth() + 1).padStart(2, "0"),
    String(prior.getUTCDate()).padStart(2, "0"),
  ].join("-");
}

export function quotaBlockForDay(
  block = null,
  dayKey = "",
  timeZone = "UTC",
  resetLocal = "00:00",
) {
  if (block?.blocked !== true) return null;
  const detectedAt = String(block.detected_at || "").trim();
  if (!detectedAt) return block;
  return quotaDayKey(detectedAt, timeZone, resetLocal) === dayKey ? block : null;
}

export function isCreationAttempt(row = {}) {
  return row.submission_attempted === true
    || ["submitted", "existing", "failed", "error"].includes(String(row.state || ""));
}

export function creationAttemptKeys(rows = [], {
  exactStoreId,
  dayKey,
  timeZone = "UTC",
  resetLocal = "00:00",
  scopedToStore = false,
} = {}) {
  const expectedStoreId = storeId(exactStoreId);
  const keys = new Set();
  for (const row of rows) {
    if (!scopedToStore && storeId(row?.store_id) !== expectedStoreId) continue;
    if (quotaDayKey(row?.at, timeZone, resetLocal) !== dayKey) continue;
    if (!isCreationAttempt(row)) continue;
    const key = String(
      row?.offer_id || row?.publication?.offer_id || row?.item_id || row?.sku || "",
    ).trim();
    if (key) keys.add(key);
  }
  return keys;
}

export function shouldRotateStore(engineStatus = {}) {
  const blockerType = String(engineStatus?.submission_blocker?.type || "");
  const stopReason = String(engineStatus?.stop_reason || "");
  return blockerType === "daily-creation-limit"
    || blockerType === "platform-daily-creation-limit"
    || blockerType === "ozon-daily-product-creation-limit"
    || stopReason === "daily-creation-limit"
    || stopReason === "platform-daily-creation-limit"
    || stopReason === "submission-blocked-ozon-daily-limit"
    || /target store .* (?:missing|unavailable|not found)/iu.test(
      String(engineStatus?.last_error?.error || engineStatus?.error || ""),
    );
}

export function emptyCandidateQueue(engineStatus = {}) {
  return String(engineStatus?.stop_reason || "") === "candidate-queue-complete"
    && number(engineStatus?.counts?.scanned) === 0;
}

export function shardStaggerSeconds(shardIndex = 0, stepSeconds = 5, maximumSeconds = 120) {
  const index = Math.max(0, Math.floor(number(shardIndex)));
  const step = Math.max(0, number(stepSeconds));
  const maximum = Math.max(0, number(maximumSeconds, 120));
  return Math.min(maximum, index * step);
}

export function enginePauseSeconds(
  engineStatus = {},
  runtime = {},
  exitCode = 0,
  { nowMs = Date.now(), shardIndex = 0 } = {},
) {
  const retryAfterAt = Date.parse(String(engineStatus?.runtime_blocker?.retry_after_at || ""));
  const serverRetrySeconds = Number.isFinite(retryAfterAt)
    ? Math.ceil(Math.max(0, retryAfterAt - nowMs) / 1_000)
    : 0;
  const rateLimited = /(?:api-)?rate-limit/iu.test(String(engineStatus?.stop_reason || ""))
    || /rate-limit/iu.test(String(engineStatus?.runtime_blocker?.type || ""));
  if (rateLimited) {
    return Math.max(60, number(runtime.rate_limit_pause_seconds, 300), serverRetrySeconds)
      + shardStaggerSeconds(shardIndex, runtime.rate_limit_retry_stagger_seconds, 120);
  }
  if (number(exitCode) !== 0) return Math.max(10, number(runtime.error_pause_seconds, 30));
  if (emptyCandidateQueue(engineStatus)) {
    return Math.max(60, number(runtime.empty_queue_pause_seconds, 300));
  }
  return Math.max(1, number(runtime.cycle_pause_seconds, 5));
}

export function workerSummary(workers = []) {
  const rows = Array.isArray(workers) ? workers : [];
  const active = rows.filter((worker) => worker?.active === true).length;
  const quotaBlocked = rows.filter((worker) => (
    worker?.daily_quota?.blocked === true || worker?.phase === "daily-quota-blocked"
  )).length;
  const queueComplete = rows.filter((worker) => (
    worker?.engine_stop_reason === "candidate-queue-complete"
      || worker?.phase === "candidate-queue-complete"
  )).length;
  const rateLimited = rows.filter((worker) => (
    /rate-limit/iu.test(String(worker?.engine_stop_reason || worker?.stop_reason || ""))
      || /rate-limit/iu.test(String(worker?.runtime_blocker?.type || ""))
  )).length;
  return {
    total_workers: rows.length,
    active_workers: active,
    quota_blocked_workers: quotaBlocked,
    queue_complete_workers: queueComplete,
    rate_limited_workers: rateLimited,
    phase: active > 0
      ? "running"
      : rows.length > 0 && quotaBlocked === rows.length
        ? "quota-blocked"
        : rateLimited > 0
          ? "rate-limited"
        : queueComplete > 0
          ? "queue-waiting"
          : "waiting",
  };
}

export function chooseAvailableStore({
  primary,
  standbys = [],
  quotaByStore = {},
  assignedStoreIds = new Set(),
} = {}) {
  for (const candidate of [primary, ...standbys].filter(Boolean)) {
    const id = storeId(candidate.store_id);
    if (!id || quotaByStore[id]?.blocked || assignedStoreIds.has(id)) continue;
    return candidate;
  }
  return null;
}

export async function readJson(filename, fallback = null) {
  try {
    return JSON.parse(await fs.readFile(filename, "utf8"));
  } catch (error) {
    if (error?.code === "ENOENT") return fallback;
    throw error;
  }
}

export async function writeJsonAtomic(filename, value) {
  await fs.mkdir(path.dirname(filename), { recursive: true });
  const temporary = `${filename}.tmp-${process.pid}-${Date.now()}-${randomUUID()}`;
  await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await fs.rename(temporary, filename);
}

export async function readJsonLines(filename) {
  const text = await fs.readFile(filename, "utf8").catch((error) => {
    if (error?.code === "ENOENT") return "";
    throw error;
  });
  return text.split(/\r?\n/u).filter(Boolean).map((line) => JSON.parse(line));
}

export async function appendJsonLine(filename, value) {
  await fs.mkdir(path.dirname(filename), { recursive: true });
  await fs.appendFile(filename, `${JSON.stringify(value)}\n`, "utf8");
}
