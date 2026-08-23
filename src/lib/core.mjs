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

export function isCreationAttempt(row = {}) {
  return row.submission_attempted === true
    || ["submitted", "existing", "failed", "error"].includes(String(row.state || ""));
}

export function creationAttemptKeys(rows = [], {
  exactStoreId,
  dayKey,
  timeZone = "UTC",
  scopedToStore = false,
} = {}) {
  const expectedStoreId = storeId(exactStoreId);
  const keys = new Set();
  for (const row of rows) {
    if (!scopedToStore && storeId(row?.store_id) !== expectedStoreId) continue;
    if (localDateKey(row?.at, timeZone) !== dayKey) continue;
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
    || stopReason === "daily-creation-limit"
    || stopReason === "platform-daily-creation-limit"
    || /target store .* (?:missing|unavailable|not found)/iu.test(
      String(engineStatus?.last_error?.error || engineStatus?.error || ""),
    );
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
  const temporary = `${filename}.tmp-${process.pid}-${Date.now()}`;
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
