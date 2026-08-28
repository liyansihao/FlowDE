import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import {
  processActive,
  readJson,
  storeId as normalizeStoreId,
  writeJsonAtomic,
} from "./core.mjs";

function itemKey(row = {}) {
  return String(row.item_id || row.offer_id || row.sku || "").trim();
}

function successfulAttempt(row = {}) {
  return ["submitted", "existing", "available", "completed"]
    .includes(String(row.state || ""));
}

function quotaFailure(row = {}) {
  if (row.quota_failure === true) return true;
  return /daily.*(?:creation.*)?limit|quota.*limit|每日.*(?:创建|商品).*额度/iu.test(
    JSON.stringify({
      reason: row.reason,
      error: row.error,
      submission_blocker: row.submission_blocker,
    }),
  );
}

function submissionAttempt(row = {}) {
  if (quotaFailure(row)) return false;
  return row.submission_attempted === true
    || ["submitted", "existing", "failed", "error", "available", "completed"]
      .includes(String(row.state || ""));
}

export function stableBucket(value, bucketCount = 1) {
  const count = Math.max(1, Math.floor(Number(bucketCount) || 1));
  let hash = 2_166_136_261;
  for (const character of String(value || "")) {
    hash ^= character.codePointAt(0);
    hash = Math.imul(hash, 16_777_619);
  }
  return (hash >>> 0) % count;
}

export function crossStoreReuseEligible(itemId, exactStoreId, divisor = 5) {
  const id = String(itemId || "").trim();
  const targetStoreId = normalizeStoreId(exactStoreId);
  if (!id || !targetStoreId) return false;
  return stableBucket(`${id}:${targetStoreId}`, divisor) === 0;
}

export function duplicateDisposition(rows = [], {
  itemId,
  storeId,
  reuseDivisor = 5,
} = {}) {
  const id = String(itemId || "").trim();
  const targetStoreId = normalizeStoreId(storeId);
  if (!id || !targetStoreId) throw new Error("itemId and storeId are required");
  const attempts = (Array.isArray(rows) ? rows : [])
    .filter((row) => itemKey(row) === id && submissionAttempt(row));
  const sameStoreAttempt = attempts.some((row) => (
    normalizeStoreId(row.store_id || row.target_store_id) === targetStoreId
  ));
  const successfulStores = [...new Set(attempts
    .filter(successfulAttempt)
    .map((row) => normalizeStoreId(row.store_id || row.target_store_id))
    .filter(Boolean))];

  if (sameStoreAttempt) {
    return {
      action: "block",
      reason: "same-store-attempted",
      prior_successful_store_ids: successfulStores,
    };
  }
  if (attempts.length === 0) {
    return {
      action: "new",
      reason: "not-attempted",
      prior_successful_store_ids: [],
    };
  }
  if (successfulStores.length > 0
    && crossStoreReuseEligible(id, targetStoreId, reuseDivisor)) {
    return {
      action: "reuse",
      reason: "successful-cross-store-stable-sample",
      prior_successful_store_ids: successfulStores,
      deterministic_sample_divisor: Math.max(1, Math.floor(Number(reuseDivisor) || 1)),
    };
  }
  return {
    action: "block",
    reason: successfulStores.length > 0
      ? "cross-store-success-outside-stable-sample"
      : "cross-store-attempt-not-successful",
    prior_successful_store_ids: successfulStores,
  };
}

export function itemClaimPath(claimDirectory, itemId) {
  const safeItemId = String(itemId || "").replace(/[^\p{L}\p{N}._-]+/gu, "_");
  if (!safeItemId) throw new Error("item claim requires an item ID");
  return path.join(path.resolve(claimDirectory), `${safeItemId}.json`);
}

export async function acquireItemClaim({
  claimDirectory,
  itemId,
  storeId,
  storeName = null,
  allowCompletedCrossStoreReuse = false,
  isProcessActive = processActive,
} = {}) {
  const targetStoreId = normalizeStoreId(storeId);
  if (!targetStoreId) throw new Error("item claim requires a store ID");
  await fs.mkdir(path.resolve(claimDirectory), { recursive: true });
  const filename = itemClaimPath(claimDirectory, itemId);

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const token = randomUUID();
    let handle = null;
    try {
      handle = await fs.open(filename, "wx");
      const claim = {
        contract: "flowde-item-claim-v1",
        item_id: String(itemId),
        state: "claimed",
        token,
        pid: process.pid,
        store_id: targetStoreId,
        store_name: storeName,
        claimed_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
      await handle.writeFile(`${JSON.stringify(claim, null, 2)}\n`, "utf8");
      await handle.close();
      return { acquired: true, filename, ...claim };
    } catch (error) {
      await handle?.close().catch(() => null);
      if (error?.code !== "EEXIST") throw error;
      const existing = await readJson(filename, { state: "unknown" });
      if (existing?.state === "claimed" && !isProcessActive(existing?.pid)) {
        await fs.unlink(filename).catch(() => null);
        continue;
      }
      const reusable = allowCompletedCrossStoreReuse
        && existing?.state === "completed"
        && normalizeStoreId(existing?.store_id)
        && normalizeStoreId(existing.store_id) !== targetStoreId;
      if (!reusable) return { acquired: false, filename, existing };

      const reuseLock = `${filename}.reuse.lock`;
      let lockHandle = null;
      let ownsLock = false;
      try {
        lockHandle = await fs.open(reuseLock, "wx");
        ownsLock = true;
        await lockHandle.writeFile(`${JSON.stringify({
          pid: process.pid,
          token,
          store_id: targetStoreId,
          created_at: new Date().toISOString(),
        })}\n`, "utf8");
        await lockHandle.close();
        lockHandle = null;
        const latest = await readJson(filename, { state: "unknown" });
        if (latest?.state !== "completed"
          || !normalizeStoreId(latest?.store_id)
          || normalizeStoreId(latest.store_id) === targetStoreId) {
          return { acquired: false, filename, existing: latest };
        }
        const claim = {
          contract: "flowde-item-claim-v1",
          item_id: String(itemId),
          state: "claimed",
          token,
          pid: process.pid,
          store_id: targetStoreId,
          store_name: storeName,
          reused_after_store_id: normalizeStoreId(latest.store_id),
          claimed_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        };
        await writeJsonAtomic(filename, claim);
        return { acquired: true, filename, ...claim };
      } catch (error) {
        await lockHandle?.close().catch(() => null);
        if (error?.code !== "EEXIST") throw error;
        const currentLock = await readJson(reuseLock, null).catch(() => null);
        if (currentLock && !isProcessActive(currentLock.pid)) {
          await fs.unlink(reuseLock).catch(() => null);
          continue;
        }
      } finally {
        if (ownsLock) await fs.unlink(reuseLock).catch(() => null);
      }
      return { acquired: false, filename, existing };
    }
  }
  return {
    acquired: false,
    filename,
    existing: await readJson(filename, { state: "unknown" }),
  };
}

export async function updateItemClaim(claim, state, details = {}) {
  if (!claim?.acquired || !claim?.filename || !claim?.token) return false;
  const existing = await readJson(claim.filename, null);
  if (!existing || existing.token !== claim.token) return false;
  await writeJsonAtomic(claim.filename, {
    ...existing,
    ...details,
    state,
    updated_at: new Date().toISOString(),
  });
  return true;
}

export async function releaseItemClaim(claim) {
  if (!claim?.acquired || !claim?.filename || !claim?.token) return false;
  const existing = await readJson(claim.filename, null);
  if (!existing || existing.token !== claim.token) return false;
  await fs.unlink(claim.filename).catch((error) => {
    if (error?.code !== "ENOENT") throw error;
  });
  return true;
}
