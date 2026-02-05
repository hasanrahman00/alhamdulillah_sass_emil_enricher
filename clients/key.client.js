import axios from 'axios';
import { config } from '../config/env.js';

const WAIT_FALLBACK_MS = 1000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractWaitDuration(payload) {
  const rawDelay =
    payload?.waitForMs ??
    payload?.waitMs ??
    payload?.retryAfterMs ??
    payload?.retryInMs ??
    payload?.nextRequestAllowedInMs;
  const parsed = Number(rawDelay);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : WAIT_FALLBACK_MS;
}

function normalizeKeyValue(value) {
  if (value == null) {
    return null;
  }
  if (typeof value === 'string' || typeof value === 'number') {
    return String(value).replace(/[{}]/g, '').trim();
  }
  if (typeof value === 'object') {
    return normalizeKeyValue(value.key ?? value.subscriptionId ?? value.id);
  }
  return null;
}

function extractKeyInfo(data) {
  const entry = Array.isArray(data?.keys) && data.keys.length > 0 ? data.keys[0] : data?.key ?? data;
  const rawKey = entry?.key ?? entry?.subscriptionId ?? entry?.id ?? entry;
  const normalizedKey = normalizeKeyValue(rawKey);
  const details = entry && typeof entry === 'object' ? entry : null;
  return { normalizedKey, details };
}

/**
 * Fetches a MailTester subscription key from the key-rotation microservice.
 * Always retrieves a fresh allocation and respects "wait" instructions.
 *
 * @returns {Promise<{key: string, status?: string, avgRequestIntervalMs?: number, nextRequestAllowedAt?: string}>}
 */
export async function getMailtesterKey() {
  while (true) {
    try {
      const response = await axios.get(config.keyProviderUrl);
      const payload = response.data || {};
      const { normalizedKey, details } = extractKeyInfo(payload);
      const status = typeof payload.status === 'string' ? payload.status.toLowerCase() : null;

      if (status === 'wait' && !normalizedKey) {
        const waitMs = extractWaitDuration(payload);
        console.log('[KeyClient] Rotation service asked us to wait', { waitMs });
        await sleep(waitMs);
        continue;
      }

      if (!normalizedKey) {
        throw new Error('Key provider response missing subscription key');
      }

      const avgRequestIntervalMs = details?.avgRequestIntervalMs ?? payload.avgRequestIntervalMs;
      const nextRequestAllowedAt = details?.nextRequestAllowedAt ?? payload.nextRequestAllowedAt;

      return {
        ...payload,
        ...(details || {}),
        avgRequestIntervalMs,
        nextRequestAllowedAt,
        key: normalizedKey,
      };
    } catch (error) {
      throw new Error(`Failed to retrieve MailTester key: ${error.message}`);
    }
  }
}