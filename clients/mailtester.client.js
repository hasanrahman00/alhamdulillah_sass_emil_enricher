import axios from 'axios';
import { config } from '../config/env.js';
import { getMailtesterKey } from './key.client.js';
import { waitForThrottle } from '../utils/rateLimiter.js';

/**
 * Verifies an email address using MailTester Ninja.
 * Enforces a minimum delay between calls via the rate limiter.
 *
 * @param {string} email The email address to verify.
 * @returns {Promise<{email: string, code: string|null, message: string|null, raw: any, error?: string}>}
 */
export async function verifyEmail(email) {
  try {
    // Respect rate limits by waiting if necessary.
    await waitForThrottle(config.minDelayMs);

    // Obtain API key from the key provider (cached after first call).
    const keyInfo = await getMailtesterKey();
    const key = keyInfo.key;
    if (!key) {
      throw new Error('Missing MailTester key');
    }

    // Prepare the request URL with encoded query parameters.
    const url = `${config.mailTesterBaseUrl}?email=${encodeURIComponent(email)}&key=${encodeURIComponent(key)}`;
    console.log('[MailTester] Requesting verification', { email, url });
    const response = await axios.get(url);
    const data = response.data || {};

    console.log('[MailTester] Response received', { email, code: data.code, message: data.message });

    return {
      email,
      code: data.code || null,
      message: data.message || null,
      raw: data,
    };
  } catch (error) {
    console.error('[MailTester] Verification failed', { email, error: error.message, response: error.response?.data });
    // Capture any error, including HTTP errors, and include in returned object.
    return {
      email,
      code: null,
      message: null,
      raw: error.response?.data,
      error: error.message,
    };
  }
}