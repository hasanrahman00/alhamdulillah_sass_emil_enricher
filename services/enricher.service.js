import { generatePatterns } from '../utils/emailPatterns.js';
import { verifyEmail } from '../clients/mailtester.client.js';

/**
 * Enriches a single contact by generating email candidates and validating them.
 * Applies Rules A, B, and C as described in the specification.
 *
 * @param {{firstName: string, lastName: string, domain: string}} contact
 * @returns {Promise<object>} Result containing bestEmail, status, details, and allCheckedCandidates
 */
export async function enrichContact(contact) {
  const { firstName, lastName, domain } = contact;
  const candidates = generatePatterns({ firstName, lastName, domain });
  const allCheckedCandidates = [];
  let bestEmail = null;
  let status = null;
  let details = {};

  // Flags to evaluate Rule B and C
  let foundValid = false;
  let allCatchAll = true;

  for (let i = 0; i < candidates.length; i++) {
    const email = candidates[i];
    const result = await verifyEmail(email);
    allCheckedCandidates.push({
      email,
      code: result.code,
      message: result.message,
      error: result.error || null,
    });

    // Rule A: stop on first valid
    if (result.code === 'ok') {
      bestEmail = email;
      status = 'valid';
      details = { code: result.code, message: result.message };
      foundValid = true;
      allCatchAll = false;
      break;
    }
    // Evaluate catch-all presence
    if (result.message !== 'Catch-All') {
      allCatchAll = false;
    }
  }

  // If none valid after testing all candidates
  if (!foundValid) {
    if (allCatchAll && allCheckedCandidates.length > 0) {
      // Rule B: all responses were catch-all; choose first name only variant (index 2)
      const firstOnly = candidates[2];
      bestEmail = firstOnly;
      status = 'catchall_default';
      details = { reason: 'All candidates returned Catch-All' };
    } else {
      // Rule C: none valid and not all catch-all
      bestEmail = null;
      status = 'not_found_valid_emails';
      const firstError = allCheckedCandidates.find((candidate) => candidate.error)?.error;
      details = {
        reason: 'All candidates rejected or unverifiable',
        ...(firstError ? { lastError: firstError } : {}),
      };
    }
  }

  return {
    firstName,
    lastName,
    domain,
    bestEmail,
    status,
    details,
    allCheckedCandidates,
  };
}

/**
 * Processes an array of contacts using enrichContact sequentially.
 * Errors for individual contacts are captured and returned in the result array.
 *
 * @param {Array<{firstName: string, lastName: string, domain: string}>} contacts
 * @returns {Promise<Array<object>>}
 */
export async function enrichContacts(contacts) {
  const results = [];
  for (const contact of contacts) {
    try {
      const result = await enrichContact(contact);
      results.push(result);
    } catch (error) {
      results.push({
        firstName: contact.firstName,
        lastName: contact.lastName,
        domain: contact.domain,
        bestEmail: null,
        status: 'error',
        details: { errorMessage: error.message },
        allCheckedCandidates: [],
      });
    }
  }
  return results;
}