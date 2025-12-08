import { config } from '../config/env.js';

const MAX_COMBOS_DEFAULT = 8;

/**
 * Processes contacts in configurable batch waves, verifying candidate emails sequentially per contact.
 * Each wave pulls up to comboBatchSize unresolved contacts and advances them by one combo.
 *
 * @param {Array<{firstName: string, lastName: string, domain: string}>} contacts
 * @param {{ verifyEmail: (email: string) => Promise<object>, generatePatterns: (contact: object) => string[], maxCombos?: number }} options
 * @returns {Promise<Array<{contact: object, bestEmail: string|null, status: string|null, details: object, resultsPerCombo: Array<{email: string, code: string|null, message: string|null, error: string|null}>}>>}
 */
export async function processContactsInBatches(contacts, {
  verifyEmail,
  generatePatterns,
  maxCombos = MAX_COMBOS_DEFAULT,
  onResult,
}) {
  const batchSize = Math.max(1, Number(config.comboBatchSize) || 1);
  const states = contacts.map((contact) => ({
    contact,
    patterns: generatePatterns(contact) || [],
    currentComboIndex: 0,
    done: false,
    bestEmail: null,
    status: null,
    details: {},
    resultsPerCombo: [],
  }));

  states.forEach((state) => {
    console.log('[ComboProcessor] Initialized contact', {
      contact: state.contact,
      patternCount: state.patterns.length,
      patterns: state.patterns,
    });
  });

  const processLoop = async () => {
    while (true) {
      const pendingStates = states.filter((state) => !state.done && state.currentComboIndex < maxCombos && state.currentComboIndex < state.patterns.length);
      if (pendingStates.length === 0) {
        // Nothing left to process; finalize any states that exhausted patterns without explicit status.
        await Promise.all(
          states.map((state) => {
            if (!state.done) {
              return finalizeState(state, onResult);
            }
            return null;
          }),
        );
        break;
      }

      const batch = pendingStates.slice(0, batchSize);
      await Promise.all(batch.map((state) => advanceState(state, verifyEmail, maxCombos, onResult)));
    }
  };

  await processLoop();

  return states.map((state) => ({
    contact: state.contact,
    bestEmail: state.bestEmail,
    status: state.status,
    details: state.details,
    resultsPerCombo: state.resultsPerCombo,
  }));
}

async function advanceState(state, verifyEmail, maxCombos, notify) {
  if (state.done) {
    return;
  }

  if (state.currentComboIndex >= maxCombos || state.currentComboIndex >= state.patterns.length) {
    await finalizeState(state, notify);
    return;
  }

  const email = state.patterns[state.currentComboIndex];
  let result;
  try {
    console.log('[ComboProcessor] Verifying candidate', {
      contact: state.contact,
      comboIndex: state.currentComboIndex,
      email,
    });
    result = await verifyEmail(email);
    console.log('[ComboProcessor] Result received', {
      email,
      code: result?.code ?? null,
      message: result?.message ?? null,
    });
  } catch (error) {
    result = { code: null, message: null, error: error.message };
    console.error('[ComboProcessor] Verification threw', {
      email,
      error: error.message,
    });
  }

  state.resultsPerCombo.push({
    email,
    code: result?.code ?? null,
    message: result?.message ?? null,
    error: result?.error ?? null,
  });

  if (state.currentComboIndex === 0 && isMissingMxRecords(result)) {
    console.log('[ComboProcessor] Terminating contact due to missing MX records', {
      contact: state.contact,
      email,
      message: result?.message ?? result?.raw?.message ?? null,
    });
    state.bestEmail = null;
    state.status = 'not_found_valid_emails';
    state.details = { reason: 'Domain missing MX records' };
    state.done = true;
    if (notify) {
      await notify(buildResultPayload(state));
    }
    return;
  }

  if (result?.code === 'ok') {
    state.bestEmail = email;
    state.status = 'valid';
    state.details = { code: result.code, message: result.message };
    state.done = true;
    if (notify) {
      await notify(buildResultPayload(state));
    }
    return;
  }

  state.currentComboIndex += 1;
  if (state.currentComboIndex >= maxCombos || state.currentComboIndex >= state.patterns.length) {
    await finalizeState(state, notify);
  }
}

async function finalizeState(state, notify) {
  if (state.done) {
    return;
  }

  const allCatchAll = state.resultsPerCombo.length > 0 && state.resultsPerCombo.every((entry) => entry.message === 'Catch-All');

  if (allCatchAll) {
    state.bestEmail = state.patterns[2] || state.patterns[0] || null;
    state.status = 'catchall_default';
    state.details = { reason: 'All candidates returned Catch-All' };
  } else {
    const firstError = state.resultsPerCombo.find((entry) => entry.error)?.error;
    state.bestEmail = null;
    state.status = 'not_found_valid_emails';
    state.details = {
      reason: 'All candidates rejected or unverifiable',
      ...(firstError ? { lastError: firstError } : {}),
    };
  }

  state.done = true;

  console.log('[ComboProcessor] Finalized contact', {
    contact: state.contact,
    status: state.status,
    bestEmail: state.bestEmail,
    checkedCombos: state.resultsPerCombo,
  });

  if (notify) {
    await notify(buildResultPayload(state));
  }
}

function buildResultPayload(state) {
  return {
    contact: state.contact,
    bestEmail: state.bestEmail,
    status: state.status,
    details: state.details,
    resultsPerCombo: state.resultsPerCombo,
  };
}

function isMissingMxRecords(result) {
  const candidates = [
    result?.code,
    result?.message,
    result?.raw?.code,
    result?.raw?.message,
    result?.raw?.reason,
    result?.error,
  ];

  return candidates.some((value) => containsNoMxSignal(value));
}

function containsNoMxSignal(value) {
  if (typeof value !== 'string') {
    return false;
  }
  const normalized = value.toLowerCase();
  if (!normalized.includes('mx')) {
    return false;
  }
  return normalized.includes('no ') || normalized.includes('not ') || normalized.includes('missing') || normalized.includes('without');
}
