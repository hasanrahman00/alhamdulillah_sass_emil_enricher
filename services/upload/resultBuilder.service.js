// Shapes final API payloads by merging normalized rows with enrichment results and summaries.
export function buildResultSets(normalizedRows, enrichmentResults) {
  const apiResults = [];
  let enrichmentIndex = 0;

  normalizedRows.forEach((row) => {
    if (!row.contact) {
      const skipResult = {
        firstName: row.profile.firstName,
        lastName: row.profile.lastName,
        domain: row.profile.domain,
        bestEmail: null,
        status: 'skipped_missing_fields',
        details: { reason: row.skipReason },
        allCheckedCandidates: [],
      };
      apiResults.push(skipResult);
      return;
    }

    const result = enrichmentResults[enrichmentIndex] || defaultResult(row.profile);
    enrichmentIndex += 1;
    apiResults.push(result);
  });

  return { apiResults };
}

export function deriveMessageSummary(result) {
  if (!result?.details) {
    return '';
  }
  return result.details.message || result.details.reason || '';
}

function defaultResult(profile) {
  return {
    firstName: profile.firstName,
    lastName: profile.lastName,
    domain: profile.domain,
    bestEmail: null,
    status: 'error',
    details: { reason: 'Unexpected processing mismatch' },
    allCheckedCandidates: [],
  };
}
