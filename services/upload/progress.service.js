// Tracks per-job enrichment progress and normalizes status buckets for metadata snapshots.
export function createProgressSnapshot(totalContacts, skippedRows) {
  return {
    totalContacts,
    processedContacts: 0,
    statusCounts: {
      valid: 0,
      catchall_default: 0,
      not_found_valid_emails: 0,
      error: 0,
      other: 0,
      skipped: skippedRows,
    },
  };
}

export function normalizeStatusBucket(status) {
  if (!status) {
    return 'other';
  }
  const allowed = new Set(['valid', 'catchall_default', 'not_found_valid_emails', 'error']);
  return allowed.has(status) ? status : 'other';
}
