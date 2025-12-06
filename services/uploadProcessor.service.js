import fs from 'fs/promises';
import path from 'path';
import XLSX from 'xlsx';
import { buildJobFilePath, writeMetadata } from '../utils/storage.js';
import { markJobActive, markJobComplete } from './jobState.service.js';
import { enrichContacts } from './enricher.service.js';
import { cleanName, cleanDomain } from '../utils/dataCleaner.js';

const ALLOWED_EXTENSIONS = ['.csv', '.xls', '.xlsx'];
const MAX_ROWS = 10000;

const COLUMN_ALIASES = {
  firstName: ['first name', 'firstname', 'first'],
  lastName: ['last name', 'lastname', 'last'],
  website: ['website', 'domain', 'company website', 'company domain'],
};

export async function processUploadedFile({ jobId, jobDir, file, userId, onReady }) {
  markJobActive(jobId);
  const baseMetadata = {
    jobId,
    userId,
    originalFilename: file.originalname,
    storedFilename: path.basename(file.path),
    createdAt: new Date().toISOString(),
  };
  let metadataSnapshot = { ...baseMetadata, status: 'processing' };
  await writeMetadata(jobDir, metadataSnapshot);

  let readyCallbackTriggered = false;

  const notifyReady = async () => {
    if (readyCallbackTriggered || typeof onReady !== 'function') {
      return;
    }
    readyCallbackTriggered = true;
    await onReady({
      jobId,
      metadata: metadataSnapshot,
    });
  };

  try {
    validateExtension(file.originalname);
    const parsed = await parseWorkbook(file.path);
    enforceRowLimit(parsed.rows.length);

    const normalizedRows = normalizeRows(
      parsed.rows,
      resolveColumns(parsed.headers),
      parsed.headerRowIndex,
      parsed.headers,
    );

    const runnableRows = normalizedRows.filter((row) => row.contact);
    const progress = createProgressSnapshot(runnableRows.length, normalizedRows.length - runnableRows.length);

    metadataSnapshot = {
      ...metadataSnapshot,
      totals: {
        totalRows: normalizedRows.length,
        runnableContacts: runnableRows.length,
        skippedRows: normalizedRows.length - runnableRows.length,
      },
      progress,
      lastUpdate: new Date().toISOString(),
    };
    await writeMetadata(jobDir, metadataSnapshot);
    await notifyReady();

    const updateProgress = async (status) => {
      progress.processedContacts += 1;
      const bucket = normalizeStatusBucket(status);
      progress.statusCounts[bucket] = (progress.statusCounts[bucket] || 0) + 1;
      metadataSnapshot = {
        ...metadataSnapshot,
        progress: { ...progress },
        lastUpdate: new Date().toISOString(),
      };
      await writeMetadata(jobDir, metadataSnapshot);
    };

    const contacts = runnableRows.map((row) => row.contact);
    const enrichmentResults = contacts.length
      ? await enrichContacts(contacts, { onResult: async (result) => updateProgress(result.status) })
      : [];

    const { apiResults, csvRows } = buildResultSets(normalizedRows, enrichmentResults);

    const outputFilename = `output-${jobId}-${Date.now()}.csv`;
    const outputPath = await writeCsv(jobDir, outputFilename, csvRows);

    const completionMetadata = {
      ...metadataSnapshot,
      status: 'completed',
      completedAt: new Date().toISOString(),
      resultCount: apiResults.length,
      outputFilename,
      downloadUrl: `/v1/scraper/enricher/download/${jobId}`,
    };
    await writeMetadata(jobDir, completionMetadata);

    return {
      jobId,
      userId,
      outputFile: outputFilename,
      outputPath,
      downloadUrl: `/v1/scraper/enricher/download/${jobId}`,
      results: apiResults,
    };
  } catch (error) {
    await writeMetadata(jobDir, {
      ...metadataSnapshot,
      status: 'failed',
      failedAt: new Date().toISOString(),
      error: error.message,
    });
    throw error;
  } finally {
    markJobComplete(jobId);
    await notifyReady();
  }
}

function validateExtension(filename) {
  const ext = path.extname(filename || '').toLowerCase();
  if (!ALLOWED_EXTENSIONS.includes(ext)) {
    throw new Error('Unsupported file type. Please upload a CSV, XLS, or XLSX file.');
  }
}

async function parseWorkbook(filePath) {
  try {
    const workbook = XLSX.readFile(filePath, { cellDates: false });
    if (!workbook.SheetNames.length) {
      throw new Error('Uploaded file does not contain any sheets.');
    }
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const matrix = XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: '' });
    if (!matrix.length) {
      throw new Error('Uploaded file is empty.');
    }
    const { headerRowIndex, headers } = detectHeaderRow(matrix);
    const dataRows = matrix
      .slice(headerRowIndex + 1)
      .filter((row) => Array.isArray(row) && row.some((cell) => String(cell || '').trim() !== ''));
    if (!dataRows.length) {
      throw new Error('Uploaded file does not contain any data rows under the header.');
    }
    return { rows: dataRows, headers, headerRowIndex };
  } catch (error) {
    throw new Error(`Failed to parse uploaded file: ${error.message}`);
  }
}

function enforceRowLimit(rowCount) {
  if (rowCount > MAX_ROWS) {
    throw new Error(`Row limit exceeded. Maximum supported rows: ${MAX_ROWS}.`);
  }
}

function resolveColumns(headers) {
  const normalizedMap = new Map();
  headers.forEach((header) => {
    normalizedMap.set(normalizeKey(header), header);
  });

  const firstNameKey = findColumnKey(normalizedMap, COLUMN_ALIASES.firstName);
  const lastNameKey = findColumnKey(normalizedMap, COLUMN_ALIASES.lastName);
  const websiteKey = findColumnKey(normalizedMap, COLUMN_ALIASES.website);

  if (!firstNameKey || !lastNameKey || !websiteKey) {
    throw new Error('File must include First Name, Last Name, and Website columns.');
  }

  return { firstNameKey, lastNameKey, websiteKey };
}

function normalizeRows(rows, initialColumnMap, headerRowIndex, initialHeaders) {
  const normalized = [];
  let currentHeaders = [...initialHeaders];
  let currentColumnMap = { ...initialColumnMap };

  rows.forEach((rowValues, index) => {
    const rowNumber = headerRowIndex + 2 + index;

    if (isHeaderRowArray(rowValues)) {
      currentHeaders = sanitizeHeadersFromRow(rowValues);
      currentColumnMap = resolveColumns(currentHeaders);
      return;
    }

    const rowObject = convertRowToObject(currentHeaders, rowValues);
    const sanitized = sanitizeRow(rowObject, currentColumnMap);

    if (!sanitized) {
      return;
    }

    normalized.push({
      rowNumber,
      sanitizedRow: sanitized.sanitizedRow,
      contact: sanitized.contact,
      skipReason: sanitized.skipReason,
      profile: sanitized.profile,
    });
  });

  return normalized;
}

function sanitizeRow(rowObject, columnMap) {
  const rawFirst = rowObject[columnMap.firstNameKey];
  const rawLast = rowObject[columnMap.lastNameKey];
  const rawDomain = rowObject[columnMap.websiteKey];

  const firstName = cleanName(rawFirst);
  const lastName = cleanName(rawLast);
  const domain = cleanDomain(rawDomain);

  const sanitizedRow = { ...rowObject };
  sanitizedRow[columnMap.firstNameKey] = firstName;
  sanitizedRow[columnMap.lastNameKey] = lastName;
  sanitizedRow[columnMap.websiteKey] = domain;

  const profile = { firstName, lastName, domain };

  if (!firstName && !lastName && !domain) {
    return null;
  }

  if (!domain) {
    return { sanitizedRow, contact: null, skipReason: 'Missing website/domain', profile };
  }

  if (!firstName && !lastName) {
    return { sanitizedRow, contact: null, skipReason: 'Missing first and last name', profile };
  }

  return { sanitizedRow, contact: profile, skipReason: null, profile };
}

function normalizeKey(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

function findColumnKey(normalizedHeaderMap, candidates) {
  for (const candidate of candidates) {
    const normalizedCandidate = normalizeKey(candidate);
    if (normalizedHeaderMap.has(normalizedCandidate)) {
      return normalizedHeaderMap.get(normalizedCandidate);
    }
  }
  return null;
}

function buildResultSets(normalizedRows, enrichmentResults) {
  const apiResults = [];
  const csvRows = [];
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
      csvRows.push({
        ...row.sanitizedRow,
        bestEmail: '',
        status: skipResult.status,
        messageSummary: row.skipReason,
      });
      return;
    }

    const result = enrichmentResults[enrichmentIndex] || defaultResult(row.profile);
    enrichmentIndex += 1;
    apiResults.push(result);
    csvRows.push({
      ...row.sanitizedRow,
      bestEmail: result.bestEmail || '',
      status: result.status || '',
      messageSummary: deriveMessageSummary(result),
    });
  });

  return { apiResults, csvRows };
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

async function writeCsv(jobDir, filename, rows) {
  const worksheet = XLSX.utils.json_to_sheet(rows);
  const csv = XLSX.utils.sheet_to_csv(worksheet);
  const outputPath = buildJobFilePath(jobDir, filename);
  await fs.writeFile(outputPath, csv, 'utf-8');
  return outputPath;
}

function deriveMessageSummary(result) {
  if (!result?.details) {
    return '';
  }
  return result.details.message || result.details.reason || '';
}

function detectHeaderRow(matrix) {
  for (let rowIndex = 0; rowIndex < matrix.length; rowIndex += 1) {
    const row = matrix[rowIndex];
    if (!Array.isArray(row)) {
      continue;
    }
    const normalizedCells = row.map((cell) => normalizeKey(cell));
    if (
      containsAlias(normalizedCells, COLUMN_ALIASES.firstName) &&
      containsAlias(normalizedCells, COLUMN_ALIASES.lastName) &&
      containsAlias(normalizedCells, COLUMN_ALIASES.website)
    ) {
      const headers = row.map((cell, idx) => (String(cell || '').trim() ? String(cell) : `column_${idx + 1}`));
      return { headerRowIndex: rowIndex, headers };
    }
  }
  throw new Error('Could not locate required columns (First Name, Last Name, Website). Ensure the file contains a header row.');
}

function containsAlias(normalizedCells, aliasList) {
  return aliasList.some((alias) => normalizedCells.includes(normalizeKey(alias)));
}

function convertRowToObject(headers, rowValues) {
  return headers.reduce((acc, header, idx) => {
    acc[header] = rowValues[idx] ?? '';
    return acc;
  }, {});
}

function isHeaderRowArray(rowValues) {
  if (!Array.isArray(rowValues)) {
    return false;
  }
  const normalizedValues = rowValues.map((value) => normalizeKey(value));
  const hasFirst = COLUMN_ALIASES.firstName.some((alias) => normalizedValues.includes(normalizeKey(alias)));
  const hasLast = COLUMN_ALIASES.lastName.some((alias) => normalizedValues.includes(normalizeKey(alias)));
  const hasDomain = COLUMN_ALIASES.website.some((alias) => normalizedValues.includes(normalizeKey(alias)));
  return hasFirst && hasLast && hasDomain;
}

function sanitizeHeadersFromRow(rowValues) {
  return rowValues.map((cell, idx) => (String(cell || '').trim() ? String(cell) : `column_${idx + 1}`));
}

function createProgressSnapshot(totalContacts, skippedRows) {
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

function normalizeStatusBucket(status) {
  if (!status) {
    return 'other';
  }
  const allowed = new Set(['valid', 'catchall_default', 'not_found_valid_emails', 'error']);
  return allowed.has(status) ? status : 'other';
}
