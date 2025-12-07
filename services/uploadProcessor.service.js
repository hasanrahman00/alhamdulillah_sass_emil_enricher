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
    const csvColumns = buildCsvColumnOrder(normalizedRows);
    const outputFilename = `output-${jobId}-${Date.now()}.csv`;
    const outputPath = buildJobFilePath(jobDir, outputFilename);
    const downloadUrl = `/v1/scraper/enricher/download/${jobId}`;
    await initializeCsvFile(outputPath, csvColumns);

    metadataSnapshot = {
      ...metadataSnapshot,
      totals: {
        totalRows: normalizedRows.length,
        runnableContacts: runnableRows.length,
        skippedRows: normalizedRows.length - runnableRows.length,
      },
      progress,
      outputFilename,
      downloadUrl,
      lastUpdate: new Date().toISOString(),
    };
    await writeMetadata(jobDir, metadataSnapshot);
    await notifyReady();

    const appendRowInOrder = createCsvRowAppender(outputPath, csvColumns);
    const rowLookup = new Map(normalizedRows.map((row) => [row.rowId, row]));
    for (const row of normalizedRows) {
      if (row.contact) {
        continue;
      }
      const skipRow = composeCsvRowData(row.sanitizedRow, {
        bestEmail: '',
        status: 'skipped_missing_fields',
        messageSummary: row.skipReason,
      });
      await appendRowInOrder(row.rowId, skipRow);
    }

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

    const contacts = runnableRows.map((row) => ({ ...row.contact, rowId: row.rowId }));

    const appendContactResult = async (resultPayload) => {
      const rowId = resultPayload?.contact?.rowId;
      if (typeof rowId !== 'number') {
        return;
      }
      const rowInfo = rowLookup.get(rowId);
      if (!rowInfo) {
        return;
      }
      const csvRow = composeCsvRowData(rowInfo.sanitizedRow, {
        bestEmail: resultPayload.bestEmail || '',
        status: resultPayload.status || '',
        messageSummary: deriveMessageSummary(resultPayload),
      });
      await appendRowInOrder(rowId, csvRow);
    };

    const enrichmentResults = contacts.length
      ? await enrichContacts(contacts, {
          onResult: async (result) => {
            await appendContactResult(result);
            await updateProgress(result.status);
          },
        })
      : [];

    const { apiResults } = buildResultSets(normalizedRows, enrichmentResults);

    const completionMetadata = {
      ...metadataSnapshot,
      status: 'completed',
      completedAt: new Date().toISOString(),
      resultCount: apiResults.length,
    };
    await writeMetadata(jobDir, completionMetadata);

    return {
      jobId,
      userId,
      outputFile: outputFilename,
      outputPath,
      downloadUrl,
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
  let rowCounter = 0;

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

    const rowId = rowCounter;
    rowCounter += 1;

    normalized.push({
      rowId,
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

function buildCsvColumnOrder(normalizedRows) {
  const columns = [];
  normalizedRows.forEach((row) => {
    Object.keys(row.sanitizedRow || {}).forEach((key) => {
      if (!columns.includes(key)) {
        columns.push(key);
      }
    });
  });
  ['bestEmail', 'status', 'messageSummary'].forEach((extra) => {
    if (!columns.includes(extra)) {
      columns.push(extra);
    }
  });
  return columns;
}

async function initializeCsvFile(filePath, columns) {
  const headerLine = `${columns.map((column) => escapeCsvValue(column)).join(',')}\n`;
  await fs.writeFile(filePath, headerLine, 'utf-8');
}

async function appendCsvRow(filePath, columns, record) {
  const line = `${columns.map((column) => escapeCsvValue(record?.[column] ?? '')).join(',')}\n`;
  await fs.appendFile(filePath, line, 'utf-8');
}

function escapeCsvValue(value) {
  if (value === null || value === undefined) {
    return '';
  }
  const stringValue = String(value);
  if (/[",\n\r]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
}

function createCsvRowAppender(filePath, columns) {
  let nextRowId = 0;
  const pending = new Map();

  return async (rowId, record) => {
    pending.set(rowId, record);
    while (pending.has(nextRowId)) {
      const row = pending.get(nextRowId);
      pending.delete(nextRowId);
      await appendCsvRow(filePath, columns, row);
      nextRowId += 1;
    }
  };
}

function composeCsvRowData(baseRow, overrides = {}) {
  return {
    ...baseRow,
    bestEmail: overrides.bestEmail || '',
    status: overrides.status || '',
    messageSummary: overrides.messageSummary || '',
  };
}
