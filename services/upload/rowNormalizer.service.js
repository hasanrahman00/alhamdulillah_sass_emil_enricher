// Converts worksheet rows into sanitized contact profiles while tracking skip reasons.
import { cleanName, cleanDomain } from '../../utils/dataCleaner.js';
import { COLUMN_ALIASES } from './upload.constants.js';
import { normalizeKey } from './normalization.utils.js';

export function resolveColumns(headers) {
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

export function normalizeRows(rows, initialColumnMap, headerRowIndex, initialHeaders) {
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

export function sanitizeRow(rowObject, columnMap) {
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

function findColumnKey(normalizedHeaderMap, candidates) {
  for (const candidate of candidates) {
    const normalizedCandidate = normalizeKey(candidate);
    if (normalizedHeaderMap.has(normalizedCandidate)) {
      return normalizedHeaderMap.get(normalizedCandidate);
    }
  }
  return null;
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
