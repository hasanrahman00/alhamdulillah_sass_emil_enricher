// Builds CSV column order and maintains an append-friendly snapshot writer for job outputs.
import fs from 'fs/promises';

export function buildCsvColumnOrder(normalizedRows) {
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

export function createCsvSnapshotWriter(filePath, columns, initialRows) {
  let rows = initialRows.slice();
  let writeQueue = Promise.resolve();

  const scheduleWrite = () => {
    const payload = serializeCsv(columns, rows);
    writeQueue = writeQueue.then(() => fs.writeFile(filePath, payload, 'utf-8'));
    return writeQueue;
  };

  return {
    async writeSnapshot() {
      await scheduleWrite();
    },
    async setRow(rowId, newRow) {
      rows[rowId] = newRow;
      await scheduleWrite();
    },
  };
}

export function composeCsvRowData(baseRow, overrides = {}) {
  return {
    ...baseRow,
    bestEmail: overrides.bestEmail || '',
    status: overrides.status || '',
    messageSummary: overrides.messageSummary || '',
  };
}

function serializeCsv(columns, rows) {
  const headerLine = columns.map((column) => escapeCsvValue(column)).join(',');
  const bodyLines = rows.map((row) => columns.map((column) => escapeCsvValue(row?.[column] ?? '')).join(','));
  const lines = [headerLine, ...bodyLines];
  return `${lines.join('\n')}\n`;
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
