/**
 * Export artisan_preview.json → artisan_leads.xlsx
 *
 * Usage:
 *   npm run export:artisans
 */

import * as fs from 'fs';
import * as path from 'path';
import * as XLSX from 'xlsx';
import { DATA_DIR } from './config';

const PREVIEW_PATH = path.join(DATA_DIR, 'artisan_preview.json');
const OUTPUT_PATH  = path.join(DATA_DIR, 'artisan_leads.xlsx');

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const data: Record<string, any>[] = JSON.parse(fs.readFileSync(PREVIEW_PATH, 'utf-8'));

console.log(`Exporting ${data.length} artisan leads → ${OUTPUT_PATH}`);

// ── Build rows ───────────────────────────────────────────────────────────────

const HEADERS = [
  'Company', 'Trade', 'Website', 'Phone', 'Email',
  'Rating (⭐)', 'Score', 'Tier', 'Signals',
  'Email Subject', 'Email Body',
];

const rows = data.map(l => [
  (l.company       ?? ''),
  (l.trade         ?? ''),
  (l.website       ?? ''),
  (l.phone         ?? ''),
  (l.email         ?? ''),
  (l.rating        ?? ''),
  (l.score         ?? ''),
  (l.tier          ?? ''),
  Array.isArray(l.signals) ? l.signals.join(', ') : (l.signals ?? ''),
  (l.draft_emails?.[0]?.subject ?? ''),
  (l.draft_emails?.[0]?.body    ?? ''),
]);

// ── Build worksheet ──────────────────────────────────────────────────────────

const wsData = [HEADERS, ...rows];
const ws = XLSX.utils.aoa_to_sheet(wsData);

// Bold header row.
const headerRange = XLSX.utils.decode_range(ws['!ref'] ?? 'A1');
for (let col = headerRange.s.c; col <= headerRange.e.c; col++) {
  const addr = XLSX.utils.encode_cell({ r: 0, c: col });
  if (ws[addr]) ws[addr].s = { font: { bold: true } };
}

// Freeze first row.
ws['!freeze'] = { xSplit: 0, ySplit: 1, topLeftCell: 'A2', activePane: 'bottomLeft' };

// Auto-width: longest value per column, capped at 80.
ws['!cols'] = HEADERS.map((h, ci) => {
  const maxData = rows.reduce((max, row) => {
    const val = row[ci] == null ? '' : String(row[ci]);
    return Math.max(max, val.length);
  }, 0);
  return { wch: Math.min(Math.max(h.length, maxData) + 2, 80) };
});

// ── Write workbook ───────────────────────────────────────────────────────────

const wb = XLSX.utils.book_new();
XLSX.utils.book_append_sheet(wb, ws, 'Artisans');
XLSX.writeFile(wb, OUTPUT_PATH);

console.log(`Done. File written to ${OUTPUT_PATH}`);
