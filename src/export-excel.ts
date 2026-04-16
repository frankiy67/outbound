/**
 * Export preview JSON → Excel
 *
 * Usage:
 *   npm run export              → accounting leads (preview.json → leads.xlsx)
 *   npm run export:restaurants  → restaurant leads (restaurant_preview.json → restaurant_leads.xlsx)
 */

import * as fs from 'fs';
import * as path from 'path';
import * as XLSX from 'xlsx';
import { DATA_DIR } from './config';

// ── Shared helper ─────────────────────────────────────────────────────────────

function buildWorksheet(
  headers: string[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rows: any[][],
): XLSX.WorkSheet {
  const ws = XLSX.utils.aoa_to_sheet([headers, ...rows]);

  // Bold header row.
  const range = XLSX.utils.decode_range(ws['!ref'] ?? 'A1');
  for (let col = range.s.c; col <= range.e.c; col++) {
    const addr = XLSX.utils.encode_cell({ r: 0, c: col });
    if (ws[addr]) ws[addr].s = { font: { bold: true } };
  }

  // Freeze first row.
  ws['!freeze'] = { xSplit: 0, ySplit: 1, topLeftCell: 'A2', activePane: 'bottomLeft' };

  // Auto-width: longest value per column, capped at 80.
  ws['!cols'] = headers.map((h, ci) => {
    const maxData = rows.reduce((max, row) => {
      const val = row[ci] == null ? '' : String(row[ci]);
      return Math.max(max, val.length);
    }, 0);
    return { wch: Math.min(Math.max(h.length, maxData) + 2, 80) };
  });

  return ws;
}

function writeWorkbook(ws: XLSX.WorkSheet, sheetName: string, outPath: string): void {
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, sheetName);
  XLSX.writeFile(wb, outPath);
  console.log(`Done. File written to ${outPath}`);
}

// ── Accounting export ─────────────────────────────────────────────────────────

function exportAccounting(): void {
  const PREVIEW_PATH = path.join(DATA_DIR, 'preview.json');
  const OUTPUT_PATH  = path.join(DATA_DIR, 'leads.xlsx');

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const data: Record<string, any>[] = JSON.parse(fs.readFileSync(PREVIEW_PATH, 'utf-8'));

  const sample = data[0];
  console.log('All fields in JSON:', Object.keys(sample));
  console.log('company:', sample.company, '| company_name:', sample.company_name);
  console.log('domain :', sample.domain,  '| phone:', sample.phone);
  console.log(`\nExporting ${data.length} leads → ${OUTPUT_PATH}`);

  const HEADERS = [
    'Company', 'Email', 'Phone', 'Domain',
    'Score', 'Tier', 'Signals',
    'Email Subject', 'Email Body',
  ];

  const rows = data.map(l => [
    (l.company      ?? l.company_name ?? ''),
    (l.email        ?? ''),
    (l.phone        ?? ''),
    (l.domain       ?? ''),
    (l.score        ?? ''),
    (l.tier         ?? ''),
    Array.isArray(l.signals) ? l.signals.join(', ') : (l.signals ?? ''),
    (l.draft_emails?.[0]?.subject ?? ''),
    (l.draft_emails?.[0]?.body    ?? ''),
  ]);

  writeWorkbook(buildWorksheet(HEADERS, rows), 'Leads', OUTPUT_PATH);
}

// ── Restaurant export ─────────────────────────────────────────────────────────

function exportRestaurants(): void {
  const PREVIEW_PATH = path.join(DATA_DIR, 'restaurant_preview.json');
  const OUTPUT_PATH  = path.join(DATA_DIR, 'restaurant_leads.xlsx');

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const data: Record<string, any>[] = JSON.parse(fs.readFileSync(PREVIEW_PATH, 'utf-8'));

  console.log(`Exporting ${data.length} restaurant leads → ${OUTPUT_PATH}`);

  const HEADERS = [
    'Restaurant', 'Contact', 'Email', 'Phone', 'Website', 'Rating (⭐)',
    'Score', 'Tier', 'Signals',
    'Email Subject', 'Email Body',
  ];

  const rows = data.map(l => [
    (l.company       ?? ''),
    (l.contact_name  ?? ''),
    (l.email         ?? ''),
    (l.phone         ?? ''),
    (l.website       ?? ''),
    (l.rating        ?? ''),
    (l.score         ?? ''),
    (l.tier          ?? ''),
    Array.isArray(l.signals) ? l.signals.join(', ') : (l.signals ?? ''),
    (l.draft_emails?.[0]?.subject ?? ''),
    (l.draft_emails?.[0]?.body    ?? ''),
  ]);

  writeWorkbook(buildWorksheet(HEADERS, rows), 'Restaurants', OUTPUT_PATH);
}

// ── Entry point ───────────────────────────────────────────────────────────────

const mode = process.argv[2];
if (mode === 'restaurants') {
  exportRestaurants();
} else {
  exportAccounting();
}
