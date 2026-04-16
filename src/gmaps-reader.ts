/**
 * STEP 2 — Read and parse Google Maps scraper CSV output
 *
 * Input:  D:\outbound-data\gmaps-results.csv
 * Columns: input_id,title,website,phone,review_rating,review_count,emails,complete_address
 *
 * Matching strategy:
 *  1. SIRET in input_id  → confidence 100
 *  2. Fuzzy name + postal code → confidence varies
 */

import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse/sync';
import { DATA_DIR } from './config';

export const GMAPS_CSV = path.join(DATA_DIR, 'gmaps-results.csv');

export interface GmapsMatch {
  siret: string;
  website: string | null;
  phone: string | null;
  rating: number | null;
  reviewCount: number | null;
  gmapsEmail: string | null;
  matchConfidence: number;
}

interface GmapsRow {
  input_id: string;
  title: string;
  website: string;
  phone: string;
  review_rating: string;
  review_count: string;
  emails: string;
  complete_address: string;
}

// ── Text normalization ────────────────────────────────────────────────────────

const LEGAL_TERMS = /\b(sarl|sas|sasu|eurl|sa|sci|snc|ei|auto[-\s]?entrepreneur|auto[-\s]?entreprise|entreprise|et\s+fils|et\s+cie|cie|compagnie|group|groupe)\b/gi;

function normalize(s: string): string {
  return s
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(LEGAL_TERMS, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// Simple Levenshtein-based similarity (0–100)
function similarity(a: string, b: string): number {
  if (!a || !b) return 0;
  if (a === b) return 100;

  const la = a.length;
  const lb = b.length;
  const matrix: number[][] = Array.from({ length: la + 1 }, (_, i) =>
    Array.from({ length: lb + 1 }, (_, j) => (i === 0 ? j : j === 0 ? i : 0))
  );

  for (let i = 1; i <= la; i++) {
    for (let j = 1; j <= lb; j++) {
      if (a[i - 1] === b[j - 1]) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = 1 + Math.min(matrix[i - 1][j], matrix[i][j - 1], matrix[i - 1][j - 1]);
      }
    }
  }

  const dist = matrix[la][lb];
  return Math.round((1 - dist / Math.max(la, lb)) * 100);
}

// ── Email cleaning ────────────────────────────────────────────────────────────

const JUNK_EMAIL_PATTERNS = [
  /noreply/i,
  /no-reply/i,
  /webmaster/i,
  /support@wix/i,
  /support@jimdo/i,
  /support@wordpress/i,
  /info@wix/i,
  /contact@wix/i,
  /admin@/i,
  /abuse@/i,
  /postmaster@/i,
  /hostmaster@/i,
  /domains@/i,
  /privacy@/i,
  /gdpr@/i,
  /@example\./i,
  /@test\./i,
  /@mailchimp/i,
  /@sendgrid/i,
  /\.png$/i,
  /\.jpg$/i,
  /\.gif$/i,
];

function cleanEmail(raw: string): string | null {
  if (!raw?.trim()) return null;

  const emails = raw.split(/[,;|\s]+/).map(e => e.trim().toLowerCase()).filter(Boolean);

  for (const email of emails) {
    if (!email.includes('@')) continue;
    if (JUNK_EMAIL_PATTERNS.some(re => re.test(email))) continue;
    // Basic format check
    if (!/^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$/.test(email)) continue;
    return email;
  }

  return null;
}

// ── CSV loading ───────────────────────────────────────────────────────────────

function loadCsv(): GmapsRow[] {
  if (!fs.existsSync(GMAPS_CSV)) return [];
  const content = fs.readFileSync(GMAPS_CSV, 'utf-8');
  return parse(content, { columns: true, skip_empty_lines: true, trim: true }) as GmapsRow[];
}

// Extract SIRET from input_id (last token after #!#)
function extractSiretFromInputId(inputId: string): string | null {
  const match = inputId.match(/#!#(\d{14})/);
  return match ? match[1] : null;
}

// Extract postal code from complete_address
function extractPostalFromAddress(address: string): string | null {
  const match = address.match(/\b(\d{5})\b/);
  return match ? match[1] : null;
}

// ── Build lookup index ────────────────────────────────────────────────────────

interface IndexedRow extends GmapsRow {
  _siret: string | null;
  _normalizedTitle: string;
  _postal: string | null;
}

let _index: IndexedRow[] | null = null;

function getIndex(): IndexedRow[] {
  if (_index) return _index;
  const rows = loadCsv();
  _index = rows.map(r => ({
    ...r,
    _siret: extractSiretFromInputId(r.input_id),
    _normalizedTitle: normalize(r.title),
    _postal: extractPostalFromAddress(r.complete_address),
  }));
  return _index;
}

// ── Public API ────────────────────────────────────────────────────────────────

export function findGmapsMatch(
  siret: string,
  companyName: string,
  postalCode: string,
): GmapsMatch | null {
  const index = getIndex();
  const normName = normalize(companyName);

  let bestRow: IndexedRow | null = null;
  let bestConfidence = 0;

  for (const row of index) {
    let confidence = 0;

    // Strategy 1: SIRET match → instant win
    if (row._siret && row._siret === siret) {
      confidence = 100;
      bestRow = row;
      bestConfidence = confidence;
      break;
    }

    // Strategy 2: Fuzzy match
    const sim = similarity(normName, row._normalizedTitle);
    const samePostal = postalCode && row._postal && postalCode === row._postal;

    if (sim > 75 && samePostal) {
      confidence = Math.round(sim * 0.9); // up to 90
    } else if (sim > 85) {
      confidence = Math.round(sim * 0.85); // up to ~85
    }

    if (confidence > bestConfidence) {
      bestConfidence = confidence;
      bestRow = row;
    }
  }

  if (!bestRow || bestConfidence === 0) return null;

  return {
    siret,
    website: bestRow.website?.trim() || null,
    phone: bestRow.phone?.trim() || null,
    rating: bestRow.review_rating ? parseFloat(bestRow.review_rating) || null : null,
    reviewCount: bestRow.review_count ? parseInt(bestRow.review_count, 10) || null : null,
    gmapsEmail: cleanEmail(bestRow.emails),
    matchConfidence: bestConfidence,
  };
}

export function gmapsCsvExists(): boolean {
  return fs.existsSync(GMAPS_CSV);
}

// ── Stats flag ────────────────────────────────────────────────────────────────

export function printGmapsStats(matches: (GmapsMatch | null)[]): void {
  const total = matches.length;
  const matched = matches.filter(Boolean).length;
  const withEmail = matches.filter(m => m?.gmapsEmail).length;
  const withPhone = matches.filter(m => m?.phone).length;

  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📊 GMAPS READER STATS');
  console.log(`  Matched:    ${matched}/${total} (${Math.round(matched / total * 100)}%)`);
  console.log(`  With email: ${withEmail}/${total} (${Math.round(withEmail / total * 100)}%)`);
  console.log(`  With phone: ${withPhone}/${total} (${Math.round(withPhone / total * 100)}%)`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
}

// ── CLI: npm run gmaps:stats ──────────────────────────────────────────────────

if (process.argv.includes('--stats')) {
  const index = getIndex();
  const total = index.length;
  const withEmail = index.filter(r => cleanEmail(r.emails)).length;
  const withPhone = index.filter(r => r.phone?.trim()).length;
  const withWebsite = index.filter(r => r.website?.trim()).length;

  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📊 GMAPS CSV STATS');
  console.log(`  Total rows:   ${total}`);
  console.log(`  With website: ${withWebsite} (${Math.round(withWebsite / total * 100)}%)`);
  console.log(`  With phone:   ${withPhone} (${Math.round(withPhone / total * 100)}%)`);
  console.log(`  With email:   ${withEmail} (${Math.round(withEmail / total * 100)}%)`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
}
