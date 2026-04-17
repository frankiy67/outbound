/**
 * STEP 2 — Read and parse Google Maps scraper CSV output
 *
 * Input:  D:\outbound-data\gmaps-results.csv
 * Columns: input_id,title,website,phone,review_rating,review_count,emails,complete_address
 *
 * Matching strategy (in priority order):
 *  1. SIRET in input_id        → confidence 100
 *  2. Domain match             → confidence 95
 *  3. Smart token name match   → confidence 60-90
 *  4. Phone match              → confidence 85
 */

import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse/sync';
import { DATA_DIR } from './config';

export const GMAPS_CSV = path.join(DATA_DIR, 'gmaps-results.csv');

export type MatchMethod = 'siret' | 'domain' | 'name' | 'phone';

export interface GmapsMatch {
  siret: string;
  website: string | null;
  phone: string | null;
  rating: number | null;
  reviewCount: number | null;
  gmapsEmail: string | null;
  matchConfidence: number;
  matchMethod: MatchMethod;
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

// Order: longest first to avoid partial shadowing
const LEGAL_SUFFIXES =
  /\b(etablissements?|entreprises?|societes?|selarl|compagnies?|sasu|sarl|eurl|earl|gaec|sprl|scm|sci|snc|scp|ets|sas|eirl|sa|groupe|cie)\b/gi;

const FILLER_WORDS =
  /\b(et|les|des|de|du|la|le|au|aux|sur|sous)\b/gi;

function normalize(s: string): string {
  return s
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '') // é→e, à→a, ç→c, ô→o …
    .replace(LEGAL_SUFFIXES, '')
    .replace(FILLER_WORDS, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenize(s: string): string[] {
  return normalize(s)
    .split(' ')
    .filter(t => t.length > 0);
}

// ── Domain helpers ────────────────────────────────────────────────────────────

function extractDomain(url: string): string | null {
  if (!url?.trim()) return null;
  try {
    const withProto = /^https?:\/\//i.test(url) ? url : `https://${url}`;
    const hostname = new URL(withProto).hostname.toLowerCase();
    return hostname.replace(/^www\./, '');
  } catch {
    const m = url.match(
      /(?:https?:\/\/)?(?:www\.)?([a-z0-9][a-z0-9-]*\.[a-z]{2,}(?:\.[a-z]{2})?)/i,
    );
    return m ? m[1].toLowerCase() : null;
  }
}

/** Look for an embedded domain pattern inside a raw string (e.g. company name). */
function extractDomainFromText(text: string): string | null {
  const m = text.match(
    /(?:https?:\/\/)?(?:www\.)?([a-z0-9][a-z0-9-]*\.[a-z]{2,}(?:\.[a-z]{2})?)/i,
  );
  if (!m) return null;
  return m[1].toLowerCase().replace(/^www\./, '');
}

// ── Phone helpers ─────────────────────────────────────────────────────────────

/** Return last 9 digits to handle country-code variants (+33 vs 0…). */
function normalizePhone(phone: string): string | null {
  if (!phone?.trim()) return null;
  const digits = phone.replace(/\D/g, '');
  return digits.length >= 9 ? digits.slice(-9) : null;
}

// ── Smart token name match ────────────────────────────────────────────────────

function longestCommonSubstring(a: string, b: string): number {
  const la = a.length;
  const lb = b.length;
  let best = 0;
  const prev = new Array<number>(lb + 1).fill(0);
  const curr = new Array<number>(lb + 1).fill(0);

  for (let i = 1; i <= la; i++) {
    for (let j = 1; j <= lb; j++) {
      curr[j] = a[i - 1] === b[j - 1] ? prev[j - 1] + 1 : 0;
      if (curr[j] > best) best = curr[j];
    }
    curr.copyWithin(0, 0); // reuse – swap via index trick
    for (let k = 0; k <= lb; k++) { prev[k] = curr[k]; curr[k] = 0; }
  }
  return best;
}

/**
 * Returns confidence score 0–90 based on token overlap, then LCS fallback.
 */
function smartNameMatch(nameA: string, nameB: string): number {
  const tokA = tokenize(nameA);
  const tokB = tokenize(nameB);
  if (tokA.length === 0 || tokB.length === 0) return 0;

  const [shorter, longer] =
    tokA.length <= tokB.length ? [tokA, tokB] : [tokB, tokA];

  const matched = shorter.filter(t => longer.includes(t)).length;
  const ratio = matched / shorter.length;

  if (ratio === 1) return 90;   // ALL tokens of shorter found in longer
  if (ratio >= 0.75) return 75; // ≥75 % match

  // LCS fallback on normalized strings (spaces removed for robustness)
  const normA = normalize(nameA).replace(/\s/g, '');
  const normB = normalize(nameB).replace(/\s/g, '');
  const shorterNorm = normA.length <= normB.length ? normA : normB;
  const lcs = longestCommonSubstring(normA, normB);

  if (lcs > 6 && shorterNorm.length > 0 && lcs / shorterNorm.length >= 0.6) {
    return 60;
  }

  return 0;
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
  for (const email of raw.split(/[,;|\s]+/).map(e => e.trim().toLowerCase())) {
    if (!email.includes('@')) continue;
    if (JUNK_EMAIL_PATTERNS.some(re => re.test(email))) continue;
    if (!/^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$/.test(email)) continue;
    return email;
  }
  return null;
}

// ── CSV loading ───────────────────────────────────────────────────────────────

function loadCsv(): GmapsRow[] {
  if (!fs.existsSync(GMAPS_CSV)) return [];
  const content = fs.readFileSync(GMAPS_CSV, 'utf-8');
  return parse(content, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  }) as GmapsRow[];
}

function extractSiretFromInputId(inputId: string): string | null {
  const m = inputId.match(/#!#(\d{14})/);
  return m ? m[1] : null;
}

// ── Build lookup index ────────────────────────────────────────────────────────

interface IndexedRow extends GmapsRow {
  _siret: string | null;
  _normalizedTitle: string;
  _domain: string | null;
  _normalizedPhone: string | null;
}

let _index: IndexedRow[] | null = null;

function getIndex(): IndexedRow[] {
  if (_index) return _index;
  _index = loadCsv().map(r => ({
    ...r,
    _siret: extractSiretFromInputId(r.input_id),
    _normalizedTitle: normalize(r.title),
    _domain: extractDomain(r.website),
    _normalizedPhone: normalizePhone(r.phone),
  }));
  return _index;
}

// ── Match builder ─────────────────────────────────────────────────────────────

function buildMatch(
  siret: string,
  row: IndexedRow,
  confidence: number,
  method: MatchMethod,
): GmapsMatch {
  return {
    siret,
    website: row.website?.trim() || null,
    phone: row.phone?.trim() || null,
    rating: row.review_rating ? parseFloat(row.review_rating) || null : null,
    reviewCount: row.review_count ? parseInt(row.review_count, 10) || null : null,
    gmapsEmail: cleanEmail(row.emails),
    matchConfidence: confidence,
    matchMethod: method,
  };
}

// ── Public API ────────────────────────────────────────────────────────────────

export function findGmapsMatch(
  siret: string,
  companyName: string,
  postalCode: string,
  sireneDomain?: string | null,
  sirenePhone?: string | null,
): GmapsMatch | null {
  const index = getIndex();

  // Precompute lookup domain: prefer explicit SIRENE domain, fall back to
  // any URL-like pattern embedded in the company name itself.
  const lookupDomain =
    (sireneDomain && extractDomain(sireneDomain)) ||
    extractDomainFromText(companyName);

  const normSirenePhone = sirenePhone ? normalizePhone(sirenePhone) : null;

  let bestRow: IndexedRow | null = null;
  let bestConfidence = 0;
  let bestMethod: MatchMethod = 'name';

  for (const row of index) {
    // ── 1. SIRET match ──────────────────────────────────────────────────────
    if (row._siret && row._siret === siret) {
      return buildMatch(siret, row, 100, 'siret');
    }

    let rowConf = 0;
    let rowMethod: MatchMethod = 'name';

    // ── 2. Domain match (95) ────────────────────────────────────────────────
    if (lookupDomain && row._domain && lookupDomain === row._domain) {
      rowConf = 95;
      rowMethod = 'domain';
    }

    // ── 3. Smart name match (60-90) — skip if domain already matched ────────
    if (rowConf < 90) {
      const nameConf = smartNameMatch(companyName, row.title);
      if (nameConf > rowConf) {
        rowConf = nameConf;
        rowMethod = 'name';
      }
    }

    // ── 4. Phone match (85) — skip if we already have a better signal ───────
    if (rowConf < 85 && normSirenePhone && row._normalizedPhone) {
      if (normSirenePhone === row._normalizedPhone) {
        rowConf = 85;
        rowMethod = 'phone';
      }
    }

    if (rowConf > bestConfidence) {
      bestConfidence = rowConf;
      bestRow = row;
      bestMethod = rowMethod;
    }
  }

  if (!bestRow || bestConfidence === 0) return null;
  return buildMatch(siret, bestRow, bestConfidence, bestMethod);
}

export function gmapsCsvExists(): boolean {
  return fs.existsSync(GMAPS_CSV);
}

// ── Stats ─────────────────────────────────────────────────────────────────────

export function printGmapsStats(matches: (GmapsMatch | null)[]): void {
  const total = matches.length;
  const matched = matches.filter(Boolean).length;
  const pct = total > 0 ? Math.round((matched / total) * 100) : 0;

  const bySiret = matches.filter(m => m?.matchMethod === 'siret').length;
  const byDomain = matches.filter(m => m?.matchMethod === 'domain').length;
  const byName = matches.filter(m => m?.matchMethod === 'name').length;
  const byPhone = matches.filter(m => m?.matchMethod === 'phone').length;

  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📊 GMAPS READER STATS');
  console.log(`  Matched ${matched}/${total} companies (${pct}%)`);
  console.log(`  By SIRET: ${bySiret} | By domain: ${byDomain} | By name: ${byName} | By phone: ${byPhone}`);
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
  console.log(`  With website: ${withWebsite} (${Math.round((withWebsite / total) * 100)}%)`);
  console.log(`  With phone:   ${withPhone} (${Math.round((withPhone / total) * 100)}%)`);
  console.log(`  With email:   ${withEmail} (${Math.round((withEmail / total) * 100)}%)`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
}
