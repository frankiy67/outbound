/**
 * Artisan Web Redesign Pipeline
 *
 * Finds artisans (electricians, plumbers, painters…) via SIRENE + Google Maps
 * Places, scores their web presence for redesign opportunity, drafts outreach
 * emails in French.
 *
 * Usage:
 *   npm run artisans "Paris" [--limit 200] [--trade electricien] [--go]
 *   npm run artisans --status
 *   npm run artisans --reset
 */

import 'dotenv/config';
import * as path from 'path';
import * as fs from 'fs';
import * as nodemailer from 'nodemailer';
import { DatabaseSync } from 'node:sqlite';
import cron from 'node-cron';
import fetch from 'node-fetch';
import pino from 'pino';
import { sourceCompanies } from './sourcing';
import { scoreRestaurantWebsite } from './website-scorer';
import {
  findContactForDomainDnsOnly,
  randomUA,
  type ContactResult,
} from './contacts';
import { DATA_DIR } from './config';

const logger = pino({ level: 'info' });

const ARTISAN_QUEUE_DB = path.join(DATA_DIR, 'artisan_queue.db');
const ARTISAN_PREVIEW  = path.join(DATA_DIR, 'artisan_preview.json');
const SIRENE_DB_PATH   = path.join(DATA_DIR, 'sirene.db');

// ── NAF codes ─────────────────────────────────────────────────────────────────

const NAF_LABEL: Record<string, string> = {
  '43.21A': 'Électricien',
  '43.22A': 'Plombier',
  '43.22B': 'Chauffagiste',
  '43.31Z': 'Plâtrier',
  '43.32A': 'Menuisier',
  '43.32B': 'Menuisier métal',
  '43.33Z': 'Carreleur',
  '43.34Z': 'Peintre',
  '43.39Z': 'Finition',
  '43.91A': 'Charpentier',
  '43.99B': 'Maçon',
};

const ALL_ARTISAN_NAF = Object.keys(NAF_LABEL);

// --trade CLI value → NAF codes to query
const TRADE_NAF: Record<string, string[]> = {
  electricien:  ['43.21A'],
  plombier:     ['43.22A'],
  chauffagiste: ['43.22B'],
  platrier:     ['43.31Z'],
  menuisier:    ['43.32A', '43.32B'],
  carreleur:    ['43.33Z'],
  peintre:      ['43.34Z'],
  macon:        ['43.99B'],
};

// ── Types ─────────────────────────────────────────────────────────────────────

interface ArtisanRecord {
  siren: string;
  company_name: string;
  city: string;
  postal_code: string;
  naf_code: string;
  headcount_tranche: string;
  trade_label: string;
  // Places API enrichment
  place_id: string | null;
  website: string | null;
  phone: string | null;
  rating: number | null;
  user_ratings_total: number | null;
  // Website scoring
  opportunity_score: number;
  opportunity_tier: 'hot' | 'warm' | 'cold';
  signals: string[];
  // Contact finding
  contact_name: string | null;
  email: string | null;
  email_status: ContactResult['email_status'];
  email_confidence: number;
}

// ── CLI parsing ───────────────────────────────────────────────────────────────

interface CliArgs {
  city: string | null;
  limit: number | null;
  trade: string | null;
  naf_codes: string[];
  go: boolean;
  reset: boolean;
  status: boolean;
  noWebsiteOnly: boolean;
  count: boolean;
}

function parseCli(args: string[]): CliArgs {
  // npm 9 on Windows treats unknown --flags as boolean config, stripping them
  // from argv and (for value flags) leaving the value as a bare positional arg.
  // We detect this via npm_config_* env vars and recover the value from argv.
  const npmEnv = (key: string): string | undefined =>
    process.env[`npm_config_${key.replace(/-/g, '_')}`];

  // ── Boolean flags ─────────────────────────────────────────────────────────
  const go            = args.includes('--go')             || npmEnv('go')             !== undefined;
  const reset         = args.includes('--reset')           || npmEnv('reset')           !== undefined;
  const status        = args.includes('--status')          || npmEnv('status')          !== undefined;
  const noWebsiteOnly = args.includes('--no-website-only') || npmEnv('no-website-only') !== undefined;
  const count         = args.includes('--count')           || npmEnv('count')           !== undefined;

  // ── --trade [value] ───────────────────────────────────────────────────────
  const tradeIdx = args.findIndex(a => a === '--trade');
  let tradeRaw: string | null = tradeIdx !== -1 && args[tradeIdx + 1]
    ? args[tradeIdx + 1].toLowerCase()
    : null;

  // npm consumed --trade: its value was pushed back as a bare positional.
  // Identify it by checking whether all comma-parts are valid trade keys.
  if (tradeRaw === null && npmEnv('trade') !== undefined) {
    const allKeys = Object.keys(TRADE_NAF);
    const found = args.find(a => {
      if (a.startsWith('--')) return false;
      const parts = a.toLowerCase().split(',').map(t => t.trim()).filter(Boolean);
      return parts.length > 0 && parts.every(p => allKeys.includes(p));
    });
    if (found) tradeRaw = found.toLowerCase();
  }

  const tradeKeys = tradeRaw ? tradeRaw.split(',').map(t => t.trim()).filter(Boolean) : [];

  for (const key of tradeKeys) {
    if (!TRADE_NAF[key]) {
      console.error(`Unknown trade "${key}". Valid values: ${Object.keys(TRADE_NAF).join(', ')}`);
      process.exit(1);
    }
  }

  // Display as "electricien, plombier" rather than the raw "electricien,plombier".
  const trade     = tradeKeys.length > 0 ? tradeKeys.join(', ') : null;
  const naf_codes = tradeKeys.length > 0
    ? [...new Set(tradeKeys.flatMap(k => TRADE_NAF[k]))]
    : ALL_ARTISAN_NAF;

  // ── --limit [value] ───────────────────────────────────────────────────────
  const limitIdx = args.findIndex(a => a === '--limit');
  let limit: number | null = limitIdx !== -1 && args[limitIdx + 1]
    ? parseInt(args[limitIdx + 1], 10)
    : null;

  // npm consumed --limit: its value landed as a bare numeric positional.
  if (limit === null && npmEnv('limit') !== undefined) {
    const numArg = args.find(a => /^\d+$/.test(a));
    if (numArg) limit = parseInt(numArg, 10);
  }

  // ── City: first positional arg that is not a flag, number, or trade value ──
  // "France" / "all" / absent → nationwide (no city filter).
  const rawCity = args.find(a => {
    if (a.startsWith('--')) return false;
    if (/^\d+$/.test(a)) return false;
    if (tradeRaw && a.toLowerCase() === tradeRaw) return false; // bare trade value injected by npm
    return true;
  }) ?? null;

  const city = (
    rawCity === null ||
    rawCity.toLowerCase() === 'france' ||
    rawCity.toLowerCase() === 'all'
  ) ? null : rawCity;

  return { city, limit, trade, naf_codes, go, reset, status, noWebsiteOnly, count };
}

// ── False-positive filters ────────────────────────────────────────────────────

// Large construction groups — they won't need a freelance redesign.
const CHAIN_NAMES = [
  'VINCI',
  'BOUYGUES',
  'EIFFAGE',
  'SPIE ',      // trailing space avoids matching e.g. "SPIELBERG"
  'GTM ',
  'COLAS',
  'SOGEA',
  'FAYAT',
  'LEON GROSSE',
  'ENGIE',
  'TOTAL',
  'ELECTRICITE DE FRANCE',
  'EDF ',
  'VEOLIA',
  'SCHNEIDER',
  'DALKIA',
  'IDEX ',
  'COFELY',
  'INEO',
  'GRDF',
];

function isChain(name: string): boolean {
  const upper = name.toUpperCase();
  return CHAIN_NAMES.some(chain => upper === chain || upper.startsWith(chain));
}

const LEGAL_PREFIX_RE = /^(SARL|SAS|SASU|EURL|SA|SNC|SCI|SELARL|SC|GIE|ASSOCIATION|ASSO)\s+/;

// "SAS GDM" or "EURL XB" — legal wrapper around initials, no real business identity.
function isGenericLegalName(name: string): boolean {
  const stripped = name.toUpperCase().replace(LEGAL_PREFIX_RE, '').trim();
  if (!stripped) return true;            // nothing after the legal form
  if (stripped.length <= 3) return true; // e.g. "SAS DG"
  // All consonants → abbreviation (e.g. "SARL BRGR", "EURL GTP")
  if (!/[AEIOUÉÈÊËÀÂÙÛÜÎÏÔŒ]/i.test(stripped)) return true;
  return false;
}

// 0 reviews + no website = unclaimed or permanently closed Maps listing.
function isGhostListing(rec: ArtisanRecord): boolean {
  return rec.user_ratings_total === 0 && rec.website === null;
}

// ── Module 2: Google Maps Places enrichment ───────────────────────────────────

interface PlaceSearchResult {
  place_id: string;
  name: string;
  formatted_address: string;
  rating?: number;
  user_ratings_total?: number;
}

async function placesTextSearch(
  apiKey: string,
  query: string,
): Promise<PlaceSearchResult | null> {
  // No type filter — artisans are not categorised as restaurants/cafes.
  const url =
    `https://maps.googleapis.com/maps/api/place/textsearch/json` +
    `?query=${encodeURIComponent(query)}&key=${apiKey}`;
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const data = await res.json() as { status: string; results?: PlaceSearchResult[] };
    if (data.status !== 'OK' || !data.results?.length) return null;
    return data.results[0];
  } catch {
    return null;
  }
}

async function placesDetails(
  apiKey: string,
  placeId: string,
): Promise<{ website: string | null; phone: string | null }> {
  const url =
    `https://maps.googleapis.com/maps/api/place/details/json` +
    `?place_id=${placeId}&fields=website,formatted_phone_number&key=${apiKey}`;
  try {
    const res = await fetch(url);
    if (!res.ok) return { website: null, phone: null };
    const data = await res.json() as {
      status: string;
      result?: { website?: string; formatted_phone_number?: string };
    };
    if (data.status !== 'OK' || !data.result) return { website: null, phone: null };
    return {
      website: data.result.website ?? null,
      phone:   data.result.formatted_phone_number ?? null,
    };
  } catch {
    return { website: null, phone: null };
  }
}

async function enrichArtisanWithPlaces(
  apiKey: string,
  companyName: string,
  tradeLabel: string,
  city: string,
): Promise<{
  place_id: string | null;
  website: string | null;
  phone: string | null;
  rating: number | null;
  user_ratings_total: number | null;
}> {
  // Query format: "DUPONT ELECTRICITE Électricien Paris"
  const query = `${companyName} ${tradeLabel} ${city}`;
  const hit = await placesTextSearch(apiKey, query);
  if (!hit) {
    return { place_id: null, website: null, phone: null, rating: null, user_ratings_total: null };
  }
  const details = await placesDetails(apiKey, hit.place_id);
  return {
    place_id:           hit.place_id,
    website:            details.website,
    phone:              details.phone,
    rating:             hit.rating             ?? null,
    user_ratings_total: hit.user_ratings_total ?? null,
  };
}

// ── Module 3b: Contact finding ────────────────────────────────────────────────

// Domains that are social platforms or third-party listing pages — no owned
// email can be found by scraping or DNS, so we skip and rely on phone only.
const SOCIAL_OR_PLATFORM_DOMAINS = [
  'instagram.com',
  'facebook.com',
  'twitter.com',
  'linkedin.com',
  'youtube.com',
  'tiktok.com',
  'google.com',
  'maps.google.com',
  'privateaser.com',
  'menuonline.fr',
  'wiicmenu-qrcode.com',
  'eatbu.com',
  'lafourchette.com',
  'tripadvisor.com',
  'pages.fr',
  'wixsite.com',
];

function isSocialOrPlatformUrl(url: string): boolean {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return SOCIAL_OR_PLATFORM_DOMAINS.some(d => host === d || host.endsWith(`.${d}`));
  } catch {
    return false;
  }
}

// Matches explicit mailto: href values (most reliable).
const MAILTO_HREF_RE = /href=["']mailto:([^"'?\s]+)/gi;
// General email pattern for full-text scan.
const EMAIL_TEXT_RE  = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;
// Local-part substrings that indicate system/noise addresses.
const EMAIL_NOISE    = [
  'noreply', 'no-reply', 'webmaster', 'wordpress',
  'spam', 'example', 'test', 'privacy', 'dpo', 'rgpd',
];

/** Extract and deduplicate real email addresses from HTML. */
function extractEmailsFromHtml(html: string): string[] {
  const seen    = new Set<string>();
  const results: string[] = [];

  const addIfClean = (raw: string) => {
    const e = raw.toLowerCase().trim();
    if (!e.includes('@') || seen.has(e)) return;
    const local = e.split('@')[0];
    if (EMAIL_NOISE.some(n => local.includes(n))) return;
    seen.add(e);
    results.push(e);
  };

  // Priority 1 — explicit mailto: links (precise, avoids obfuscated junk).
  for (const m of html.matchAll(MAILTO_HREF_RE)) addIfClean(m[1]);

  // Priority 2 — full text scan (catches plain-text addresses on contact pages).
  for (const m of (html.match(EMAIL_TEXT_RE) ?? [])) addIfClean(m);

  return results;
}

/**
 * Scrapes an artisan website for a contact email address.
 * Tries homepage (5 s timeout) then /contact and /nous-contacter (3 s each).
 * Prefers an address whose domain matches the site's own domain (avoids
 * third-party builder addresses like support@webador.fr appearing first).
 * Returns the best address found, or null.
 */
async function scrapeEmailFromWebsite(websiteUrl: string): Promise<string | null> {
  const base       = websiteUrl.replace(/\/$/, '');
  const siteDomain = new URL(websiteUrl).hostname.replace(/^www\./, '');
  const ua         = randomUA();
  const hdrs       = { 'User-Agent': ua, 'Accept-Language': 'fr-FR,fr;q=0.9' };

  // Fetch one page and return all clean emails found on it.
  const fetchEmails = async (url: string, timeoutMs: number): Promise<string[]> => {
    try {
      const res = await fetch(url, { headers: hdrs, timeout: timeoutMs } as Parameters<typeof fetch>[1]);
      if (!res.ok) return [];
      return extractEmailsFromHtml(await res.text());
    } catch { return []; }
  };

  // Among a set of candidates, prefer one whose domain matches the site.
  const pickBest = (emails: string[]): string | null => {
    if (emails.length === 0) return null;
    const ownDomain = emails.find(e => e.split('@')[1] === siteDomain);
    return ownDomain ?? emails[0];
  };

  // Homepage first (5 s), then contact pages (3 s each).
  for (const [url, ms] of [
    [base,                     5000],
    [`${base}/contact`,        3000],
    [`${base}/nous-contacter`, 3000],
  ] as [string, number][]) {
    const emails = await fetchEmails(url, ms);
    const best   = pickBest(emails);
    if (best) {
      logger.debug({ url, email: best }, 'Email found via scraping');
      return best;
    }
  }

  return null;
}

/**
 * Finds a contact email for an artisan with a known website.
 * Primary:  scrape the site for a real mailto / text email.
 * Fallback: DNS confidence scoring (no TCP connections).
 */
async function findArtisanContact(
  siren: string,
  companyName: string,
  city: string,
  domain: string,
  websiteUrl: string,
): Promise<{
  contact_name: string | null;
  email: string | null;
  email_status: ContactResult['email_status'];
  email_confidence: number;
}> {
  // Step 1: scrape the website.
  const scraped = await scrapeEmailFromWebsite(websiteUrl);
  if (scraped) {
    return { contact_name: null, email: scraped, email_status: 'unverified', email_confidence: 60 };
  }

  // Step 2: DNS confidence fallback.
  const result       = await findContactForDomainDnsOnly(siren, companyName, city, domain);
  const contact_name = result.first_name && result.last_name
    ? `${result.first_name} ${result.last_name}`
    : null;
  return {
    contact_name,
    email:            result.email,
    email_status:     result.email_status,
    email_confidence: result.confidence,
  };
}

// ── Module 4: Email drafting ──────────────────────────────────────────────────

const SEQUENCE_STEPS: Record<'hot' | 'warm' | 'cold', number[]> = {
  hot:  [0, 3, 7],
  warm: [0, 5],
  cold: [0],
};

function selectAngle(signals: string[]): string {
  if (signals.includes('no_website'))  return 'no_website';
  if (signals.includes('no_ssl'))      return 'no_ssl';
  if (signals.includes('not_mobile'))  return 'not_mobile';
  if (signals.includes('no_booking'))  return 'no_devis';
  if (signals.includes('slow_site'))   return 'slow_site';
  if (signals.includes('old_builder')) return 'old_builder';
  return 'generic';
}

function draftEmail(
  rec: ArtisanRecord,
  step: number,
): { subject: string; body: string } {
  const angle   = selectAngle(rec.signals);
  const company = rec.company_name;
  const city    = rec.city;
  const trade   = rec.trade_label.toLowerCase();

  type TemplateFn = (s: number) => { subject: string; body: string };

  const templates: Record<string, TemplateFn> = {

    no_website: (s) => ({
      subject: s === 0
        ? `${company} — introuvable en ligne`
        : s === 1
        ? `suite — ${company}`
        : `dernière relance — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nJ'ai cherché votre site avant de vous écrire — vos clients aussi le font avant d'appeler.\n\nAujourd'hui un ${trade} sans site perd des demandes de devis à des concurrents qui en ont un, même moins bons.\n\nOn crée des sites pensés pour les artisans : rapides, avec un formulaire devis intégré.\n\nEst-ce que c'est quelque chose qui vous manque ?\n\nPaul`
        : s === 1
        ? `Bonjour,\n\nJe reviens sur mon message de la semaine dernière.\n\nUne seule question : l'absence de site est-elle un choix délibéré, ou quelque chose que vous n'avez pas encore eu le temps de traiter ?\n\nOn peut avoir quelque chose en ligne en 2 semaines.\n\nPaul`
        : `Bonjour,\n\nDernier message de ma part.\n\nSi le timing n'est pas bon pour l'instant, aucun problème. Bonne continuation à ${company}.\n\nPaul`,
    }),

    no_ssl: (s) => ({
      subject: s === 0
        ? `${company} — site non sécurisé`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nLe site de ${company} n'est pas sécurisé — les navigateurs affichent un avertissement avant même que les clients vous contactent.\n\nPour un ${trade} qui propose des devis en ligne, ça freine directement les demandes.\n\nUn certificat SSL + une page devis propre, c'est ce qu'on fait.\n\nÇa vous pose problème en ce moment ?\n\nPaul`
        : `Bonjour,\n\nPas de retour à mon message précédent — peut-être que le timing n'était pas idéal.\n\nL'offre reste valable si le site de ${company} a besoin d'être sécurisé.\n\nPaul`,
    }),

    not_mobile: (s) => ({
      subject: s === 0
        ? `${company} — votre site ne passe pas sur mobile`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\n80 % des recherches de ${trade} se font sur smartphone.\n\nJ'ai regardé le site de ${company} — il ne s'affiche pas correctement sur mobile. Résultat : les visiteurs partent sans appeler.\n\nOn peut corriger ça rapidement, sans tout refaire.\n\nC'est un problème que vous avez déjà remarqué ?\n\nPaul`
        : `Bonjour,\n\nPas de réponse à mon message précédent — peut-être que le moment n'était pas le bon.\n\nL'offre tient toujours si vous voulez améliorer l'expérience mobile de ${company}.\n\nPaul`,
    }),

    no_devis: (s) => ({
      subject: s === 0
        ? `${company} — devis en ligne = moins de téléphone`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nJ'ai regardé le site de ${company} : il n'y a pas de formulaire de devis en ligne.\n\nUn formulaire bien placé capte les demandes à 23h quand les clients planifient leurs travaux — sans que vous décrochiez le téléphone.\n\nOn intègre ça simplement dans un site artisan.\n\nC'est quelque chose qui manque chez vous ?\n\nPaul`
        : `Bonjour,\n\nJuste une relance — est-ce que la prise de devis en ligne est un sujet pour ${company} en ce moment ?\n\nPaul`,
    }),

    slow_site: (s) => ({
      subject: s === 0
        ? `${company} — 3 secondes et le client appelle ailleurs`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nJ'ai chargé le site de ${company} — il met plus de 3 secondes à s'afficher.\n\nC'est le seuil au-delà duquel la majorité des visiteurs ferment l'onglet et appellent le concurrent.\n\nOn optimise ça en quelques jours.\n\nVous avez remarqué un taux de rebond élevé ?\n\nPaul`
        : `Bonjour,\n\nPas de retour — peut-être que le timing n'était pas le bon.\n\nL'offre reste valable si la vitesse du site de ${company} est encore un sujet.\n\nPaul`,
    }),

    old_builder: (s) => ({
      subject: s === 0
        ? `${company} — Wix limite votre visibilité locale`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nLe site de ${company} est sur un template Wix — ce qui limite votre référencement local et les options de formulaire devis.\n\nLes clients qui cherchent "${trade} ${city}" vous trouvent moins facilement que vos concurrents sur un vrai site.\n\nOn fait des migrations simples, sans perdre votre contenu.\n\nC'est quelque chose qui vous préoccupe ?\n\nPaul`
        : `Bonjour,\n\nPas de retour — peut-être que le timing n'était pas idéal.\n\nToujours disponible si vous souhaitez migrer le site de ${company}.\n\nPaul`,
    }),

    generic: (s) => ({
      subject: s === 0 ? `idée pour ${company}` : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nQuelques améliorations à votre présence en ligne pourraient vous apporter plus de demandes de devis à ${city}.\n\nOn travaille avec des artisans sur ce sujet — site, formulaire devis, visibilité Google.\n\nEst-ce que ça vous parle ?\n\nPaul`
        : `Bonjour,\n\nPas de retour — je suppose que le timing n'était pas idéal. Toujours disponible si le sujet revient.\n\nPaul`,
    }),
  };

  return (templates[angle] ?? templates['generic'])(step);
}

// ── Module 5: SQLite queue ────────────────────────────────────────────────────

function openQueueDb(): DatabaseSync {
  const db = new DatabaseSync(ARTISAN_QUEUE_DB);
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;

    CREATE TABLE IF NOT EXISTS artisan_queue (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      siren          TEXT NOT NULL,
      lead_email     TEXT,
      lead_company   TEXT NOT NULL,
      lead_city      TEXT NOT NULL,
      trade_label    TEXT NOT NULL,
      subject        TEXT NOT NULL,
      body           TEXT NOT NULL,
      step           INTEGER NOT NULL DEFAULT 0,
      scheduled_date TEXT NOT NULL,
      status         TEXT NOT NULL DEFAULT 'pending',
      created_at     TEXT NOT NULL,
      sent_at        TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_artisan_siren_step
      ON artisan_queue(siren, step);

    CREATE TABLE IF NOT EXISTS contacted (
      siren        TEXT PRIMARY KEY,
      company_name TEXT NOT NULL,
      contacted_at TEXT NOT NULL,
      status       TEXT NOT NULL DEFAULT 'sent'
    );
  `);
  return db;
}

function getContactedSirens(db: DatabaseSync): Set<string> {
  const rows = db.prepare('SELECT siren FROM contacted').all() as Array<{ siren: string }>;
  return new Set(rows.map(r => r.siren));
}

function enqueueArtisans(artisans: ArtisanRecord[]): void {
  const db        = openQueueDb();
  const contacted = getContactedSirens(db);

  const insertQueue = db.prepare(`
    INSERT OR IGNORE INTO artisan_queue
      (siren, lead_email, lead_company, lead_city, trade_label, subject, body,
       step, scheduled_date, status, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
  `);
  const markContacted = db.prepare(`
    INSERT OR IGNORE INTO contacted (siren, company_name, contacted_at, status)
    VALUES (?, ?, ?, 'sent')
  `);

  const beginTx  = db.prepare('BEGIN');
  const commitTx = db.prepare('COMMIT');

  let queued  = 0;
  let skipped = 0;

  beginTx.run();
  for (const rec of artisans) {
    if (contacted.has(rec.siren)) { skipped++; continue; }

    const steps = SEQUENCE_STEPS[rec.opportunity_tier];
    const today = new Date();

    for (let i = 0; i < steps.length; i++) {
      const scheduledDate = new Date(today);
      scheduledDate.setDate(today.getDate() + steps[i]);
      const draft = draftEmail(rec, i);

      insertQueue.run(
        rec.siren,
        rec.email ?? null,
        rec.company_name,
        rec.city,
        rec.trade_label,
        draft.subject,
        draft.body,
        i,
        scheduledDate.toISOString().split('T')[0],
        new Date().toISOString(),
      );
    }

    markContacted.run(rec.siren, rec.company_name, new Date().toISOString());
    queued++;
  }
  commitTx.run();
  db.close();

  logger.info({ queued, skipped }, 'Artisans enqueued (skipped = already contacted)');
}

// ── Status ────────────────────────────────────────────────────────────────────

function printStatus(): void {
  if (!fs.existsSync(ARTISAN_QUEUE_DB)) {
    console.log('No queue database found. Run the pipeline first.');
    return;
  }

  const db      = openQueueDb();
  const total   = (db.prepare('SELECT COUNT(*) AS n FROM artisan_queue').get() as { n: number }).n;
  const pending = (db.prepare("SELECT COUNT(*) AS n FROM artisan_queue WHERE status='pending'").get() as { n: number }).n;
  const sent    = (db.prepare("SELECT COUNT(*) AS n FROM artisan_queue WHERE status='sent'").get() as { n: number }).n;
  const failed  = (db.prepare("SELECT COUNT(*) AS n FROM artisan_queue WHERE status='failed'").get() as { n: number }).n;

  const contactedRows = db.prepare(
    "SELECT status, COUNT(*) AS n FROM contacted GROUP BY status",
  ).all() as Array<{ status: string; n: number }>;
  db.close();

  const cs: Record<string, number> = {};
  for (const r of contactedRows) cs[r.status] = r.n;

  console.log('\n════════════════════════════════════════════');
  console.log('  ARTISAN PIPELINE — STATUS');
  console.log('════════════════════════════════════════════');
  console.log(`  Emails in queue      : ${total}`);
  console.log(`  Pending              : ${pending}`);
  console.log(`  Sent                 : ${sent}`);
  console.log(`  Failed               : ${failed}`);
  console.log(`  Artisans contacted   : ${(cs['sent'] ?? 0) + (cs['preview'] ?? 0)}`);
  console.log(`    → sent             : ${cs['sent']    ?? 0}`);
  console.log(`    → bounced          : ${cs['bounced'] ?? 0}`);
  console.log(`    → replied          : ${cs['replied'] ?? 0}`);
  console.log('════════════════════════════════════════════\n');
}

// ── Reset ─────────────────────────────────────────────────────────────────────

function resetQueues(): void {
  if (!fs.existsSync(ARTISAN_QUEUE_DB)) {
    console.log('Nothing to reset — queue database does not exist.');
    return;
  }
  const db = openQueueDb();
  db.exec('DELETE FROM artisan_queue; DELETE FROM contacted;');
  db.close();
  console.log('Queue and contacted table cleared.');
}

// ── Gmail sender ──────────────────────────────────────────────────────────────

function createTransporter(): nodemailer.Transporter {
  const user = process.env['GMAIL_USER'];
  const pass = process.env['GMAIL_APP_PASSWORD'];
  if (!user || !pass) throw new Error('GMAIL_USER and GMAIL_APP_PASSWORD must be set in .env');
  return nodemailer.createTransport({ service: 'gmail', auth: { user, pass } });
}

async function sendPendingEmails(): Promise<void> {
  const db    = openQueueDb();
  const today = new Date().toISOString().split('T')[0];

  const pending = db.prepare(`
    SELECT * FROM artisan_queue
    WHERE scheduled_date <= ? AND status = 'pending' AND lead_email IS NOT NULL
    ORDER BY trade_label, scheduled_date ASC
  `).all(today) as unknown as Array<{
    id: number;
    lead_email: string;
    lead_company: string;
    trade_label: string;
    subject: string;
    body: string;
    step: number;
  }>;

  if (pending.length === 0) {
    logger.info('No artisan emails to send today');
    db.close();
    return;
  }

  const transporter = createTransporter();
  const update = db.prepare(
    `UPDATE artisan_queue SET status = ?, sent_at = ? WHERE id = ?`,
  );

  for (const entry of pending) {
    try {
      await transporter.sendMail({
        from:    process.env['GMAIL_USER'],
        to:      entry.lead_email,
        subject: entry.subject,
        text:    entry.body,
      });
      update.run('sent', new Date().toISOString(), entry.id);
      logger.info(
        { to: entry.lead_email, company: entry.lead_company, trade: entry.trade_label, step: entry.step },
        'Email sent',
      );
    } catch (err) {
      update.run('failed', new Date().toISOString(), entry.id);
      logger.error({ err, to: entry.lead_email }, 'Email send failed');
    }
    // Respect Gmail rate limits.
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

  db.close();
}

function startScheduler(): void {
  logger.info('Artisan scheduler started — runs daily at 08:00');
  cron.schedule('0 8 * * *', async () => {
    logger.info('Running scheduled artisan email send...');
    await sendPendingEmails();
  });
}

// ── Concurrency helper ────────────────────────────────────────────────────────

async function runWithConcurrency<T>(
  tasks: Array<() => Promise<T>>,
  limit: number,
): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let next = 0;
  async function worker(): Promise<void> {
    while (next < tasks.length) {
      const i = next++;
      results[i] = await tasks[i]();
    }
  }
  await Promise.allSettled(
    Array.from({ length: Math.min(limit, tasks.length) }, () => worker()),
  );
  return results;
}

// ── Preview ───────────────────────────────────────────────────────────────────

function writePreview(artisans: ArtisanRecord[]): void {
  const preview = artisans.map(rec => ({
    company:          rec.company_name,
    trade:            rec.trade_label,
    city:             rec.city,
    website:          rec.website,
    phone:            rec.phone,
    rating:           rec.rating,
    reviews:          rec.user_ratings_total,
    score:            rec.opportunity_score,
    tier:             rec.opportunity_tier,
    signals:          rec.signals,
    contact_name:     rec.contact_name,
    email:            rec.email,
    email_status:     rec.email_status,
    email_confidence: rec.email_confidence,
    draft_emails:     SEQUENCE_STEPS[rec.opportunity_tier].map((_, i) => draftEmail(rec, i)),
  }));

  fs.writeFileSync(ARTISAN_PREVIEW, JSON.stringify(preview, null, 2));
  logger.info({ path: ARTISAN_PREVIEW, count: preview.length }, 'Artisan preview written');
}

// ── Summary table ─────────────────────────────────────────────────────────────

function printSummaryTable(artisans: ArtisanRecord[]): void {
  const separator = '─'.repeat(120);
  console.log('\n' + '═'.repeat(120));
  console.log('  ARTISAN PIPELINE — RESULTS');
  console.log('═'.repeat(120));
  console.log(
    `${'Company'.padEnd(32)} ${'Trade'.padEnd(14)} ${'Website'.padEnd(26)} ${'⭐'.padEnd(5)} ${'Score'.padEnd(6)} ${'Tier'.padEnd(6)} Signals`,
  );
  console.log(separator);

  for (const rec of artisans) {
    const name   = (rec.company_name ?? 'Unknown').substring(0, 31).padEnd(32);
    const trade  = rec.trade_label.substring(0, 13).padEnd(14);
    const web    = (rec.website ?? '—').substring(0, 25).padEnd(26);
    const rating = rec.rating !== null ? String(rec.rating).padEnd(5) : '—    ';
    const sigs   = rec.signals.join(', ');
    console.log(
      `${name} ${trade} ${web} ${rating} ${String(rec.opportunity_score).padEnd(6)} ${rec.opportunity_tier.padEnd(6)} ${sigs}`,
    );
  }

  const hot  = artisans.filter(r => r.opportunity_tier === 'hot').length;
  const warm = artisans.filter(r => r.opportunity_tier === 'warm').length;
  const cold = artisans.filter(r => r.opportunity_tier === 'cold').length;

  console.log(separator);
  console.log(`Total: ${artisans.length}  |  Hot: ${hot}  |  Warm: ${warm}  |  Cold: ${cold}`);
  console.log('═'.repeat(120) + '\n');
}

// ── Count mode ────────────────────────────────────────────────────────────────

function runCount(cli: CliArgs): void {
  if (!fs.existsSync(SIRENE_DB_PATH)) {
    console.error(`SIRENE database not found at ${SIRENE_DB_PATH}.`);
    console.error('Run the full pipeline once to build it, then use --count.');
    process.exit(1);
  }

  const db       = new DatabaseSync(SIRENE_DB_PATH);
  const cityNorm = cli.city ? cli.city.toUpperCase() : null;
  const cityLike = cityNorm ? `%${cityNorm}%` : null;
  const cityClause = cityLike ? 'AND UPPER(libelleCommuneEtablissement) LIKE ?' : '';
  const cityLabel  = cli.city ?? 'France';

  if (cli.trade) {
    // Single-trade count — exact same WHERE clause as the pipeline (no LIMIT).
    const placeholders = cli.naf_codes.map(() => '?').join(', ');
    const row = db.prepare(`
      SELECT COUNT(*) AS n FROM etablissements
      WHERE activitePrincipaleEtablissement IN (${placeholders})
        ${cityClause}
    `).get(...cli.naf_codes, ...(cityLike ? [cityLike] : [])) as { n: number };
    db.close();

    const label = cli.naf_codes.map(n => `${NAF_LABEL[n] ?? n} (${n})`).join(', ');
    console.log(`\n${label}: ${row.n} in ${cityLabel}\n`);
  } else {
    // Full breakdown — one COUNT per NAF code, then total.
    console.log(`\nArtisan count in ${cityLabel}:\n`);
    let total = 0;

    for (const [naf, label] of Object.entries(NAF_LABEL)) {
      const row = db.prepare(`
        SELECT COUNT(*) AS n FROM etablissements
        WHERE activitePrincipaleEtablissement = ?
          ${cityClause}
      `).get(naf, ...(cityLike ? [cityLike] : [])) as { n: number };

      console.log(`  ${label.padEnd(18)} (${naf}): ${row.n}`);
      total += row.n;
    }

    db.close();
    console.log(`\n  Total: ${total}\n`);
  }
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const argv = process.argv.slice(2);

  // ── --count: query SIRENE only, log counts, exit immediately ─────────────
  // Checked before parseCli and before any async work so nothing else runs.
  const npmEnvMain = (key: string): string | undefined =>
    process.env[`npm_config_${key.replace(/-/g, '_')}`];

  if (process.argv.includes('--count') || npmEnvMain('count') !== undefined) {
    const tradeIdx = argv.findIndex(a => a === '--trade');
    let tradeRaw: string | null = tradeIdx !== -1 && argv[tradeIdx + 1]
      ? argv[tradeIdx + 1].toLowerCase()
      : null;
    if (tradeRaw === null && npmEnvMain('trade') !== undefined) {
      const allKeys = Object.keys(TRADE_NAF);
      const found = argv.find(a => {
        if (a.startsWith('--')) return false;
        const parts = a.toLowerCase().split(',').map(t => t.trim()).filter(Boolean);
        return parts.length > 0 && parts.every(p => allKeys.includes(p));
      });
      if (found) tradeRaw = found.toLowerCase();
    }
    const tradeKeys = tradeRaw ? tradeRaw.split(',').map(t => t.trim()).filter(Boolean) : [];
    const trade     = tradeKeys.length > 0 ? tradeKeys.join(', ') : null;
    const naf_codes = tradeKeys.length > 0
      ? [...new Set(tradeKeys.flatMap(k => TRADE_NAF[k] ?? []))]
      : ALL_ARTISAN_NAF;
    const rawCity2 = argv.find(a => {
      if (a.startsWith('--')) return false;
      if (/^\d+$/.test(a)) return false;
      if (tradeRaw && a.toLowerCase() === tradeRaw) return false;
      return true;
    }) ?? null;
    const city = (
      rawCity2 === null ||
      rawCity2.toLowerCase() === 'france' ||
      rawCity2.toLowerCase() === 'all'
    ) ? null : rawCity2;

    runCount({
      city, trade, naf_codes, count: true,
      limit: null, go: false, reset: false, status: false, noWebsiteOnly: false,
    });
    process.exit(0);
  }

  // ── Normal pipeline ───────────────────────────────────────────────────────
  const args = argv;

  if (args.includes('--status') || npmEnvMain('status') !== undefined) { printStatus(); return; }
  if (args.includes('--reset')  || npmEnvMain('reset')  !== undefined) { resetQueues(); return; }

  const cli = parseCli(args);

  // ── Test modes (exit early, no pipeline) ────────────────────────────────
  if (args.includes('--test-parse') || npmEnvMain('test-parse') !== undefined) {
    console.log('\n=== CLI Parse Test ===');
    console.log(`Trade:     ${cli.trade ?? '(all trades)'}`);
    console.log(`NAF codes: ${cli.naf_codes.join(', ')}`);
    console.log(`City:      ${cli.city ?? '(all France)'}`);
    console.log(`Limit:     ${cli.limit ?? 'none'}`);
    process.exit(0);
  }

  if (args.includes('--test-email') || npmEnvMain('test-email') !== undefined) {
    const testUrl = 'http://www.etsgilles.fr/';
    console.log(`\n=== Email Scrape Test: ${testUrl} ===`);
    const email = await scrapeEmailFromWebsite(testUrl);
    console.log(`Result: ${email ?? '(no email found)'}`);
    process.exit(0);
  }

  const tradeDisplay = cli.trade ? `${cli.trade} (${cli.naf_codes.join(', ')})` : 'all trades';
  const flags = [cli.go ? 'SEND' : 'DRY RUN', ...(cli.noWebsiteOnly ? ['--no-website-only'] : [])].join(' | ');
  console.log('\nRunning artisan web-redesign pipeline...');
  console.log(`City: ${cli.city ?? 'France (all)'} | Trade: ${tradeDisplay} | Limit: ${cli.limit ?? 'none'} | ${flags}\n`);

  const apiKey = process.env['GOOGLE_MAPS_API_KEY'] ?? '';
  if (!apiKey) logger.warn('GOOGLE_MAPS_API_KEY not set — Places enrichment will be skipped');

  // ── Module 1: Source from SIRENE ──────────────────────────────────────────
  console.log('[1/4] Sourcing artisans from SIRENE...');
  const companies = await sourceCompanies({
    naf_codes: cli.naf_codes,
    city:      cli.city,
    // No headcount filter: artisans include self-employed (tranche 'NN') and
    // large contractors alike — the chain/generic JS filters remove outliers.
  });
  console.log(`      → ${companies.length} artisans found`);

  // Pre-Places filters: chains and generic legal shells.
  const preFiltered = companies.filter(c => {
    const name = c.company_name?.trim() ?? '';
    if (isChain(name))            return false;
    if (isGenericLegalName(name)) return false;
    return true;
  });
  const preRemoved = companies.length - preFiltered.length;
  if (preRemoved > 0) {
    console.log(`      → ${preRemoved} removed (chains / generic legal names)`);
  }
  // Apply --limit only when explicitly provided; otherwise process everything.
  const limited = cli.limit !== null ? preFiltered.slice(0, cli.limit) : preFiltered;
  if (cli.limit !== null && limited.length < preFiltered.length) {
    console.log(`      → sliced to ${limited.length} (--limit ${cli.limit})`);
  }
  console.log();

  // ── Module 2: Google Maps Places enrichment ───────────────────────────────
  console.log('[2/4] Enriching with Google Maps Places...');
  let artisans: ArtisanRecord[];

  if (apiKey) {
    const tasks = limited.map(c => async (): Promise<ArtisanRecord> => {
      const tradeLabel = NAF_LABEL[c.naf_code] ?? c.naf_code;
      if (!c.company_name?.trim()) {
        return {
          ...c,
          trade_label:       tradeLabel,
          place_id:          null,
          website:           null,
          phone:             null,
          rating:            null,
          user_ratings_total: null,
          opportunity_score: 0,
          opportunity_tier:  'cold',
          signals:           [],
          contact_name:      null,
          email:             null,
          email_status:      'unknown',
          email_confidence:  0,
        };
      }
      const place = await enrichArtisanWithPlaces(apiKey, c.company_name, tradeLabel, c.city);
      return {
        siren:             c.siren,
        company_name:      c.company_name,
        city:              c.city,
        postal_code:       c.postal_code,
        naf_code:          c.naf_code,
        headcount_tranche: c.headcount_tranche,
        trade_label:       tradeLabel,
        ...place,
        opportunity_score: 0,
        opportunity_tier:  'cold',
        signals:           [],
        contact_name:      null,
        email:             null,
        email_status:      'unknown' as const,
        email_confidence:  0,
      };
    });
    artisans = await runWithConcurrency(tasks, 5);
  } else {
    artisans = limited.map(c => ({
      ...c,
      trade_label:       NAF_LABEL[c.naf_code] ?? c.naf_code,
      place_id:          null,
      website:           null,
      phone:             null,
      rating:            null,
      user_ratings_total: null,
      opportunity_score: 0,
      opportunity_tier:  'cold' as const,
      signals:           [],
      contact_name:      null,
      email:             null,
      email_status:      'unknown' as const,
      email_confidence:  0,
    }));
  }

  // Post-Places filter: ghost listings.
  const beforeGhost = artisans.length;
  artisans = artisans.filter(r => !isGhostListing(r));
  const ghostRemoved = beforeGhost - artisans.length;
  if (ghostRemoved > 0) {
    console.log(`      → ${ghostRemoved} removed (ghost listings: 0 reviews + no website)`);
  }

  const withWebsite = artisans.filter(r => r.website).length;
  console.log(`      → ${withWebsite}/${artisans.length} have a website on Google Maps\n`);

  // ── Module 3a: Website opportunity scoring ────────────────────────────────
  console.log('[3a/4] Scoring web presence...');
  const scoreTasks = artisans.map(rec => async (): Promise<void> => {
    const result = await scoreRestaurantWebsite(rec.website, rec.rating, rec.user_ratings_total);
    rec.opportunity_score = result.score;
    rec.opportunity_tier  = result.tier;
    rec.signals           = result.signals;
  });
  await runWithConcurrency(scoreTasks, 5);

  const hot  = artisans.filter(r => r.opportunity_tier === 'hot').length;
  const warm = artisans.filter(r => r.opportunity_tier === 'warm').length;
  console.log(`      → ${hot} hot, ${warm} warm opportunities\n`);

  // ── Module 3b: Contact finding (scrape → DNS fallback) ────────────────────
  console.log('[3b/4] Finding contacts — scraping websites, DNS fallback...');
  const contactTasks = artisans
    .filter(r => r.website)
    .map(rec => async (): Promise<void> => {
      if (isSocialOrPlatformUrl(rec.website!)) {
        logger.debug({ website: rec.website, company: rec.company_name }, 'Skipping email — social/platform URL');
        return; // leave email null, phone is the only contact channel
      }
      try {
        const domain  = new URL(rec.website!).hostname.replace(/^www\./, '');
        const contact = await findArtisanContact(
          rec.siren, rec.company_name, rec.city, domain, rec.website!,
        );
        rec.contact_name     = contact.contact_name;
        rec.email            = contact.email;
        rec.email_status     = contact.email_status;
        rec.email_confidence = contact.email_confidence;
      } catch (err) {
        logger.warn({ err, company: rec.company_name }, 'Contact finding failed — skipping');
      }
    });
  await runWithConcurrency(contactTasks, 10);

  const emailsFound    = artisans.filter(r => r.email).length;
  const emailsScraped  = artisans.filter(r => r.email_status === 'unverified').length;
  const emailsDns      = artisans.filter(r => r.email_status === 'probable').length;
  console.log(`      → ${emailsFound} emails found (${emailsScraped} scraped, ${emailsDns} DNS-probable)\n`);

  // Sort: hot → warm → cold, score desc within tier.
  const tierOrder: Record<string, number> = { hot: 0, warm: 1, cold: 2 };
  artisans.sort((a, b) => {
    const d = tierOrder[a.opportunity_tier] - tierOrder[b.opportunity_tier];
    return d !== 0 ? d : b.opportunity_score - a.opportunity_score;
  });

  // --no-website-only filter.
  if (cli.noWebsiteOnly) {
    const before = artisans.length;
    artisans = artisans.filter(r => r.website === null);
    console.log(`--no-website-only: keeping ${artisans.length}/${before} artisans with no website\n`);
  }

  printSummaryTable(artisans);

  // ── Module 4+5: Preview or enqueue ───────────────────────────────────────
  if (!cli.go) {
    console.log('[4/4] DRY RUN — writing preview...');
    writePreview(artisans);
    console.log(`Preview written to ${ARTISAN_PREVIEW}`);
    console.log('Add --go to enqueue and start the scheduler.\n');
  } else {
    console.log('[4/4] Enqueueing and starting scheduler...');
    enqueueArtisans(artisans);
    console.log('LIVE MODE — artisans queued. Emails send daily at 08:00.\n');
    startScheduler();
  }
}

main().catch(err => {
  logger.error(err, 'Artisan pipeline failed');
  process.exit(1);
});
