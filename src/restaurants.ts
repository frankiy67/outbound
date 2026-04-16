/**
 * Restaurant Web Redesign Pipeline
 *
 * Finds restaurants via SIRENE + Google Maps Places, scores their web
 * presence for redesign opportunity, drafts outreach emails.
 *
 * Usage:
 *   npm run restaurants "Paris" [--limit 200] [--go] [--reset] [--status] [--no-website-only]
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
  findContactForDomain,
  getMxHost,
  smtpVerify,
  fetchSocieteComPage,
  generateEmailPatterns,
  randomDelay,
  randomUA,
  type ContactResult,
} from './contacts';
import { DATA_DIR } from './config';

const logger = pino({ level: 'info' });

const RESTAURANT_QUEUE_DB = path.join(DATA_DIR, 'restaurant_queue.db');
const RESTAURANT_PREVIEW  = path.join(DATA_DIR, 'restaurant_preview.json');

const RESTAURANT_NAF = ['56.10A', '56.10B', '56.21Z', '56.30Z'];

// ── Types ─────────────────────────────────────────────────────────────────────

interface RestaurantRecord {
  siren: string;
  company_name: string;
  city: string;
  postal_code: string;
  naf_code: string;
  headcount_tranche: string;
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
  city: string;
  limit: number;
  go: boolean;
  reset: boolean;
  status: boolean;
  noWebsiteOnly: boolean;
}

function parseCli(args: string[]): CliArgs {
  const go            = args.includes('--go');
  const reset         = args.includes('--reset');
  const status        = args.includes('--status');
  const noWebsiteOnly = args.includes('--no-website-only');

  const limitIdx = args.findIndex(a => a === '--limit');
  const limit = limitIdx !== -1 && args[limitIdx + 1]
    ? parseInt(args[limitIdx + 1], 10)
    : 200;

  // City is the first positional arg (not a flag, not a pure number).
  const city = args.find(a => !a.startsWith('--') && !/^\d+$/.test(a)) ?? 'Paris';

  return { city, limit, go, reset, status, noWebsiteOnly };
}

// ── False-positive filters ────────────────────────────────────────────────────

// Known national/international chains — these aren't indie restaurants and
// will never need a freelance web redesign.
const CHAIN_NAMES = [
  'SODEXO',
  'HIPPOPOTAMUS',
  'HIPPO EXPLOITATION',
  'HIPPOTAMUS',
  'BUFFALO GRILL',
  'INDIANA CAFE',
  'INDIANA RESTAURANT',
  'COURTEPAILLE',
  'FLUNCH',
  'LEON DE BRUXELLES',
  'LEON RESTAURANT',
  'QUICK RESTAURANT',
  'MCDONALD',
  'KFC ',          // trailing space avoids matching e.g. "BKFC"
  'BURGER KING',
  'SUBWAY FRANCE',
  'PAUL BOULANGERIE',
  'BRIOCHE DOREE',
  'COJEAN',
  'EXKI',
];

function isChain(name: string): boolean {
  const upper = name.toUpperCase();
  return CHAIN_NAMES.some(chain => upper === chain || upper.startsWith(chain));
}

// Legal-form prefix list — used to strip the entity type and inspect what's left.
const LEGAL_PREFIX_RE = /^(SARL|SAS|SASU|EURL|SA|SNC|SCI|SELARL|SC|GIE|ASSOCIATION|ASSO)\s+/;

// "SARL GDM" or "SAS XB" — just a legal wrapper with initials, no restaurant identity.
function isGenericLegalName(name: string): boolean {
  const stripped = name.toUpperCase().replace(LEGAL_PREFIX_RE, '').trim();
  if (!stripped) return true;           // nothing after legal form
  if (stripped.length <= 3) return true; // e.g. "SAS DG"
  // No vowels → all consonants/digits → looks like an abbreviation (e.g. "SARL BRGR")
  if (!/[AEIOUÉÈÊËÀÂÙÛÜÎÏÔŒ]/i.test(stripped)) return true;
  return false;
}

// Ghost listing: Places API found it but it has zero reviews AND no website —
// likely a permanently closed or unclaimed listing with no real activity.
function isGhostListing(rec: RestaurantRecord): boolean {
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
  const url =
    `https://maps.googleapis.com/maps/api/place/textsearch/json` +
    `?query=${encodeURIComponent(query)}&type=restaurant&key=${apiKey}`;
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const data = await res.json() as {
      status: string;
      results?: PlaceSearchResult[];
    };
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
    `?place_id=${placeId}` +
    `&fields=website,formatted_phone_number` +
    `&key=${apiKey}`;
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

async function enrichRestaurantWithPlaces(
  apiKey: string,
  companyName: string,
  city: string,
): Promise<{
  place_id: string | null;
  website: string | null;
  phone: string | null;
  rating: number | null;
  user_ratings_total: number | null;
}> {
  const query = `${companyName} restaurant ${city}`;
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

// ── Module 3b: Contact finding for restaurants with a domain ─────────────────

// Extract all mailto: hrefs from a fetched HTML page, excluding obvious
// no-reply / system addresses.
const MAILTO_RE = /href=["']mailto:([^"'?\s]+)/gi;
const IGNORED_PREFIXES = ['noreply', 'no-reply', 'mailer', 'bounce', 'postmaster', 'abuse'];

function extractMailtos(html: string): string[] {
  const found: string[] = [];
  for (const match of html.matchAll(MAILTO_RE)) {
    const email = match[1].toLowerCase();
    if (!email.includes('@')) continue;
    const local = email.split('@')[0];
    if (IGNORED_PREFIXES.some(p => local.startsWith(p))) continue;
    found.push(email);
  }
  return [...new Set(found)];
}

async function fetchHtml(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': randomUA(), 'Accept-Language': 'fr-FR,fr;q=0.9' },
      timeout: 8000,
    } as Parameters<typeof fetch>[1]);
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  }
}

async function scrapeMailtoFromWebsite(domain: string): Promise<string | null> {
  const base = `https://${domain}`;
  for (const url of [base, `${base}/contact`, `${base}/nous-contacter`, `${base}/reservation`]) {
    const html = await fetchHtml(url);
    if (!html) continue;
    const emails = extractMailtos(html);
    if (emails.length > 0) {
      logger.debug({ domain, url, email: emails[0] }, 'mailto found on website');
      return emails[0];
    }
  }
  return null;
}

/**
 * Full contact-finding chain for a restaurant with a known domain:
 * 1. societe.com gérant → 6 SMTP-verified patterns     (reuses findContactForDomain)
 * 2. Scrape website pages for mailto: links
 * 3. SMTP check contact@domain → reservation@domain
 * 4. Hard fallback: contact@domain as generic
 */
async function findRestaurantContact(
  siren: string,
  companyName: string,
  city: string,
  domain: string,
  smtpChecksPerDomain: Record<string, number>,
): Promise<{ contact_name: string | null; email: string | null; email_status: ContactResult['email_status']; email_confidence: number }> {
  // Step 1: gérant name → SMTP-verified personal email.
  const base = await findContactForDomain(siren, companyName, city, domain, smtpChecksPerDomain);

  // If we have a high-confidence result, use it immediately.
  if (base.email_status === 'verified' || base.email_status === 'unverified') {
    const name = base.first_name && base.last_name
      ? `${base.first_name} ${base.last_name}`
      : null;
    return { contact_name: name, email: base.email, email_status: base.email_status, email_confidence: base.confidence };
  }

  // Step 2: scrape website for mailto: links.
  const scraped = await scrapeMailtoFromWebsite(domain);
  if (scraped) {
    const name = base.first_name && base.last_name ? `${base.first_name} ${base.last_name}` : null;
    return { contact_name: name, email: scraped, email_status: 'unverified', email_confidence: 50 };
  }

  // Step 3: SMTP check contact@ then reservation@.
  const mxHost = await getMxHost(domain);
  if (mxHost) {
    smtpChecksPerDomain[domain] = smtpChecksPerDomain[domain] ?? 0;

    for (const candidate of [`contact@${domain}`, `reservation@${domain}`]) {
      if (smtpChecksPerDomain[domain] >= 5) break;
      const result = await smtpVerify(candidate, mxHost);
      smtpChecksPerDomain[domain]++;
      if (result === '250') {
        return { contact_name: null, email: candidate, email_status: 'generic', email_confidence: 60 };
      }
    }
  }

  // Step 4: hard fallback — return whatever findContactForDomain gave us
  // (at minimum contact@domain as a generic address).
  const name = base.first_name && base.last_name ? `${base.first_name} ${base.last_name}` : null;
  return { contact_name: name, email: base.email, email_status: base.email_status, email_confidence: base.confidence };
}

// ── Module 4: Email drafting ──────────────────────────────────────────────────

const SEQUENCE_STEPS: Record<'hot' | 'warm' | 'cold', number[]> = {
  hot:  [0, 3, 7],
  warm: [0, 5],
  cold: [0],
};

function selectAngle(signals: string[]): string {
  if (signals.includes('no_website'))  return 'no_website';
  if (signals.includes('not_mobile'))  return 'not_mobile';
  if (signals.includes('no_booking'))  return 'no_booking';
  if (signals.includes('old_builder') || signals.includes('old_copyright')) return 'old_website';
  if (signals.includes('low_rating'))  return 'low_rating';
  return 'generic';
}

function draftEmail(
  rec: RestaurantRecord,
  step: number,
): { subject: string; body: string } {
  const angle   = selectAngle(rec.signals);
  const company = rec.company_name;
  const city    = rec.city;

  type TemplateFn = (s: number) => { subject: string; body: string };

  const templates: Record<string, TemplateFn> = {
    no_website: (s) => ({
      subject: s === 0
        ? `${company} — votre restaurant est invisible en ligne`
        : s === 1
        ? `suite — ${company}`
        : `dernière relance — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nJe recherche des restaurants à ${city} et j'ai remarqué que ${company} n'a pas de site web.\n\nAujourd'hui, 8 clients sur 10 cherchent un restaurant en ligne avant de se déplacer. Sans présence web, vous passez sous le radar.\n\nOn crée des sites simples, rapides et abordables pour les restaurants — pensés pour être trouvés sur Google et convertir les visites en réservations.\n\nEst-ce un sujet qui vous parle ?`
        : s === 1
        ? `Bonjour,\n\nJe reviens sur mon message de la semaine dernière.\n\nUne question directe : est-ce que l'absence de site web est un choix délibéré, ou simplement quelque chose que vous n'avez pas encore eu le temps de traiter ?\n\nOn peut avoir quelque chose en ligne en moins de 2 semaines.`
        : `Bonjour,\n\nDernier message de ma part.\n\nSi le timing n'est pas bon pour l'instant, aucun problème. Bonne continuation à ${company}.`,
    }),

    not_mobile: (s) => ({
      subject: s === 0
        ? `${company} — votre site ne s'affiche pas bien sur mobile`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nJ'ai regardé le site de ${company} et il n'est pas adapté aux smartphones.\n\nOr 70 % des recherches de restaurants se font sur mobile — si le site est difficile à lire, les gens passent à l'autre.\n\nOn peut corriger ça rapidement avec une refonte légère, sans tout changer.\n\nÇa vous intéresse ?`
        : `Bonjour,\n\nPas de réponse à mon précédent message — peut-être que le moment n'était pas idéal.\n\nL'offre tient toujours si vous souhaitez améliorer l'expérience mobile de ${company}.`,
    }),

    no_booking: (s) => ({
      subject: s === 0
        ? `${company} — vous perdez des réservations chaque soir`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nJ'ai regardé le site de ${company} et il n'y a pas de système de réservation en ligne.\n\nLes restaurants qui activent la réservation en ligne reçoivent en moyenne 20 % de couverts supplémentaires — simplement parce que les clients peuvent réserver à 23 h quand ils planifient leur semaine.\n\nOn intègre ça dans les sites qu'on crée. Est-ce quelque chose qui manque chez vous ?`
        : `Bonjour,\n\nJuste une relance — est-ce que la réservation en ligne est un sujet pour ${company} en ce moment ?`,
    }),

    old_website: (s) => ({
      subject: s === 0
        ? `${company} — votre site a besoin d'une mise à jour`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nJe suis tombé sur le site de ${company} et j'ai remarqué qu'il semble dater d'il y a quelques années.\n\nUn site vieillissant donne une mauvaise première impression — surtout quand les concurrents ont des sites modernes et rapides.\n\nOn fait des refontes rapides pour les restaurants, sans tout réinventer. Ça vous intéresse ?`
        : `Bonjour,\n\nPas de retour à mon message précédent — peut-être que le timing n'était pas le bon.\n\nL'offre reste valable si vous souhaitez moderniser le site de ${company}.`,
    }),

    low_rating: (s) => ({
      subject: s === 0
        ? `${company} — votre note Google mérite attention`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nJ'ai vu que ${company} a une note Google en dessous de la moyenne pour ${city}.\n\nUne partie du travail pour améliorer cette note passe par l'expérience en ligne : un site qui inspire confiance, des réponses aux avis, une présentation soignée.\n\nOn aide les restaurants à travailler ça. Est-ce que c'est quelque chose qui vous préoccupe ?`
        : `Bonjour,\n\nJuste un suivi — est-ce que la présence en ligne de ${company} est un chantier ouvert chez vous en ce moment ?`,
    }),

    generic: (s) => ({
      subject: s === 0 ? `idée pour ${company}` : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nQuelques améliorations simples à votre présence en ligne pourraient amener plus de couverts à ${company}.\n\nOn travaille avec des restaurants à ${city} sur ce sujet — site, réservation, visibilité Google.\n\nEst-ce que ça vous parle ?`
        : `Bonjour,\n\nPas de retour — je suppose que le timing n'était pas idéal. Toujours disponible si le sujet revient.`,
    }),
  };

  return (templates[angle] ?? templates['generic'])(step);
}

// ── Module 5: SQLite queue ────────────────────────────────────────────────────

function openQueueDb(): DatabaseSync {
  const db = new DatabaseSync(RESTAURANT_QUEUE_DB);
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;

    CREATE TABLE IF NOT EXISTS restaurant_queue (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      siren          TEXT NOT NULL,
      lead_email     TEXT,
      lead_company   TEXT NOT NULL,
      lead_city      TEXT NOT NULL,
      subject        TEXT NOT NULL,
      body           TEXT NOT NULL,
      step           INTEGER NOT NULL DEFAULT 0,
      scheduled_date TEXT NOT NULL,
      status         TEXT NOT NULL DEFAULT 'pending',
      created_at     TEXT NOT NULL,
      sent_at        TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_siren_step
      ON restaurant_queue(siren, step);

    CREATE TABLE IF NOT EXISTS contacted (
      siren        TEXT PRIMARY KEY,
      contacted_at TEXT NOT NULL
    );
  `);
  return db;
}

function getContactedSirens(db: DatabaseSync): Set<string> {
  const rows = db.prepare('SELECT siren FROM contacted').all() as Array<{ siren: string }>;
  return new Set(rows.map(r => r.siren));
}

function enqueueRestaurants(restaurants: RestaurantRecord[]): void {
  const db        = openQueueDb();
  const contacted = getContactedSirens(db);

  const insert = db.prepare(`
    INSERT OR IGNORE INTO restaurant_queue
      (siren, lead_email, lead_company, lead_city, subject, body, step,
       scheduled_date, status, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
  `);
  const markContacted = db.prepare(`
    INSERT OR IGNORE INTO contacted (siren, contacted_at) VALUES (?, ?)
  `);

  const beginTx  = db.prepare('BEGIN');
  const commitTx = db.prepare('COMMIT');

  let queued  = 0;
  let skipped = 0;

  beginTx.run();
  for (const rec of restaurants) {
    if (contacted.has(rec.siren)) { skipped++; continue; }

    const steps = SEQUENCE_STEPS[rec.opportunity_tier];
    const today = new Date();

    for (let i = 0; i < steps.length; i++) {
      const scheduledDate = new Date(today);
      scheduledDate.setDate(today.getDate() + steps[i]);
      const draft = draftEmail(rec, i);

      insert.run(
        rec.siren,
        rec.email ?? null,
        rec.company_name,
        rec.city,
        draft.subject,
        draft.body,
        i,
        scheduledDate.toISOString().split('T')[0],
        new Date().toISOString(),
      );
    }

    markContacted.run(rec.siren, new Date().toISOString());
    queued++;
  }
  commitTx.run();
  db.close();

  logger.info({ queued, skipped }, 'Restaurants enqueued (skipped = already contacted)');
}

// ── Status ────────────────────────────────────────────────────────────────────

function printStatus(): void {
  if (!fs.existsSync(RESTAURANT_QUEUE_DB)) {
    console.log('No queue database found. Run the pipeline first.');
    return;
  }
  const db      = openQueueDb();
  const total   = (db.prepare('SELECT COUNT(*) AS n FROM restaurant_queue').get() as { n: number }).n;
  const pending = (db.prepare("SELECT COUNT(*) AS n FROM restaurant_queue WHERE status='pending'").get() as { n: number }).n;
  const sent    = (db.prepare("SELECT COUNT(*) AS n FROM restaurant_queue WHERE status='sent'").get() as { n: number }).n;
  const failed  = (db.prepare("SELECT COUNT(*) AS n FROM restaurant_queue WHERE status='failed'").get() as { n: number }).n;
  const touches = (db.prepare('SELECT COUNT(*) AS n FROM contacted').get() as { n: number }).n;
  db.close();

  console.log('\n═══════════════════════════════════════════');
  console.log('  RESTAURANT PIPELINE — STATUS');
  console.log('═══════════════════════════════════════════');
  console.log(`  Emails in queue      : ${total}`);
  console.log(`  Pending              : ${pending}`);
  console.log(`  Sent                 : ${sent}`);
  console.log(`  Failed               : ${failed}`);
  console.log(`  Restaurants contacted: ${touches}`);
  console.log('═══════════════════════════════════════════\n');
}

// ── Reset ─────────────────────────────────────────────────────────────────────

function resetQueues(): void {
  if (!fs.existsSync(RESTAURANT_QUEUE_DB)) {
    console.log('Nothing to reset — queue database does not exist.');
    return;
  }
  const db = openQueueDb();
  db.exec('DELETE FROM restaurant_queue; DELETE FROM contacted;');
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
    SELECT * FROM restaurant_queue
    WHERE scheduled_date <= ? AND status = 'pending' AND lead_email IS NOT NULL
    ORDER BY scheduled_date ASC
  `).all(today) as unknown as Array<{
    id: number;
    lead_email: string;
    lead_company: string;
    subject: string;
    body: string;
    step: number;
  }>;

  if (pending.length === 0) {
    logger.info('No restaurant emails to send today');
    db.close();
    return;
  }

  const transporter = createTransporter();
  const update = db.prepare(
    `UPDATE restaurant_queue SET status = ?, sent_at = ? WHERE id = ?`,
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
      logger.info({ to: entry.lead_email, company: entry.lead_company, step: entry.step }, 'Email sent');
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
  logger.info('Restaurant scheduler started — runs daily at 08:00');
  cron.schedule('0 8 * * *', async () => {
    logger.info('Running scheduled restaurant email send...');
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

function writePreview(restaurants: RestaurantRecord[]): void {
  const preview = restaurants.map(rec => ({
    company:          rec.company_name,
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

  fs.writeFileSync(RESTAURANT_PREVIEW, JSON.stringify(preview, null, 2));
  logger.info({ path: RESTAURANT_PREVIEW, count: preview.length }, 'Restaurant preview written');
}

// ── Summary table ─────────────────────────────────────────────────────────────

function printSummaryTable(restaurants: RestaurantRecord[]): void {
  console.log('\n═══════════════════════════════════════════════════════════════════════');
  console.log('  RESTAURANT PIPELINE — RESULTS');
  console.log('═══════════════════════════════════════════════════════════════════════');
  console.log(
    `${'Company'.padEnd(35)} ${'Website'.padEnd(28)} ${'⭐'.padEnd(5)} ${'Score'.padEnd(6)} ${'Tier'.padEnd(6)} Signals`,
  );
  console.log('─'.repeat(110));

  for (const rec of restaurants) {
    const name   = (rec.company_name ?? 'Unknown').substring(0, 34).padEnd(35);
    const web    = (rec.website ?? '—').substring(0, 27).padEnd(28);
    const rating = rec.rating !== null ? String(rec.rating).padEnd(5) : '—    ';
    const sigs   = rec.signals.join(', ');
    console.log(
      `${name} ${web} ${rating} ${String(rec.opportunity_score).padEnd(6)} ${rec.opportunity_tier.padEnd(6)} ${sigs}`,
    );
  }

  const hot  = restaurants.filter(r => r.opportunity_tier === 'hot').length;
  const warm = restaurants.filter(r => r.opportunity_tier === 'warm').length;
  const cold = restaurants.filter(r => r.opportunity_tier === 'cold').length;

  console.log('─'.repeat(110));
  console.log(`Total: ${restaurants.length}  |  Hot: ${hot}  |  Warm: ${warm}  |  Cold: ${cold}`);
  console.log('═══════════════════════════════════════════════════════════════════════\n');
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const args = process.argv.slice(2);

  if (args.includes('--status')) { printStatus(); return; }
  if (args.includes('--reset'))  { resetQueues(); return; }

  const cli = parseCli(args);

  console.log('\nRunning restaurant web-redesign pipeline...');
  const flags = [cli.go ? 'SEND' : 'DRY RUN', ...(cli.noWebsiteOnly ? ['--no-website-only'] : [])].join(' | ');
  console.log(`City: ${cli.city} | Limit: ${cli.limit} | ${flags}\n`);

  const apiKey = process.env['GOOGLE_MAPS_API_KEY'] ?? '';
  if (!apiKey) logger.warn('GOOGLE_MAPS_API_KEY not set — Places enrichment will be skipped');

  // ── Module 1: Source from SIRENE ──────────────────────────────────────────
  console.log('[1/4] Sourcing restaurants from SIRENE...');
  const companies = await sourceCompanies({
    naf_codes:     RESTAURANT_NAF,
    city:          cli.city,
    headcount_min: 0,
    headcount_max: 500,
    limit:         cli.limit,
  });
  console.log(`      → ${companies.length} restaurants found`);

  // Pre-Places filters: chains and generic legal shells — no point paying for
  // API calls on companies we'd discard anyway.
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
  console.log();

  // ── Module 2: Google Maps Places enrichment ───────────────────────────────
  console.log('[2/4] Enriching with Google Maps Places...');
  let restaurants: RestaurantRecord[];

  if (apiKey) {
    const tasks = preFiltered.map(c => async (): Promise<RestaurantRecord> => {
      if (!c.company_name?.trim()) {
        return {
          ...c, place_id: null, website: null, phone: null,
          rating: null, user_ratings_total: null,
          opportunity_score: 0, opportunity_tier: 'cold', signals: [],
          contact_name: null, email: null, email_status: 'unknown', email_confidence: 0,
        };
      }
      const place = await enrichRestaurantWithPlaces(apiKey, c.company_name, c.city);
      return {
        siren:              c.siren,
        company_name:       c.company_name,
        city:               c.city,
        postal_code:        c.postal_code,
        naf_code:           c.naf_code,
        headcount_tranche:  c.headcount_tranche,
        ...place,
        opportunity_score:  0,
        opportunity_tier:   'cold',
        signals:            [],
        contact_name:       null,
        email:              null,
        email_status:       'unknown' as const,
        email_confidence:   0,
      };
    });
    restaurants = await runWithConcurrency(tasks, 5);
  } else {
    restaurants = preFiltered.map(c => ({
      ...c, place_id: null, website: null, phone: null,
      rating: null, user_ratings_total: null,
      opportunity_score: 0, opportunity_tier: 'cold' as const, signals: [],
      contact_name: null, email: null, email_status: 'unknown' as const, email_confidence: 0,
    }));
  }

  // Post-Places filter: ghost listings (0 reviews + no website = likely closed/unclaimed).
  const beforeGhost = restaurants.length;
  restaurants = restaurants.filter(r => !isGhostListing(r));
  const ghostRemoved = beforeGhost - restaurants.length;
  if (ghostRemoved > 0) {
    console.log(`      → ${ghostRemoved} removed (ghost listings: 0 reviews + no website)`);
  }

  const withWebsite = restaurants.filter(r => r.website).length;
  console.log(`      → ${withWebsite}/${restaurants.length} have a website on Google Maps\n`);

  // ── Module 3: Website opportunity scoring ─────────────────────────────────
  console.log('[3a/4] Scoring web presence...');
  const scoreTasks = restaurants.map(rec => async (): Promise<void> => {
    const result = await scoreRestaurantWebsite(
      rec.website,
      rec.rating,
      rec.user_ratings_total,
    );
    rec.opportunity_score = result.score;
    rec.opportunity_tier  = result.tier;
    rec.signals           = result.signals;
  });
  await runWithConcurrency(scoreTasks, 5);

  const hot  = restaurants.filter(r => r.opportunity_tier === 'hot').length;
  const warm = restaurants.filter(r => r.opportunity_tier === 'warm').length;
  console.log(`      → ${hot} hot, ${warm} warm opportunities\n`);

  // ── Module 3b: Contact finding ───────────────────────────────────────────
  console.log('[3b/4] Finding contacts for restaurants with a domain...');
  const smtpChecksPerDomain: Record<string, number> = {};
  const contactTasks = restaurants
    .filter(r => r.website)
    .map(rec => async (): Promise<void> => {
      const domain = new URL(rec.website!).hostname.replace(/^www\./, '');
      await randomDelay(500, 1500); // lighter delay than accounting — restaurants have simpler sites
      const contact = await findRestaurantContact(
        rec.siren, rec.company_name, rec.city, domain, smtpChecksPerDomain,
      );
      rec.contact_name    = contact.contact_name;
      rec.email           = contact.email;
      rec.email_status    = contact.email_status;
      rec.email_confidence = contact.email_confidence;
    });
  await runWithConcurrency(contactTasks, 3);

  const emailsFound  = restaurants.filter(r => r.email).length;
  const emailVerified = restaurants.filter(r => r.email_status === 'verified').length;
  console.log(`      → ${emailsFound} emails found (${emailVerified} SMTP-verified)\n`);

  // Sort: hot → warm → cold, then by score descending within tier.
  const tierOrder: Record<string, number> = { hot: 0, warm: 1, cold: 2 };
  restaurants.sort((a, b) => {
    const d = tierOrder[a.opportunity_tier] - tierOrder[b.opportunity_tier];
    return d !== 0 ? d : b.opportunity_score - a.opportunity_score;
  });

  // ── --no-website-only filter ──────────────────────────────────────────────
  if (cli.noWebsiteOnly) {
    const before = restaurants.length;
    restaurants = restaurants.filter(r => r.website === null);
    console.log(`--no-website-only: keeping ${restaurants.length}/${before} restaurants with no website\n`);
  }

  printSummaryTable(restaurants);

  // ── Module 4+5: Preview or enqueue ───────────────────────────────────────
  if (!cli.go) {
    console.log('[4/4] DRY RUN — writing preview...');
    writePreview(restaurants);
    console.log(`Preview written to ${RESTAURANT_PREVIEW}`);
    console.log('Add --go to enqueue and start the scheduler.\n');
  } else {
    console.log('[4/4] Enqueueing and starting scheduler...');
    enqueueRestaurants(restaurants);
    console.log('LIVE MODE — restaurants queued. Emails send daily at 08:00.\n');
    startScheduler();
  }
}

main().catch(err => {
  logger.error(err, 'Restaurant pipeline failed');
  process.exit(1);
});
