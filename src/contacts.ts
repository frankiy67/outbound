/**
 * Module 3 — Contact Finding
 * Finds decision-maker names via societe.com + Google,
 * generates email patterns, verifies via raw SMTP.
 */

import * as net from 'net';
import * as dns from 'dns';
import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import pino from 'pino';
import { CompanyRecord, ContactRecord } from './types';

const logger = pino({ level: 'info' });

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
  'Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
];

export function randomUA(): string {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

export function randomDelay(min = 2000, max = 5000): Promise<void> {
  const ms = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise(resolve => setTimeout(resolve, ms));
}

interface NameResult {
  first_name: string;
  last_name: string;
  source: string;
}

export async function fetchSocieteComPage(siren: string): Promise<NameResult[]> {
  const url = `https://www.societe.com/societe/x-${siren}.html`;
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': randomUA(), 'Accept-Language': 'fr-FR,fr;q=0.9' },
    });

    if (!res.ok) return [];

    const html = await res.text();
    const $ = cheerio.load(html);
    const names: NameResult[] = [];

    // societe.com lists dirigeants in elements with class identite-dirigeant or similar
    $('[class*="dirigeant"], [class*="gerant"], [class*="manager"]').each((_, el) => {
      const text = $(el).text().trim();
      const match = text.match(/([A-ZÉÈÊËÀÂÙÛÜÎÏÔÇ][a-zéèêëàâùûüîïôç'-]+)\s+([A-ZÉÈÊËÀÂÙÛÜÎÏÔÇ]{2,})/);
      if (match) {
        names.push({ first_name: capitalize(match[1]), last_name: match[2], source: 'societe_com' });
      }
    });

    // Also look for the identity block
    $('p, li, td').each((_, el) => {
      const text = $(el).text().trim();
      const match = text.match(/(?:gérant|associé|directeur|président)[^:]*:\s*([A-ZÉÈÊËÀÂÙÛÜÎÏÔÇ][a-zéèêëàâùûüîïôç'-]+)\s+([A-ZÉÈÊËÀÂÙÛÜÎÏÔÇ]{2,})/i);
      if (match) {
        names.push({ first_name: capitalize(match[1]), last_name: match[2], source: 'societe_com' });
      }
    });

    return names;
  } catch (err) {
    logger.warn({ err, siren }, 'societe.com fetch failed');
    return [];
  }
}

async function googleSearchName(companyName: string, city: string): Promise<NameResult[]> {
  const query = encodeURIComponent(
    `"${companyName}" "gérant" OR "associé" OR "directeur" site:linkedin.com OR societe.com OR infogreffe.fr`
  );
  const url = `https://www.google.com/search?q=${query}&num=5`;

  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': randomUA(), 'Accept-Language': 'fr-FR,fr;q=0.9' },
    });

    if (!res.ok) return [];

    const html = await res.text();
    const $ = cheerio.load(html);
    const names: NameResult[] = [];

    // Parse snippet text for name patterns: "Prénom NOM"
    $('.VwiC3b, .BNeawe, span, p').each((_, el) => {
      const text = $(el).text();
      const matches = [...text.matchAll(/([A-ZÉÈÊËÀÂÙÛÜÎÏÔÇ][a-zéèêëàâùûüîïôç'-]+)\s+([A-ZÉÈÊËÀÂÙÛÜÎÏÔÇ]{2,})/g)];
      for (const m of matches) {
        names.push({ first_name: capitalize(m[1]), last_name: m[2], source: 'google' });
      }
    });

    return names;
  } catch (err) {
    logger.warn({ err }, 'Google name search failed');
    return [];
  }
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
}

function normalizeName(name: string): string {
  return name
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z]/g, '');
}

export function generateEmailPatterns(firstName: string, lastName: string, domain: string): string[] {
  const f = normalizeName(firstName);
  const l = normalizeName(lastName);
  const fi = f.charAt(0);

  return [
    `${f}.${l}@${domain}`,
    `${fi}.${l}@${domain}`,
    `${f}@${domain}`,
    `${l}@${domain}`,
    `${f}${l}@${domain}`,
    `contact@${domain}`,
  ];
}

export async function getMxHost(domain: string): Promise<string | null> {
  try {
    const records = await dns.promises.resolveMx(domain);
    if (records.length === 0) return null;
    records.sort((a, b) => a.priority - b.priority);
    return records[0].exchange;
  } catch {
    return null;
  }
}

export type SmtpResult = '250' | '550' | 'greylist' | 'error';

// Internal: attempt on a specific port. Also returns whether we received the server
// banner (step > 0), so the caller can distinguish "port blocked" from "real error".
function attemptSmtpOnPort(
  email: string,
  mxHost: string,
  port: number,
): Promise<{ result: SmtpResult; reachedBanner: boolean }> {
  return new Promise(resolve => {
    let socket: net.Socket;
    let step = 0;
    let resolved = false;
    let buf = '';

    const done = (result: SmtpResult, reason?: string) => {
      if (!resolved) {
        resolved = true;
        console.log(`[SMTP] Result on :${port}: ${result}${reason ? ' (' + reason + ')' : ''}`);
        try { socket.destroy(); } catch { /* ignore destroy errors */ }
        resolve({ result, reachedBanner: step > 0 });
      }
    };

    try {
      socket = new net.Socket();
    } catch (err) {
      console.log(`[SMTP] Failed to create socket: ${(err as Error).message}`);
      resolve({ result: 'error', reachedBanner: false });
      return;
    }

    console.log(`[SMTP] Connecting to MX: ${mxHost}:${port}`);
    socket.setTimeout(3000);

    socket.on('timeout', () => {
      console.log(`[SMTP] Timeout after 3s at step ${step}`);
      done('error', 'timeout');
    });

    socket.on('error', (err: Error) => {
      console.log(`[SMTP] Connection error at step ${step}: ${err.message}`);
      done('error', err.message);
    });

    try {
      socket.connect(port, mxHost, () => {
        console.log(`[SMTP] TCP connected to ${mxHost}:${port}`);
      });
    } catch (err) {
      console.log(`[SMTP] socket.connect threw synchronously: ${(err as Error).message}`);
      done('error', (err as Error).message);
    }

    socket.on('data', (data: Buffer) => {
      buf += data.toString();

      // Wait until we have a complete final response line (no trailing dash = not multi-line).
      const lines = buf.split('\r\n');
      const lastComplete = lines.slice(0, -1);
      let finalLine: string | undefined;
      for (let i = lastComplete.length - 1; i >= 0; i--) {
        const l = lastComplete[i];
        if (l.length >= 4 && l[3] !== '-') { finalLine = l; break; }
        if (l.length === 3) { finalLine = l; break; }
      }
      if (!finalLine) return;
      buf = '';

      const code = finalLine.slice(0, 3);

      if (step === 0 && code === '220') {
        console.log(`[SMTP] Banner: ${finalLine.slice(0, 100)}`);
        step = 1;
        console.log(`[SMTP] Sending: EHLO verify.local`);
        socket.write('EHLO verify.local\r\n');

      } else if (step === 1 && (code === '250' || code === '220')) {
        console.log(`[SMTP] EHLO response: ${finalLine.slice(0, 100)}`);
        step = 2;
        console.log(`[SMTP] Sending: MAIL FROM:<verify@verify.local>`);
        socket.write('MAIL FROM:<verify@verify.local>\r\n');

      } else if (step === 2 && code === '250') {
        console.log(`[SMTP] MAIL FROM response: ${finalLine.slice(0, 100)}`);
        step = 3;
        console.log(`[SMTP] Sending: RCPT TO:<${email}>`);
        socket.write(`RCPT TO:<${email}>\r\n`);

      } else if (step === 3) {
        console.log(`[SMTP] RCPT TO <${email}> response: ${finalLine.slice(0, 100)}`);
        if (code === '250') {
          done('250');
        } else if (code === '550' || code === '551' || code === '553') {
          done('550');
        } else if (code === '450' || code === '451' || code === '452') {
          done('greylist');
        } else {
          done('error', `unexpected code ${code}`);
        }

      } else {
        console.log(`[SMTP] Unexpected data at step ${step}: ${finalLine.slice(0, 100)}`);
      }
    });
  });
}

// Public: attempt on port 25 only. If port 25 times out without a banner
// (blocked by ISP/firewall), skip port 587 and let the caller fall back to
// DNS confidence scoring — trying 587 in that situation just wastes another 3s.
// Any unexpected socket error is caught and returned as 'error' so the
// pipeline never crashes on ECONNREFUSED / WSAECONNREFUSED (10061) etc.
export async function smtpVerify(email: string, mxHost: string): Promise<SmtpResult> {
  try {
    const { result } = await attemptSmtpOnPort(email, mxHost, 25);
    return result;
  } catch (err) {
    console.log(`[SMTP] Unexpected error in smtpVerify for ${email}: ${(err as Error).message}`);
    return 'error';
  }
}

// DNS-based confidence score (0–100).
// Signals: MX present (+30), SPF present (+20), domain has A record (+20),
//          generic address like contact@/devis@/info@ (+15), local part matches company name (+10).
export async function dnsConfidenceScore(
  email: string,
  domain: string,
  companyName: string,
  mxHost: string | null,
): Promise<number> {
  let score = 0;

  if (mxHost) score += 30;

  try {
    const txt = await dns.promises.resolveTxt(domain);
    if (txt.some(r => r.join('').includes('v=spf1'))) score += 20;
  } catch { /* no SPF */ }

  try {
    await dns.promises.resolve4(domain);
    score += 20;
  } catch { /* no A record */ }

  const local = email.split('@')[0].toLowerCase();
  if (local === 'contact' || local === 'devis' || local === 'info') score += 15;

  const normalized = normalizeName(companyName);
  if (normalized && local && normalized.includes(local)) score += 10;

  console.log(`[DNS] Confidence for ${email}: ${score}/95`);
  return score;
}

/**
 * DNS-only contact finding — no TCP/SMTP connections.
 * Resolves gérant name (societe.com → Google), generates email patterns,
 * scores all candidates via DNS signals, and returns the highest-scoring one.
 * Designed for pipelines where SMTP is too slow or unreliable.
 */
export async function findContactForDomainDnsOnly(
  siren: string,
  companyName: string,
  city: string,
  domain: string,
): Promise<ContactResult> {
  logger.info({ company: companyName }, 'Finding contact (DNS only)...');

  // Step 1: gérant name via societe.com, fall back to Google.
  let names: NameResult[] = [];
  const societeNames = await fetchSocieteComPage(siren);
  await randomDelay();
  if (societeNames.length > 0) {
    names = societeNames;
  } else {
    names = await googleSearchName(companyName, city);
    await randomDelay();
  }

  const mxHost = await getMxHost(domain);

  // Build candidate list: top personal patterns then artisan-specific generics.
  const candidates: string[] = [];
  let firstName: string | null = null;
  let lastName: string | null  = null;

  if (names.length > 0) {
    const { first_name, last_name } = names[0];
    firstName = first_name;
    lastName  = last_name;
    candidates.push(...generateEmailPatterns(first_name, last_name, domain).slice(0, 3));
  }
  for (const local of ['contact', 'devis']) {
    const addr = `${local}@${domain}`;
    if (!candidates.includes(addr)) candidates.push(addr);
  }

  // Score every candidate in parallel and pick the highest.
  const scored = await Promise.all(
    candidates.map(async email => ({
      email,
      score: await dnsConfidenceScore(email, domain, companyName, mxHost),
    })),
  );
  scored.sort((a, b) => b.score - a.score);

  const best = scored[0];
  const emailStatus: ContactResult['email_status'] = best.score >= 60 ? 'probable' : 'unknown';

  return {
    first_name:   firstName,
    last_name:    lastName,
    email:        best.email,
    email_status: emailStatus,
    confidence:   best.score,
  };
}

// ── Core contact-finding logic (reusable by other modules) ────────────────────

export interface ContactResult {
  first_name: string | null;
  last_name: string | null;
  email: string | null;
  email_status: 'verified' | 'probable' | 'unverified' | 'invalid' | 'generic' | 'unknown';
  confidence: number;
}

/**
 * Finds the best contact email for a known domain.
 * Steps: societe.com gérant → Google → 6 SMTP-verified patterns → contact@ fallback.
 * Exported so other pipelines (e.g. restaurants) can reuse steps 1-4 and layer
 * their own additional fallbacks on top.
 */
export async function findContactForDomain(
  siren: string,
  companyName: string,
  city: string,
  domain: string,
  smtpChecksPerDomain: Record<string, number>,
): Promise<ContactResult> {
  logger.info({ company: companyName }, 'Finding contact...');

  // Step 1: gérant name via societe.com, fall back to Google.
  let names: NameResult[] = [];
  const societeNames = await fetchSocieteComPage(siren);
  await randomDelay();

  if (societeNames.length > 0) {
    names = societeNames;
  } else {
    names = await googleSearchName(companyName, city);
    await randomDelay();
  }

  if (names.length === 0) {
    return { first_name: null, last_name: null, email: `contact@${domain}`, email_status: 'generic', confidence: 10 };
  }

  const { first_name, last_name, source } = names[0];
  const patterns = generateEmailPatterns(first_name, last_name, domain);
  const mxHost = await getMxHost(domain);

  if (!mxHost) {
    return { first_name, last_name, email: patterns[0], email_status: 'unknown', confidence: 20 };
  }

  smtpChecksPerDomain[domain] = smtpChecksPerDomain[domain] ?? 0;
  console.log(`[SMTP] Domain ${domain}: ${smtpChecksPerDomain[domain]}/3 checks used, MX=${mxHost}`);

  let foundEmail: string | null = null;
  let emailStatus: ContactResult['email_status'] = 'unknown';
  let confidence = 20;
  let smtpAllBlocked = true; // flips false the moment we get any non-error SMTP response

  // Steps 2-4: SMTP-verify each pattern (up to 5, max 3 checks per domain).
  for (const pattern of patterns.slice(0, 5)) {
    if (smtpChecksPerDomain[domain] >= 3) {
      console.log(`[SMTP] Skipping ${pattern} — domain limit reached (3/3)`);
      break;
    }
    const result = await smtpVerify(pattern, mxHost);
    smtpChecksPerDomain[domain]++;

    if (result !== 'error') smtpAllBlocked = false;

    if (result === '250') {
      foundEmail = pattern;
      emailStatus = 'verified';
      confidence = 90 + (source === 'societe_com' ? 5 : 0);
      break;
    } else if (result === 'greylist') {
      foundEmail = pattern;
      emailStatus = 'unverified';
      confidence = 55;
    } else if (result === '550') {
      emailStatus = 'invalid';
      confidence = 10;
    }
  }

  // contact@ SMTP check when we don't have a verified personal email.
  if (!foundEmail && (emailStatus === 'unknown' || emailStatus === 'invalid')) {
    const genericEmail = `contact@${domain}`;
    if (smtpChecksPerDomain[domain] < 3) {
      const result = await smtpVerify(genericEmail, mxHost);
      smtpChecksPerDomain[domain]++;
      if (result !== 'error') smtpAllBlocked = false;
      if (result === '250') {
        foundEmail = genericEmail;
        emailStatus = 'generic';
        confidence = 60;
      }
    }
  }

  // DNS confidence fallback: both ports were blocked for every attempt.
  // Score the two best candidates and pick whichever exceeds the 60% threshold.
  if (!foundEmail && smtpAllBlocked) {
    console.log(`[DNS] SMTP blocked on all attempts — computing DNS confidence scores`);
    const contactEmail = `contact@${domain}`;
    const [contactScore, patternScore] = await Promise.all([
      dnsConfidenceScore(contactEmail, domain, companyName, mxHost),
      dnsConfidenceScore(patterns[0], domain, companyName, mxHost),
    ]);

    if (contactScore >= patternScore && contactScore > 60) {
      foundEmail  = contactEmail;
      emailStatus = 'probable';
      confidence  = contactScore;
    } else if (patternScore > 60) {
      foundEmail  = patterns[0];
      emailStatus = 'probable';
      confidence  = patternScore;
    } else {
      foundEmail  = patterns[0];
      emailStatus = 'unknown';
      confidence  = Math.max(contactScore, patternScore);
    }
  }

  if (!foundEmail) {
    foundEmail = patterns[0];
    emailStatus = emailStatus === 'invalid' ? 'unknown' : emailStatus;
    confidence = 30;
  }

  return { first_name, last_name, email: foundEmail, email_status: emailStatus, confidence };
}

// ── Accounting pipeline wrappers ──────────────────────────────────────────────

async function runWithConcurrency<T>(
  tasks: (() => Promise<T>)[],
  limit: number
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
    Array.from({ length: Math.min(limit, tasks.length) }, () => worker())
  );

  return results;
}

async function processCompany(
  company: CompanyRecord,
  smtpChecksPerDomain: Record<string, number>,
): Promise<ContactRecord> {
  if (!company.domain || !company.company_name?.trim()) {
    return { ...company, first_name: null, last_name: null, email: null, email_status: 'unknown', confidence: 0 };
  }
  const contact = await findContactForDomain(
    company.siren, company.company_name, company.city, company.domain, smtpChecksPerDomain,
  );
  return { ...company, ...contact };
}

export async function enrichWithContacts(companies: CompanyRecord[]): Promise<ContactRecord[]> {
  const smtpChecksPerDomain: Record<string, number> = {};
  const tasks = companies.map(company => () => processCompany(company, smtpChecksPerDomain));
  return runWithConcurrency(tasks, 3);
}

// ── Standalone SMTP test (npm run smtp-test) ──────────────────────────────────

async function testSmtp(): Promise<void> {
  const email = 'contact@fidimpact.fr';
  const domain = email.split('@')[1];
  const companyName = 'Fidimpact';

  console.log(`\n=== SMTP Test: ${email} ===\n`);

  console.log(`[1] Resolving MX for ${domain}...`);
  const mxHost = await getMxHost(domain);
  if (!mxHost) {
    console.log(`[!] No MX record found for ${domain} — DNS lookup failed or domain has no MX`);
    return;
  }
  console.log(`[✓] MX host: ${mxHost}\n`);

  console.log(`[2] SMTP verification (port 25, fallback 587)...`);
  const result = await smtpVerify(email, mxHost);
  console.log(`\n[✓] SMTP result: ${result}`);
  console.log('    250=exists | 550=rejected | greylist=greylisted | error=blocked/timeout\n');

  if (result === 'error') {
    console.log(`[3] SMTP blocked — computing DNS confidence score as fallback...`);
    const score = await dnsConfidenceScore(email, domain, companyName, mxHost);
    const status = score > 60 ? 'probable' : 'unknown';
    console.log(`\n[✓] DNS confidence: ${score}/95 → email_status = "${status}"`);
    console.log('    Signals: MX(+30) SPF(+20) A-record(+20) generic-address(+15) name-match(+10)');
  }
}

if (process.argv.includes('--test-smtp')) {
  testSmtp().catch(err => { console.error('[!] testSmtp crashed:', err); process.exit(1); });
}
