/**
 * Module 3 — Contact / Email Finding (NO fake emails, NO DNS guessing)
 *
 * Email priority:
 *  1. gmapsEmail (from Google Maps scraper CSV)
 *  2. Scrape email from company website
 *
 * If no real email found → email = null
 */

import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import pino from 'pino';
import { CompanyRecord, ContactRecord } from './types';
import { findGmapsMatch } from './gmaps-reader';

const logger = pino({ level: 'info' });

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
  'Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0',
];

export function randomUA(): string {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

export function randomDelay(min = 1000, max = 3000): Promise<void> {
  const ms = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ── Email extraction from HTML ────────────────────────────────────────────────

const JUNK_PATTERNS = [
  /noreply/i, /no-reply/i, /webmaster/i, /support@wix/i, /support@jimdo/i,
  /admin@/i, /abuse@/i, /postmaster@/i, /hostmaster@/i, /privacy@/i,
  /gdpr@/i, /@example\./i, /@test\./i, /\.png$/i, /\.jpg$/i,
];

function isValidEmail(email: string): boolean {
  if (!email.includes('@')) return false;
  if (JUNK_PATTERNS.some(re => re.test(email))) return false;
  return /^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$/.test(email.toLowerCase());
}

function extractEmailsFromHtml(html: string): string[] {
  const found = new Set<string>();

  // Standard mailto links
  const $ = cheerio.load(html);
  $('a[href^="mailto:"]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const email = href.replace('mailto:', '').split('?')[0].trim().toLowerCase();
    if (isValidEmail(email)) found.add(email);
  });

  // Regex sweep for obfuscated emails in text
  const emailRe = /[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}/gi;
  const matches = html.match(emailRe) || [];
  for (const m of matches) {
    const email = m.toLowerCase();
    if (isValidEmail(email)) found.add(email);
  }

  return Array.from(found);
}

async function fetchPage(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, {
      headers: {
        'User-Agent': randomUA(),
        'Accept-Language': 'fr-FR,fr;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      },
      redirect: 'follow',
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  }
}

export async function scrapeEmailFromWebsite(domain: string): Promise<string | null> {
  const base = `https://${domain}`;
  const pages = [base, `${base}/contact`, `${base}/nous-contacter`, `${base}/contactez-nous`];

  for (const url of pages) {
    const html = await fetchPage(url);
    if (!html) continue;

    const emails = extractEmailsFromHtml(html);
    if (emails.length > 0) {
      logger.info({ domain, url, email: emails[0] }, 'Email found on website');
      return emails[0];
    }
  }

  return null;
}

// ── Contact result type (inline to avoid extra file) ─────────────────────────

export interface ContactResult {
  first_name: string | null;
  last_name: string | null;
  email: string | null;
  email_status: 'verified' | 'probable' | 'unverified' | 'invalid' | 'generic' | 'unknown';
  confidence: number;
}

// ── Compat export for artisans.ts ─────────────────────────────────────────────

export async function findContactForDomainDnsOnly(
  _siren: string,
  _companyName: string,
  _city: string,
  domain: string,
): Promise<ContactResult> {
  const email = await scrapeEmailFromWebsite(domain);
  return {
    first_name: null,
    last_name: null,
    email,
    email_status: email ? 'probable' : 'unknown',
    confidence: email ? 70 : 0,
  };
}

// ── Main contact enrichment ───────────────────────────────────────────────────

async function processCompany(company: CompanyRecord): Promise<ContactRecord> {
  const name = company.company_name?.trim();

  // Step 1: gmaps email
  if (name) {
    const gmaps = findGmapsMatch(company.siret, name, company.postal_code || '');
    if (gmaps?.gmapsEmail) {
      logger.info({ company: name, email: gmaps.gmapsEmail }, 'Email from gmaps');
      return {
        ...company,
        first_name: null,
        last_name: null,
        email: gmaps.gmapsEmail,
        email_status: 'probable',
        confidence: gmaps.matchConfidence,
      };
    }
  }

  // Step 2: scrape from website
  if (company.domain) {
    const email = await scrapeEmailFromWebsite(company.domain);
    if (email) {
      return {
        ...company,
        first_name: null,
        last_name: null,
        email,
        email_status: 'probable',
        confidence: 70,
      };
    }
  }

  // No real email found
  return {
    ...company,
    first_name: null,
    last_name: null,
    email: null,
    email_status: 'unknown',
    confidence: 0,
  };
}

async function runWithConcurrency<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let next = 0;

  async function worker(): Promise<void> {
    while (next < tasks.length) {
      const i = next++;
      results[i] = await tasks[i]();
    }
  }

  await Promise.allSettled(Array.from({ length: Math.min(limit, tasks.length) }, () => worker()));
  return results;
}

export async function enrichWithContacts(companies: CompanyRecord[]): Promise<ContactRecord[]> {
  const tasks = companies.map(company => () => processCompany(company));
  return runWithConcurrency(tasks, 5);
}

// ── Legacy exports for restaurants pipeline ───────────────────────────────────
// restaurants.ts uses SMTP-based verification. Kept here to avoid breaking that pipeline.

import * as net from 'net';
import * as dns from 'dns';

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

export async function smtpVerify(email: string, mxHost: string): Promise<SmtpResult> {
  return new Promise(resolve => {
    let step = 0;
    let resolved = false;
    let buf = '';

    const done = (result: SmtpResult) => {
      if (!resolved) {
        resolved = true;
        try { socket.destroy(); } catch { /* ignore */ }
        resolve(result);
      }
    };

    const socket = new net.Socket();
    socket.setTimeout(3000);
    socket.on('timeout', () => done('error'));
    socket.on('error', () => done('error'));
    socket.connect(25, mxHost);

    socket.on('data', (data: Buffer) => {
      buf += data.toString();
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

      if (step === 0 && code === '220') { step = 1; socket.write('EHLO verify.local\r\n'); }
      else if (step === 1 && (code === '250' || code === '220')) { step = 2; socket.write('MAIL FROM:<verify@verify.local>\r\n'); }
      else if (step === 2 && code === '250') { step = 3; socket.write(`RCPT TO:<${email}>\r\n`); }
      else if (step === 3) {
        if (code === '250') done('250');
        else if (code === '550' || code === '551' || code === '553') done('550');
        else if (code === '450' || code === '451' || code === '452') done('greylist');
        else done('error');
      }
    });
  });
}

function normalizeName(name: string): string {
  return name.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z]/g, '');
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
}

interface NameResult {
  first_name: string;
  last_name: string;
  source: string;
}

export async function fetchSocieteComPage(siren: string): Promise<NameResult[]> {
  const url = `https://www.societe.com/societe/x-${siren}.html`;
  try {
    const res = await fetch(url, { headers: { 'User-Agent': randomUA(), 'Accept-Language': 'fr-FR,fr;q=0.9' } });
    if (!res.ok) return [];
    const html = await res.text();
    const $ = cheerio.load(html);
    const names: NameResult[] = [];

    $('[class*="dirigeant"], [class*="gerant"], [class*="manager"]').each((_, el) => {
      const text = $(el).text().trim();
      const match = text.match(/([A-ZÉÈÊËÀÂÙÛÜÎÏÔÇ][a-zéèêëàâùûüîïôç'-]+)\s+([A-ZÉÈÊËÀÂÙÛÜÎÏÔÇ]{2,})/);
      if (match) names.push({ first_name: capitalize(match[1]), last_name: match[2], source: 'societe_com' });
    });

    $('p, li, td').each((_, el) => {
      const text = $(el).text().trim();
      const match = text.match(/(?:gérant|associé|directeur|président)[^:]*:\s*([A-ZÉÈÊËÀÂÙÛÜÎÏÔÇ][a-zéèêëàâùûüîïôç'-]+)\s+([A-ZÉÈÊËÀÂÙÛÜÎÏÔÇ]{2,})/i);
      if (match) names.push({ first_name: capitalize(match[1]), last_name: match[2], source: 'societe_com' });
    });

    return names;
  } catch {
    return [];
  }
}

export function generateEmailPatterns(firstName: string, lastName: string, domain: string): string[] {
  const f  = normalizeName(firstName);
  const l  = normalizeName(lastName);
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

export async function findContactForDomain(
  siren: string,
  companyName: string,
  city: string,
  domain: string,
  smtpChecksPerDomain: Record<string, number>,
): Promise<ContactResult> {
  const societeNames = await fetchSocieteComPage(siren);
  await randomDelay();

  const names = societeNames.length > 0 ? societeNames : [];
  if (names.length === 0) {
    return { first_name: null, last_name: null, email: `contact@${domain}`, email_status: 'generic', confidence: 10 };
  }

  const { first_name, last_name } = names[0];
  const patterns = generateEmailPatterns(first_name, last_name, domain);
  const mxHost = await getMxHost(domain);

  if (!mxHost) {
    return { first_name, last_name, email: patterns[0], email_status: 'unknown', confidence: 20 };
  }

  smtpChecksPerDomain[domain] = smtpChecksPerDomain[domain] ?? 0;
  let foundEmail: string | null = null;
  let emailStatus: ContactResult['email_status'] = 'unknown';
  let confidence = 20;

  for (const pattern of patterns.slice(0, 5)) {
    if (smtpChecksPerDomain[domain] >= 3) break;
    const result = await smtpVerify(pattern, mxHost);
    smtpChecksPerDomain[domain]++;
    if (result === '250') {
      foundEmail = pattern;
      emailStatus = 'verified';
      confidence = 90;
      break;
    } else if (result === 'greylist' && !foundEmail) {
      foundEmail = pattern;
      emailStatus = 'unverified';
      confidence = 55;
    }
  }

  if (!foundEmail) {
    foundEmail = patterns[0];
    confidence = 30;
  }

  return { first_name, last_name, email: foundEmail, email_status: emailStatus, confidence };
}
