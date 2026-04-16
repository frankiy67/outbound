/**
 * Module 2 — Domain & Phone Discovery (NO Google Maps API)
 *
 * Priority:
 *  1. Google Maps scraper CSV match (via gmaps-reader.ts)
 *  2. Pages Jaunes scraping fallback
 */

import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import pino from 'pino';
import { CompanyRecord } from './types';
import { findGmapsMatch, gmapsCsvExists, type GmapsMatch } from './gmaps-reader';

const logger = pino({ level: 'info' });

// ── Shared fetch helper ───────────────────────────────────────────────────────

async function fetchHtml(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
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

// ── Pages Jaunes scraping ─────────────────────────────────────────────────────

function slugify(s: string): string {
  return s
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

interface PagesJaunesResult {
  website: string | null;
  phone: string | null;
}

async function scrapesPagesJaunes(
  companyName: string,
  city: string,
): Promise<PagesJaunesResult> {
  const nameSlug = slugify(companyName);
  const citySlug = slugify(city);
  const url = `https://www.pagesjaunes.fr/recherche/${nameSlug}/${citySlug}`;

  logger.info({ url }, 'Scraping Pages Jaunes');

  const html = await fetchHtml(url);
  if (!html) return { website: null, phone: null };

  const $ = cheerio.load(html);

  // Take the first result card
  const firstResult = $('.bi-content, .result-content, [class*="bi-"], .bi').first();

  let website: string | null = null;
  let phone: string | null = null;

  // Extract website link
  firstResult.find('a[href*="http"]').each((_, el) => {
    const href = $(el).attr('href') || '';
    if (href && !href.includes('pagesjaunes.fr') && !href.includes('google.') && !website) {
      website = href.split('?')[0];
    }
  });

  // Extract phone
  const FR_PHONE_RE = /(?:(?:\+|00)33|0)\s*[1-9](?:[\s.\-]*\d{2}){4}/;
  firstResult.find('[class*="phone"], [class*="tel"], [itemprop="telephone"]').each((_, el) => {
    const text = $(el).text().trim();
    const match = text.match(FR_PHONE_RE);
    if (match && !phone) {
      phone = match[0].replace(/[\s.\-]/g, '');
    }
  });

  // Fallback: search whole page for phone if not found
  if (!phone) {
    const pageText = $.text();
    const match = pageText.match(FR_PHONE_RE);
    if (match) phone = match[0].replace(/[\s.\-]/g, '');
  }

  return { website, phone };
}

// ── Extract root domain from URL ──────────────────────────────────────────────

function extractRootDomain(url: string): string | null {
  try {
    const u = new URL(url.startsWith('http') ? url : `https://${url}`);
    const parts = u.hostname.split('.');
    if (parts.length < 2) return null;
    return parts.slice(-2).join('.');
  } catch {
    return null;
  }
}

// ── Public export ─────────────────────────────────────────────────────────────

export interface EnrichedCompany extends CompanyRecord {
  gmapsMatch: GmapsMatch | null;
}

export async function enrichWithDomains(companies: CompanyRecord[]): Promise<CompanyRecord[]> {
  if (!gmapsCsvExists()) {
    // Safety check — should have been caught earlier, but guard here too
    logger.warn('gmaps-results.csv not found — skipping domain enrichment');
    return companies.map(c => ({ ...c, domain: null, mx_found: false, phone: null }));
  }

  const results: CompanyRecord[] = [];

  for (const company of companies) {
    const name = company.company_name?.trim();

    if (!name) {
      results.push({ ...company, domain: null, mx_found: false, phone: null });
      continue;
    }

    logger.info({ company: name }, 'Enriching with domain/phone...');

    // ── Strategy 1: Google Maps CSV match ──────────────────────────────────
    const gmaps = findGmapsMatch(company.siret, name, company.postal_code || '');

    if (gmaps) {
      logger.info(
        { company: name, confidence: gmaps.matchConfidence, website: gmaps.website, phone: gmaps.phone },
        'Gmaps match found',
      );

      const domain = gmaps.website ? extractRootDomain(gmaps.website) : null;
      results.push({
        ...company,
        domain,
        mx_found: false, // will be checked in contacts if needed
        phone: gmaps.phone,
        // Attach gmaps data for contacts step via extension
        ...(gmaps as object),
      });
      continue;
    }

    // ── Strategy 2: Pages Jaunes fallback ──────────────────────────────────
    const pj = await scrapesPagesJaunes(name, company.city || '');

    if (pj.website || pj.phone) {
      logger.info({ company: name, source: 'pagesjaunes', website: pj.website, phone: pj.phone }, 'Pages Jaunes result');
      const domain = pj.website ? extractRootDomain(pj.website) : null;
      results.push({ ...company, domain, mx_found: false, phone: pj.phone });
      continue;
    }

    // ── No result ───────────────────────────────────────────────────────────
    logger.info({ company: name }, 'No enrichment found — skipping');
    results.push({ ...company, domain: null, mx_found: false, phone: null });
  }

  const found = results.filter(r => r.domain).length;
  logger.info({ found, total: results.length }, 'Domain enrichment complete');
  return results;
}
