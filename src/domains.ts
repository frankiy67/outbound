/**
 * Module 2 — Domain Discovery
 * Step 1: DNS pattern guess (instant, accurate for well-known names).
 * Step 2: Google Maps Places API (two queries: name+context, then postal+context).
 */

import * as dns from 'dns';
import fetch from 'node-fetch';
import pino from 'pino';
import { CompanyRecord } from './types';

const logger = pino({ level: 'info' });

function extractRootDomain(url: string): string | null {
  try {
    const u = new URL(url);
    const parts = u.hostname.split('.');
    if (parts.length < 2) return null;
    return parts.slice(-2).join('.');
  } catch {
    return null;
  }
}

// ── Step 1: DNS pattern guessing ─────────────────────────────────────────────

function normalizeName(name: string): string {
  return name
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]/g, '');
}

function hyphenName(name: string): string {
  return name
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function generateDomainPatterns(companyName: string): string[] {
  const slug = normalizeName(companyName);
  const hyph = hyphenName(companyName);
  return [...new Set([`${slug}.fr`, `${slug}.com`, `${hyph}.fr`, `${hyph}.com`])];
}

async function dnsResolves(domain: string): Promise<boolean> {
  try {
    await dns.promises.resolve(domain);
    return true;
  } catch {
    return false;
  }
}

async function hasMxRecord(domain: string): Promise<boolean> {
  try {
    const records = await dns.promises.resolveMx(domain);
    return records.length > 0;
  } catch {
    return false;
  }
}

async function findDomainByDns(companyName: string): Promise<string | null> {
  for (const domain of generateDomainPatterns(companyName)) {
    if (await dnsResolves(domain)) return domain;
  }
  return null;
}

// ── Step 2: Google Maps Places API ───────────────────────────────────────────

interface PlacesSearchResult {
  place_id: string;
  name: string;
  formatted_address: string;
}

interface PlacesDetails {
  website: string | null;
  phone: string | null;
}

async function placesTextSearch(
  apiKey: string,
  query: string,
  verbose: boolean,
): Promise<PlacesSearchResult | null> {
  const url =
    `https://maps.googleapis.com/maps/api/place/textsearch/json` +
    `?query=${encodeURIComponent(query)}&key=${apiKey}`;
  try {
    const res = await fetch(url);
    if (!res.ok) {
      logger.warn({ status: res.status, query }, 'Places text search non-200');
      return null;
    }
    const data = await res.json() as {
      status: string;
      results?: Array<{ place_id: string; name: string; formatted_address: string }>;
    };
    if (verbose) {
      logger.info({
        query,
        status: data.status,
        hits: data.results?.length ?? 0,
        first: data.results?.[0]
          ? { name: data.results[0].name, address: data.results[0].formatted_address }
          : null,
      }, 'Places text search — diagnostic');
    }
    if (data.status !== 'OK' || !data.results?.length) return null;
    return data.results[0];
  } catch (err) {
    logger.warn({ err, query }, 'Places text search failed');
    return null;
  }
}

async function placesDetails(apiKey: string, placeId: string): Promise<PlacesDetails> {
  const url =
    `https://maps.googleapis.com/maps/api/place/details/json` +
    `?place_id=${placeId}` +
    `&fields=name,website,formatted_phone_number,formatted_address` +
    `&key=${apiKey}`;
  try {
    const res = await fetch(url);
    if (!res.ok) {
      logger.warn({ status: res.status, placeId }, 'Places details non-200');
      return { website: null, phone: null };
    }
    const data = await res.json() as {
      status: string;
      result?: { name?: string; website?: string; formatted_phone_number?: string; formatted_address?: string };
    };
    if (data.status !== 'OK' || !data.result) return { website: null, phone: null };
    return {
      website: data.result.website ?? null,
      phone: data.result.formatted_phone_number ?? null,
    };
  } catch (err) {
    logger.warn({ err, placeId }, 'Places details fetch failed');
    return { website: null, phone: null };
  }
}

async function findDomainByPlaces(
  apiKey: string,
  companyName: string,
  city: string,
  postalCode: string,
  verbose: boolean,
): Promise<{ domain: string | null; phone: string | null }> {
  // Query 1: company name + profession + city (best match for known names).
  const q1 = `${companyName} expert comptable ${city}`;
  let hit = await placesTextSearch(apiKey, q1, verbose);

  // Query 2: fallback without company name — postal code narrows to the right arrondissement.
  if (!hit) {
    const q2 = `expert comptable ${postalCode} ${city}`;
    hit = await placesTextSearch(apiKey, q2, verbose);
  }

  if (!hit) return { domain: null, phone: null };

  const details = await placesDetails(apiKey, hit.place_id);
  const domain = details.website ? extractRootDomain(details.website) : null;
  return { domain, phone: details.phone };
}

// ── Phone scraping helpers ────────────────────────────────────────────────────

// Standard French phone regex: matches +33, 0033, or 0X formats with any separators.
const FR_PHONE_RE = /(?:(?:\+|00)33|0)\s*[1-9](?:[\s.\-]*\d{2}){4}/g;

function extractPhone(html: string): string | null {
  const matches = html.match(FR_PHONE_RE);
  if (!matches) return null;
  // Clean the first match: strip spaces, dots, dashes.
  return matches[0].replace(/[\s.\-]/g, '');
}

async function fetchText(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept-Language': 'fr-FR,fr;q=0.9' },
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  }
}

// Step 2 — Scrape company website homepage + /contact + /nous-contacter.
async function findPhoneOnWebsite(domain: string): Promise<string | null> {
  const base = `https://${domain}`;
  const pages = [base, `${base}/contact`, `${base}/nous-contacter`];

  for (const url of pages) {
    const html = await fetchText(url);
    if (!html) continue;
    const phone = extractPhone(html);
    if (phone) {
      logger.info({ domain, url, phone }, 'Phone found on website');
      return phone;
    }
  }
  return null;
}

// Step 3 — Scrape societe.com which publicly lists the phone for most French companies.
async function findPhoneOnSocieteCom(siren: string): Promise<string | null> {
  const url = `https://www.societe.com/societe/x-${siren}.html`;
  const html = await fetchText(url);
  if (!html) return null;
  const phone = extractPhone(html);
  if (phone) logger.info({ siren, phone }, 'Phone found on societe.com');
  return phone;
}

// ── Public export ────────────────────────────────────────────────────────────

export async function enrichWithDomains(companies: CompanyRecord[]): Promise<CompanyRecord[]> {
  const apiKey = process.env['GOOGLE_MAPS_API_KEY'] ?? '';
  if (!apiKey) {
    logger.error('GOOGLE_MAPS_API_KEY not set — Places API step will be skipped');
  }

  const results: CompanyRecord[] = [];
  let processed = 0;

  for (const company of companies) {
    const name = company.company_name?.trim();

    if (!name) {
      logger.info({ siren: company.siren }, 'Skipping — no company name');
      results.push({ ...company, domain: null, mx_found: false, phone: null });
      continue;
    }

    // Verbose logging for the first 3 companies to inspect raw Places responses.
    const verbose = processed < 3;
    processed++;

    logger.info({ company: name }, 'Finding domain...');
    let domain: string | null = null;
    let phone: string | null = null;
    let method = '';

    // Step 1 — DNS pattern guess (instant).
    domain = await findDomainByDns(name);
    if (domain) method = 'dns';

    // Step 2 — Google Maps Places API.
    if (!domain && apiKey) {
      const result = await findDomainByPlaces(
        apiKey, name, company.city,
        company.postal_code || company.city,
        verbose,
      );
      phone = result.phone;
      if (result.domain) { domain = result.domain; method = 'places_api'; }
    }

    // MX verification.
    let mx_found = false;
    if (domain) {
      mx_found = await hasMxRecord(domain);
      if (!mx_found) {
        logger.info({ domain }, 'No MX record, discarding domain');
        domain = null;
      }
    }

    if (domain) {
      logger.info({ company: name, domain, phone: phone ?? 'none', method }, 'Domain found');
    } else {
      logger.info({ company: name }, 'No domain found');
    }

    // Step 2 — Website scraping for phone (runs if Places API gave no phone but domain is known).
    if (!phone && domain) {
      phone = await findPhoneOnWebsite(domain);
    }

    // Step 3 — Societe.com fallback for phone (runs if website scrape also failed).
    if (!phone) {
      phone = await findPhoneOnSocieteCom(company.siren);
    }

    results.push({ ...company, domain, mx_found, phone });
  }

  const found = results.filter(r => r.domain).length;
  logger.info({ found, total: results.length }, 'Domain enrichment complete');
  return results;
}
