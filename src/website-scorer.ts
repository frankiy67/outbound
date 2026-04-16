/**
 * Module 3 — Restaurant Website Opportunity Scorer
 *
 * Fetches a restaurant's website and scores how much it needs a redesign.
 * Signals: no_website, not_mobile, no_ssl, old_builder, slow_site,
 *          no_booking, old_copyright, low_rating, low_reviews.
 *
 * Tiers: hot ≥ 60 | warm 35–59 | cold < 35
 */

import fetch from 'node-fetch';
import pino from 'pino';

const logger = pino({ level: 'info' });

export interface ScoreResult {
  score: number;
  tier: 'hot' | 'warm' | 'cold';
  signals: string[];
}

// Fingerprints for old/cheap website builders.
const OLD_BUILDER_PATTERNS = [
  /jimdo\.com/i,
  /wixsite\.com|wix\.com|wixstatic\.com/i,
  /e-monsite\.com/i,
  /webnode\./i,
  /weebly\.com/i,
  /ovh-websites\.com|ovhcloud\.com\/fr\/web\/site-creator/i,
  /1and1\.com|ionos\.com/i,
  /sitebuilder\./i,
  /site123\.com/i,
  /strikingly\.com/i,
];

// Booking-related keywords indicating an online reservation system.
const BOOKING_PATTERNS = [
  /r[eé]server/i,
  /r[eé]servation/i,
  /booking\./i,
  /opentable/i,
  /thefork/i,
  /lafourchette/i,
  /resy\./i,
  /zenchef/i,
  /guestonline/i,
  /mapstr/i,
  /quandoo/i,
  /sevenrooms/i,
];

const CURRENT_YEAR = new Date().getFullYear();
const FETCH_TIMEOUT_MS = 8000;
const SLOW_THRESHOLD_MS = 3000;

async function fetchWithTiming(
  url: string,
): Promise<{ html: string | null; ms: number }> {
  const t0 = Date.now();
  try {
    const res = await fetch(url, {
      headers: {
        // Simulate a mobile browser so viewport detection is meaningful.
        'User-Agent':
          'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15',
        'Accept-Language': 'fr-FR,fr;q=0.9',
        Accept: 'text/html,application/xhtml+xml',
      },
      timeout: FETCH_TIMEOUT_MS,
    } as Parameters<typeof fetch>[1]);
    const ms = Date.now() - t0;
    if (!res.ok) return { html: null, ms };
    const html = await res.text();
    return { html, ms };
  } catch {
    return { html: null, ms: Date.now() - t0 };
  }
}

function calcTier(score: number): 'hot' | 'warm' | 'cold' {
  if (score >= 55) return 'hot';
  if (score >= 35) return 'warm';
  return 'cold';
}

export async function scoreRestaurantWebsite(
  website: string | null,
  rating: number | null,
  userRatingsTotal: number | null,
): Promise<ScoreResult> {
  const signals: string[] = [];
  let score = 0;

  // ── Rating signals (independent of website) ──────────────────────────────
  if (rating !== null && rating < 3.5) {
    signals.push('low_rating');
    score += 5;
  }
  if (userRatingsTotal !== null && userRatingsTotal < 50) {
    signals.push('low_reviews');
    score += 5;
  }

  // ── No website at all ────────────────────────────────────────────────────
  if (!website) {
    signals.push('no_website');
    score += 50;
    logger.debug({ score, signals }, 'No website — scored early');
    return { score, tier: calcTier(score), signals };
  }

  // ── SSL check ────────────────────────────────────────────────────────────
  const isHttp = /^http:\/\//i.test(website);
  if (isHttp) {
    signals.push('no_ssl');
    score += 15;
  }

  // ── Fetch page ───────────────────────────────────────────────────────────
  // Try HTTPS first; if the original URL was HTTP and HTTPS fails, fall back.
  const fetchUrl = isHttp
    ? website.replace(/^http:\/\//i, 'https://')
    : website;

  let { html, ms } = await fetchWithTiming(fetchUrl);

  if (!html && isHttp) {
    // HTTPS failed — retry with original HTTP URL.
    const fallback = await fetchWithTiming(website);
    html = fallback.html;
    ms   = fallback.ms;
  }

  if (!html) {
    // Site unreachable — return what we have so far.
    logger.debug({ website }, 'Could not fetch website');
    return { score, tier: calcTier(score), signals };
  }

  // ── Speed signal ─────────────────────────────────────────────────────────
  if (ms > SLOW_THRESHOLD_MS) {
    signals.push('slow_site');
    score += 10;
  }

  // ── Mobile-friendliness (viewport meta tag) ───────────────────────────────
  if (!/meta[^>]+name=["']viewport["']/i.test(html)) {
    signals.push('not_mobile');
    score += 20;
  }

  // ── Old builder detection ────────────────────────────────────────────────
  if (OLD_BUILDER_PATTERNS.some(re => re.test(html!))) {
    signals.push('old_builder');
    score += 10;
  }

  // ── No online booking ─────────────────────────────────────────────────────
  if (!BOOKING_PATTERNS.some(re => re.test(html!))) {
    signals.push('no_booking');
    score += 10;
  }

  // ── Old copyright year ────────────────────────────────────────────────────
  const copyrightMatch = html.match(/©\s*(\d{4})|copyright\s+(\d{4})/i);
  if (copyrightMatch) {
    const year = parseInt(copyrightMatch[1] ?? copyrightMatch[2], 10);
    if (year <= CURRENT_YEAR - 2) {
      signals.push('old_copyright');
      score += 10;
    }
  }

  logger.debug({ website, score, signals, ms }, 'Website scored');
  return { score, tier: calcTier(score), signals };
}
