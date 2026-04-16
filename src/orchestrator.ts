/**
 * Entry point — parses natural language query, runs the full pipeline.
 *
 * Usage:
 *   ts-node src/orchestrator.ts "Find 100 accounting firms in Paris, 10-50 employees, get the CEO --go"
 */

import 'dotenv/config';
import * as path from 'path';
import pino from 'pino';
import { sourceCompanies } from './sourcing';
import { enrichWithDomains } from './domains';
import { enrichWithContacts } from './contacts';
import { scoreLeads } from './scoring';
import { enqueueLeads, writePreview, startScheduler } from './sender';
import { ParsedQuery } from './types';
import { DATA_DIR } from './config';

const logger = pino({ level: 'info' });

// NAF code lookup for common industries
const INDUSTRY_NAF_MAP: Record<string, string[]> = {
  'accounting': ['69.20Z'],
  'comptable': ['69.20Z'],
  'cabinet comptable': ['69.20Z'],
  'legal': ['69.10Z'],
  'juridique': ['69.10Z'],
  'conseil': ['70.22Z', '74.90B'],
  'consulting': ['70.22Z', '74.90B'],
  'architecture': ['71.11Z'],
  'marketing': ['73.11Z', '73.12Z'],
  'it': ['62.01Z', '62.02A'],
  'informatique': ['62.01Z', '62.02A'],
  'real estate': ['68.31Z'],
  'immobilier': ['68.31Z'],
  'medecin': ['86.21Z'],
  'dentiste': ['86.23Z'],
};

function parseHeadcount(text: string): { min: number; max: number } {
  const match = text.match(/(\d+)\s*[–\-–]\s*(\d+)/);
  if (match) {
    return { min: parseInt(match[1], 10), max: parseInt(match[2], 10) };
  }
  const single = text.match(/(\d+)\+/);
  if (single) {
    return { min: parseInt(single[1], 10), max: 10000 };
  }
  return { min: 10, max: 50 };
}

function parseQuery(raw: string): ParsedQuery {
  const lower = raw.toLowerCase();
  const flag_go = /--go/.test(raw);

  // City: after "in " or "à "
  const cityMatch = lower.match(/\bin\s+([a-zéèêëàâùûüîïôçœ -]+?)(?:,|\.|--|\d|$)/) ||
                    lower.match(/\bà\s+([a-zéèêëàâùûüîïôçœ -]+?)(?:,|\.|--|\d|$)/);
  const city = cityMatch ? cityMatch[1].trim() : 'Paris';

  // Headcount
  const hcMatch = lower.match(/(\d+)\s*[–\-–]\s*(\d+)\s*(?:employees?|employés?|salariés?)?/);
  const { min, max } = hcMatch
    ? { min: parseInt(hcMatch[1], 10), max: parseInt(hcMatch[2], 10) }
    : parseHeadcount(lower);

  // Industry keyword
  let industryKeyword = 'accounting';
  let naf_codes: string[] = ['69.20Z'];
  for (const [key, codes] of Object.entries(INDUSTRY_NAF_MAP)) {
    if (lower.includes(key)) {
      industryKeyword = key;
      naf_codes = codes;
      break;
    }
  }

  // Title
  const titleMatch = raw.match(/(?:get|find|contact)\s+the\s+([^,.\n--]+)/i) ||
                     raw.match(/(?:CEO|CFO|CTO|DG|gérant|directeur|associé|président)/i);
  const target_title = titleMatch ? titleMatch[0].replace(/^(get|find|contact)\s+the\s+/i, '').trim() : 'CEO';

  // Limit
  const limitMatch = lower.match(/find\s+(\d+)/);
  const limit = limitMatch ? parseInt(limitMatch[1], 10) : 100;

  return { industry_keyword: industryKeyword, city, headcount_min: min, headcount_max: max, target_title, naf_codes, flag_go, limit };
}

function printSummaryTable(leads: ReturnType<typeof scoreLeads> extends Promise<infer T> ? T : never): void {
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  OUTBOUND PIPELINE — RESULTS SUMMARY');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`${'Company'.padEnd(35)} ${'Email'.padEnd(35)} ${'Score'.padEnd(6)} ${'Tier'.padEnd(8)} Signals`);
  console.log('─'.repeat(100));

  for (const lead of leads) {
    const name    = (lead.company_name ?? 'Unknown').substring(0, 34).padEnd(35);
    const email   = (lead.email ?? '—').substring(0, 34).padEnd(35);
    const signals = lead.signals_found.join(', ');
    console.log(`${name} ${email} ${String(lead.score).padEnd(6)} ${lead.tier.padEnd(8)} ${signals}`);
  }

  const tier1 = leads.filter(l => l.tier === 'tier_1').length;
  const tier2 = leads.filter(l => l.tier === 'tier_2').length;
  const nurture = leads.filter(l => l.tier === 'nurture').length;

  console.log('─'.repeat(100));
  console.log(`Total: ${leads.length} leads  |  Tier 1: ${tier1}  |  Tier 2: ${tier2}  |  Nurture: ${nurture}`);
  console.log('═══════════════════════════════════════════════════════════════\n');
}

async function main(): Promise<void> {
  const rawInput = process.argv.slice(2).join(' ');
  if (!rawInput) {
    console.error('Usage: ts-node src/orchestrator.ts "<query> [--go]"');
    console.error('Example: ts-node src/orchestrator.ts "Find 100 accounting firms in Paris, 10-50 employees --go"');
    process.exit(1);
  }

  const query = parseQuery(rawInput);
  logger.info({ query }, 'Parsed query');

  console.log('\nRunning zero-cost outbound pipeline...');
  console.log(`Industry: ${query.industry_keyword} | City: ${query.city} | Headcount: ${query.headcount_min}–${query.headcount_max}`);
  console.log(`NAF codes: ${query.naf_codes.join(', ')} | Mode: ${query.flag_go ? 'SEND' : 'DRY RUN'}\n`);

  // Module 1: Source companies
  console.log('[1/4] Sourcing companies from SIRENE...');
  const companies = await sourceCompanies({
    naf_codes: query.naf_codes,
    city: query.city,
    headcount_min: query.headcount_min,
    headcount_max: query.headcount_max,
    limit: query.limit,
  });
  console.log(`      → ${companies.length} companies found\n`);

  // Module 2: Domain discovery
  console.log('[2/4] Discovering domains...');
  const withDomains = await enrichWithDomains(companies);
  const domainsFound = withDomains.filter(c => c.domain).length;
  console.log(`      → ${domainsFound}/${companies.length} domains found\n`);

  // Module 3: Contact finding
  console.log('[3/4] Finding contacts + SMTP verification...');
  const withContacts = await enrichWithContacts(withDomains);
  const emailsFound = withContacts.filter(c => c.email).length;
  const verified = withContacts.filter(c => c.email_status === 'verified').length;
  console.log(`      → ${emailsFound} emails found, ${verified} SMTP-verified\n`);

  // Module 4: Scoring
  console.log('[4/4] Scoring leads...');
  const scoredLeads = await scoreLeads(withContacts, {
    naf_codes: query.naf_codes,
    headcount_min: query.headcount_min,
    headcount_max: query.headcount_max,
  });
  console.log('      → Scoring complete\n');

  // Print summary
  printSummaryTable(scoredLeads);

  if (!query.flag_go) {
    // Dry run: write preview
    console.log(`Writing ${scoredLeads.length} leads to preview.json`);
    if (scoredLeads.length === 0) {
      console.log('WARNING: scoredLeads is empty — checking upstream counts:');
      console.log('  companies sourced:', companies.length);
      console.log('  after domain enrichment:', withDomains.length, '| with domain:', withDomains.filter(c => c.domain).length);
      console.log('  after contact enrichment:', withContacts.length, '| with email:', withContacts.filter(c => c.email).length);
    } else {
      console.log('Sample lead:', JSON.stringify(scoredLeads[0], null, 2));
    }
    writePreview(scoredLeads);
    console.log(`DRY RUN — No emails sent. Preview written to ${path.join(DATA_DIR, 'preview.json')}`);
    console.log('Add --go to your query to activate sending.\n');
  } else {
    // Enqueue and start scheduler
    enqueueLeads(scoredLeads);
    console.log(`LIVE MODE — ${scoredLeads.filter(l => l.email).length} leads queued for sending.`);
    console.log('Scheduler started. Emails will send daily at 08:00.\n');
    startScheduler();
  }
}

main().catch(err => {
  logger.error(err, 'Pipeline failed');
  process.exit(1);
});
