/**
 * Module 4 — Lead Scoring
 * Scores each lead 0–100 and assigns a tier.
 */

import * as dns from 'dns';
import pino from 'pino';
import { ContactRecord, ScoredLead } from './types';

const logger = pino({ level: 'info' });

async function hasSpfRecord(domain: string): Promise<boolean> {
  try {
    const records = await dns.promises.resolveTxt(domain);
    return records.some(r => r.join('').includes('v=spf1'));
  } catch {
    return false;
  }
}

async function hasDkimRecord(domain: string): Promise<boolean> {
  try {
    const records = await dns.promises.resolveTxt(`_domainkey.${domain}`);
    return records.length > 0;
  } catch {
    return false;
  }
}

export async function scoreLeads(contacts: ContactRecord[], params: {
  naf_codes: string[];
  headcount_min: number;
  headcount_max: number;
}): Promise<ScoredLead[]> {
  const results: ScoredLead[] = [];

  for (const contact of contacts) {
    const signals: string[] = [];
    let score = 0;

    // --- ICP fit (45 pts max) ---
    if (params.naf_codes.includes(contact.naf_code)) {
      score += 25;
      signals.push(`naf_${contact.naf_code.replace('.', '').toLowerCase()}`);
    }

    const trancheMap: Record<string, { min: number; max: number }> = {
      '11': { min: 10, max: 19 },
      '12': { min: 20, max: 49 },
    };
    const tranche = trancheMap[contact.headcount_tranche];
    if (tranche && tranche.min >= params.headcount_min && tranche.max <= params.headcount_max) {
      score += 20;
      if (contact.headcount_tranche === '11') signals.push('small_team');
      else if (contact.headcount_tranche === '12') signals.push('mid_team');
    }

    // --- Contact quality (35 pts max) ---
    if (contact.email) {
      score += 20;
      if (contact.email_status === 'verified') {
        score += 15;
        signals.push('smtp_250');
      } else if (contact.email_status === 'probable') {
        score += 8;
        signals.push('smtp_probable');
      } else if (contact.email_status === 'unverified') {
        score += 5;
        signals.push('smtp_unverified');
      } else if (contact.email_status === 'generic') {
        signals.push('generic_email');
      }
    }

    // --- Domain health (20 pts max) ---
    if (contact.domain) {
      if (contact.mx_found) {
        score += 10;
      }

      const spf = await hasSpfRecord(contact.domain);
      if (spf) {
        score += 5;
        signals.push('spf_present');
      } else {
        signals.push('no_spf');
      }

      const dkim = await hasDkimRecord(contact.domain);
      if (dkim) {
        score += 5;
        signals.push('dkim_present');
      }
    }

    // societe.com source signal
    if (contact.confidence >= 90) {
      signals.push('societe_com_found');
    }

    const tier: ScoredLead['tier'] =
      score >= 75 ? 'tier_1' :
      score >= 50 ? 'tier_2' :
      'nurture';

    logger.info({ company: contact.company_name, score, tier }, 'Lead scored');

    results.push({ ...contact, score, tier, signals_found: signals });
  }

  return results;
}
