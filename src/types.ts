export interface CompanyRecord {
  siren: string;
  siret: string;
  company_name: string;
  city: string;
  postal_code: string;
  naf_code: string;
  headcount_tranche: string;
  domain: string | null;
  mx_found: boolean;
  phone: string | null;
}

export interface ContactRecord extends CompanyRecord {
  first_name: string | null;
  last_name: string | null;
  email: string | null;
  email_status: 'verified' | 'probable' | 'unverified' | 'invalid' | 'generic' | 'unknown';
  confidence: number;
}

export interface ScoredLead extends ContactRecord {
  score: number;
  tier: 'tier_1' | 'tier_2' | 'nurture';
  signals_found: string[];
}

export interface QueueEntry {
  id?: number;
  lead_email: string;
  lead_first_name: string | null;
  lead_company: string;
  subject: string;
  body: string;
  step: number;
  scheduled_date: string;
  status: 'pending' | 'sent' | 'failed';
  created_at: string;
  sent_at: string | null;
}

export interface ParsedQuery {
  industry_keyword: string;
  city: string;
  headcount_min: number;
  headcount_max: number;
  target_title: string;
  naf_codes: string[];
  flag_go: boolean;
  limit: number;
}
