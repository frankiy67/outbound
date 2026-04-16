/**
 * Module 5 — Sending
 * Gmail SMTP via nodemailer, SQLite queue, node-cron scheduler.
 */

import * as path from 'path';
import * as fs from 'fs';
import * as nodemailer from 'nodemailer';
import { DatabaseSync } from 'node:sqlite';
import cron from 'node-cron';
import pino from 'pino';
import { ScoredLead, QueueEntry } from './types';
import { DATA_DIR } from './config';

const logger = pino({ level: 'info' });
const QUEUE_DB_PATH = path.join(DATA_DIR, 'queue.db');

// --- Sequence config ---
const SEQUENCE_STEPS: Record<ScoredLead['tier'], number[]> = {
  tier_1: [0, 3, 7],
  tier_2: [0, 5],
  nurture: [0],
};

// --- Database ---
function openQueueDb(): DatabaseSync {
  const db = new DatabaseSync(QUEUE_DB_PATH);
  db.exec(`
    CREATE TABLE IF NOT EXISTS leads_queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_email TEXT NOT NULL,
      lead_first_name TEXT,
      lead_company TEXT NOT NULL,
      subject TEXT NOT NULL,
      body TEXT NOT NULL,
      step INTEGER NOT NULL DEFAULT 0,
      scheduled_date TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TEXT NOT NULL,
      sent_at TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_email_step
      ON leads_queue(lead_email, step);
  `);
  return db;
}

// --- Email drafting ---

function selectAngle(signals: string[]): string {
  if (signals.includes('naf_6920z')) return 'compliance_burden';
  if (signals.includes('small_team')) return 'founder_direct';
  if (signals.includes('mid_team')) return 'process_scale';
  if (signals.includes('generic_email')) return 'generic_forward';
  return 'mid_team';
}

function draftEmail(lead: ScoredLead, step: number): { subject: string; body: string } {
  const angle = selectAngle(lead.signals_found);
  const firstName = lead.first_name ?? 'vous';
  const company = lead.company_name;
  const city = lead.city;

  const templates: Record<string, (step: number) => { subject: string; body: string }> = {
    compliance_burden: (s) => ({
      subject: s === 0
        ? `question sur la charge admin chez ${company}`
        : s === 1
        ? `suite — ${company}`
        : `dernière tentative — ${company}`,
      body: s === 0
        ? `Bonjour ${firstName},\n\nLes cabinets comptables comme ${company} passent souvent trop de temps sur des tâches administratives répétitives.\n\nOn aide à automatiser ça sans rien changer à votre stack actuelle.\n\nEst-ce que c'est un sujet chez vous en ce moment ?`
        : s === 1
        ? `Bonjour ${firstName},\n\nJe me permets de revenir — je n'ai pas eu de retour.\n\nUne seule question : la charge admin est-elle un frein à ${company} ?`
        : `Bonjour ${firstName},\n\nDernier message de ma part.\n\nSi ce n'est pas le bon moment, pas de problème. Bonne continuation à ${company}.`,
    }),
    founder_direct: (s) => ({
      subject: s === 0
        ? `idée rapide pour ${company}`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour ${firstName},\n\nJe serai bref : on aide les équipes de moins de 20 personnes à gagner du temps sur la partie opérationnelle.\n\nÇa parle à ${company} ?`
        : `Bonjour ${firstName},\n\nJuste une relance — est-ce que le timing était mauvais ?`,
    }),
    process_scale: (s) => ({
      subject: s === 0
        ? `croissance et friction à ${city}`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour ${firstName},\n\nQuand une équipe comme ${company} grandit, la friction opérationnelle grandit avec elle.\n\nOn aide à la réduire concrètement.\n\nEst-ce que c'est quelque chose que vous ressentez en ce moment ?`
        : `Bonjour ${firstName},\n\nPas de retour — je suppose que le timing n'était pas idéal.\n\nToujours disponible si ça devient pertinent.`,
    }),
    generic_forward: (s) => ({
      subject: s === 0
        ? `pour le gérant de ${company}`
        : `suite — ${company}`,
      body: s === 0
        ? `Bonjour,\n\nCe message est destiné au responsable de ${company}.\n\nOn aide les entreprises à ${city} à réduire leurs tâches répétitives.\n\nEst-ce le bon interlocuteur ?`
        : `Bonjour,\n\nSuite à mon précédent message — est-ce que quelqu'un chez ${company} est en charge de ce sujet ?`,
    }),
  };

  const gen = templates[angle] ?? templates['process_scale'];
  return gen(step);
}

// --- Enqueue ---
export function enqueueLeads(leads: ScoredLead[]): void {
  const db = openQueueDb();
  const insert = db.prepare(`
    INSERT OR IGNORE INTO leads_queue
      (lead_email, lead_first_name, lead_company, subject, body, step, scheduled_date, status, created_at, sent_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL)
  `);

  const beginTx = db.prepare('BEGIN');
  const commitTx = db.prepare('COMMIT');

  beginTx.run();
  for (const lead of leads) {
    if (!lead.email) continue;
    const steps = SEQUENCE_STEPS[lead.tier];
    const today = new Date();

    for (const dayOffset of steps) {
      const scheduledDate = new Date(today);
      scheduledDate.setDate(today.getDate() + dayOffset);
      const dateStr = scheduledDate.toISOString().split('T')[0];
      const draft = draftEmail(lead, steps.indexOf(dayOffset));
      insert.run(
        lead.email,
        lead.first_name,
        lead.company_name,
        draft.subject,
        draft.body,
        steps.indexOf(dayOffset),
        dateStr,
        new Date().toISOString()
      );
    }
  }
  commitTx.run();
  db.close();
  logger.info('Leads enqueued successfully');
}

// --- Send ---
function createTransporter(): nodemailer.Transporter {
  const user = process.env['GMAIL_USER'];
  const pass = process.env['GMAIL_APP_PASSWORD'];

  if (!user || !pass) {
    throw new Error('GMAIL_USER and GMAIL_APP_PASSWORD must be set in .env');
  }

  return nodemailer.createTransport({
    service: 'gmail',
    auth: { user, pass },
  });
}

async function sendPendingEmails(): Promise<void> {
  const db = openQueueDb();
  const today = new Date().toISOString().split('T')[0];

  const pending = db.prepare(`
    SELECT * FROM leads_queue
    WHERE scheduled_date <= ? AND status = 'pending'
    ORDER BY scheduled_date ASC
  `).all(today) as unknown as QueueEntry[];

  if (pending.length === 0) {
    logger.info('No emails to send today');
    db.close();
    return;
  }

  const transporter = createTransporter();
  const updateStmt = db.prepare(`
    UPDATE leads_queue SET status = ?, sent_at = ? WHERE id = ?
  `);

  for (const entry of pending) {
    try {
      await transporter.sendMail({
        from: process.env['GMAIL_USER'],
        to: entry.lead_email,
        subject: entry.subject,
        text: entry.body,
      });

      updateStmt.run('sent', new Date().toISOString(), entry.id ?? 0);
      logger.info({ to: entry.lead_email, step: entry.step }, 'Email sent');
    } catch (err) {
      updateStmt.run('failed', new Date().toISOString(), entry.id ?? 0);
      logger.error({ err, to: entry.lead_email }, 'Email send failed');
    }

    // Respect Gmail rate limits
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

  db.close();
}

// --- Scheduler ---
export function startScheduler(): void {
  logger.info('Email scheduler started — runs daily at 08:00');
  cron.schedule('0 8 * * *', async () => {
    logger.info('Running scheduled email send...');
    await sendPendingEmails();
  });
}

// --- Preview ---
export function writePreview(leads: ScoredLead[]): void {
  console.log(`writePreview received ${leads.length} leads`);
  if (leads.length > 0) {
    console.log('First lead keys:', Object.keys(leads[0]));
    console.log('First lead company_name:', leads[0].company_name);
    console.log('First lead domain:', leads[0].domain);
    console.log('First lead phone:', leads[0].phone);
  }
  const withName = leads.filter(lead => lead.company_name);
  console.log(`  → ${withName.length}/${leads.length} passed company_name filter`);
  const preview = withName
    .map(lead => ({
      company: lead.company_name,
      city: lead.city,
      domain: lead.domain,
      phone: lead.phone,
      email: lead.email,
      email_status: lead.email_status,
      score: lead.score,
      tier: lead.tier,
      signals: lead.signals_found,
      draft_emails: SEQUENCE_STEPS[lead.tier].map((_, i) => draftEmail(lead, i)),
    }));

  const outPath = path.join(DATA_DIR, 'preview.json');
  fs.writeFileSync(outPath, JSON.stringify(preview, null, 2));
  logger.info({ path: outPath }, 'Preview written');
}
