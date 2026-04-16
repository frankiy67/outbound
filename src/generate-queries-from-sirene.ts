/**
 * STEP 1 — Generate search queries from SIRENE SQLite database
 *
 * Usage:
 *   npm run generate:queries -- --trade electricien --city Paris
 *   npm run generate:queries -- --trade plombier
 */

import 'dotenv/config';
import * as fs from 'fs';
import * as path from 'path';
import { DatabaseSync } from 'node:sqlite';
import { DATA_DIR } from './config';

const SIRENE_DB = path.join(DATA_DIR, 'sirene.db');
const OUTPUT_FILE = path.join(DATA_DIR, 'queries-sirene.txt');

const NAF_LABEL: Record<string, string> = {
  '43.21A': 'Électricien',
  '43.22A': 'Plombier',
  '43.22B': 'Chauffagiste',
  '43.31Z': 'Plâtrier',
  '43.32A': 'Menuisier',
  '43.32B': 'Menuisier métal',
  '43.33Z': 'Carreleur',
  '43.34Z': 'Peintre',
  '43.39Z': 'Finition',
  '43.91A': 'Charpentier',
  '43.99B': 'Maçon',
};

const TRADE_NAF: Record<string, string[]> = {
  electricien:  ['43.21A'],
  plombier:     ['43.22A'],
  chauffagiste: ['43.22B'],
  platrier:     ['43.31Z'],
  menuisier:    ['43.32A', '43.32B'],
  carreleur:    ['43.33Z'],
  peintre:      ['43.34Z'],
  macon:        ['43.99B'],
};

function getArg(flag: string): string | null {
  const idx = process.argv.indexOf(flag);
  return idx !== -1 && process.argv[idx + 1] ? process.argv[idx + 1] : null;
}

interface SireneRow {
  siren: string;
  denominationUsuelleEtablissement: string | null;
  enseigne1Etablissement: string | null;
  postal_code: string | null;
  city: string | null;
}

function buildName(row: SireneRow): string | null {
  // Priority 1: denominationUsuelleEtablissement
  if (row.denominationUsuelleEtablissement?.trim()) {
    return row.denominationUsuelleEtablissement.trim();
  }
  // Priority 2: enseigne1Etablissement
  if (row.enseigne1Etablissement?.trim()) {
    return row.enseigne1Etablissement.trim();
  }
  return null;
}

async function main(): Promise<void> {
  const tradeArg = getArg('--trade');
  const cityArg = getArg('--city');

  if (!fs.existsSync(SIRENE_DB)) {
    console.error(`\n⚠️  SIRENE database not found at: ${SIRENE_DB}`);
    process.exit(1);
  }

  const db = new DatabaseSync(SIRENE_DB);

  // Determine NAF codes to query
  let nafCodes: string[] = Object.keys(NAF_LABEL);
  if (tradeArg) {
    const key = tradeArg.toLowerCase();
    if (!TRADE_NAF[key]) {
      console.error(`Unknown trade: ${tradeArg}`);
      console.error(`Available: ${Object.keys(TRADE_NAF).join(', ')}`);
      process.exit(1);
    }
    nafCodes = TRADE_NAF[key];
  }

  const nafPlaceholders = nafCodes.map(() => '?').join(',');

  let sql = `
    SELECT
      siren,
      denominationUsuelleEtablissement,
      enseigne1Etablissement,
      codePostalEtablissement  AS postal_code,
      libelleCommuneEtablissement AS city
    FROM etablissements
    WHERE etatAdministratifEtablissement = 'A'
      AND activitePrincipaleEtablissement IN (${nafPlaceholders})
  `;

  const params: string[] = [...nafCodes];

  if (cityArg) {
    sql += ` AND LOWER(libelleCommuneEtablissement) LIKE LOWER(?)`;
    params.push(`%${cityArg}%`);
  }

  sql += ` ORDER BY siren`;

  const rows = db.prepare(sql).all(...params) as unknown as SireneRow[];
  db.close();

  const queries: string[] = [];

  for (const row of rows) {
    const name = buildName(row);
    if (!name) continue;

    const city   = (row.city   || '').trim();
    const postal = (row.postal_code || '').trim();
    const siren  = (row.siren || '').trim();

    // Format: "{NAME} {CITY} {POSTAL_CODE} #!#{SIREN}"
    // Note: SIRENE DB stores siren (9 digits). SIRET = siren + 5-digit NIC.
    // The #!# tag is used by gmaps-reader for SIRET matching; siren is the fallback.
    const parts = [name];
    if (city)   parts.push(city);
    if (postal) parts.push(postal);

    queries.push(`${parts.join(' ')} #!#${siren}`);
  }

  fs.writeFileSync(OUTPUT_FILE, queries.join('\n'), 'utf-8');

  const tradeLabel = tradeArg ? (NAF_LABEL[TRADE_NAF[tradeArg.toLowerCase()]?.[0]] ?? tradeArg) : 'all trades';
  console.log(`\n✅ Generated ${queries.length} queries (${tradeLabel}${cityArg ? ` in ${cityArg}` : ''})`);
  console.log(`📄 Output: ${OUTPUT_FILE}`);
  console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`Next step — run the scraper on your Mac:`);
  console.log(``);
  console.log(`  google-maps-scraper \\`);
  console.log(`    --input "${OUTPUT_FILE.replace(/\\/g, '/')}" \\`);
  console.log(`    --output "D:/outbound-data/gmaps-results.csv" \\`);
  console.log(`    --lang fr \\`);
  console.log(`    --depth 1`);
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
