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

const SIRENE_DB   = path.join(DATA_DIR, 'sirene.db');
const OUTPUT_FILE = path.join(DATA_DIR, 'queries-sirene.txt');

// Possible locations for StockUniteLegale (parquet or CSV)
const UNITE_LEGALE_PARQUET = path.join(DATA_DIR, 'StockUniteLegale_utf8.parquet');
const UNITE_LEGALE_CSV     = path.join(DATA_DIR, 'StockUniteLegale_utf8.csv');

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

// ── [ND] helpers ──────────────────────────────────────────────────────────────

function isND(s: string | null | undefined): boolean {
  if (!s) return true;
  const t = s.trim();
  return t === '' || t === '[ND]';
}

function cleanField(s: string | null | undefined): string | null {
  if (isND(s)) return null;
  return s!.trim();
}

// ── Types ─────────────────────────────────────────────────────────────────────

interface SireneRow {
  siren: string;
  denominationUsuelleEtablissement: string | null;
  enseigne1Etablissement: string | null;
  postal_code: string | null;
  city: string | null;
}

function buildName(row: SireneRow): string | null {
  return cleanField(row.denominationUsuelleEtablissement)
      ?? cleanField(row.enseigne1Etablissement)
      ?? null;
}

// ── DuckDB lookup for [ND] names ──────────────────────────────────────────────
// Tries StockUniteLegale parquet (or CSV) to recover prenom+nom for sole traders.
// Returns a map of siren → "PRENOM NOM"

async function loadUniteLegaleNames(sirens: string[]): Promise<Map<string, string>> {
  const result = new Map<string, string>();
  if (sirens.length === 0) return result;

  const parquetExists = fs.existsSync(UNITE_LEGALE_PARQUET);
  const csvExists     = fs.existsSync(UNITE_LEGALE_CSV);

  if (!parquetExists && !csvExists) return result;

  try {
    // Dynamic import so the script still runs if duckdb isn't working
    const duckdb = await import('duckdb');
    const db     = new duckdb.Database(':memory:');
    const conn   = db.connect();

    const source = parquetExists
      ? `read_parquet('${UNITE_LEGALE_PARQUET.replace(/\\/g, '/')}')`
      : `read_csv_auto('${UNITE_LEGALE_CSV.replace(/\\/g, '/')}')`;

    const sirenList = sirens.map(s => `'${s}'`).join(',');

    const sql = `
      SELECT siren,
             COALESCE(NULLIF(TRIM(prenomUsuelUniteLegale), ''), NULLIF(TRIM(prenom1UniteLegale), '')) AS prenom,
             NULLIF(TRIM(nomUniteLegale), '') AS nom
      FROM ${source}
      WHERE siren IN (${sirenList})
        AND nomUniteLegale IS NOT NULL
        AND nomUniteLegale != ''
        AND nomUniteLegale != '[ND]'
    `;

    await new Promise<void>((resolve, reject) => {
      conn.all(sql, (err: Error | null, rows: Array<Record<string, string>>) => {
        if (err) { reject(err); return; }
        for (const row of rows) {
          const prenom = row['prenom']?.trim() ?? '';
          const nom    = row['nom']?.trim()    ?? '';
          if (nom) {
            result.set(row['siren'], [prenom, nom].filter(Boolean).join(' ').toUpperCase());
          }
        }
        resolve();
      });
    });

    conn.close();
    db.close();
  } catch (err) {
    // DuckDB unavailable or file unreadable — skip silently
    process.stderr.write(`[DuckDB] Skipping UniteLegale lookup: ${(err as Error).message}\n`);
  }

  return result;
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const tradeArg = getArg('--trade');
  const cityArg  = getArg('--city');

  if (!fs.existsSync(SIRENE_DB)) {
    console.error(`\n⚠️  SIRENE database not found at: ${SIRENE_DB}`);
    process.exit(1);
  }

  const db = new DatabaseSync(SIRENE_DB);

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

  // ── Collect sirens with no name for DuckDB fallback ───────────────────────
  const ndSirens = rows
    .filter(r => buildName(r) === null)
    .map(r => r.siren);

  const uniteLegaleNames = await loadUniteLegaleNames(ndSirens);

  if (uniteLegaleNames.size > 0) {
    console.log(`  [DuckDB] Recovered ${uniteLegaleNames.size} names from StockUniteLegale`);
  }

  // ── Build queries ─────────────────────────────────────────────────────────
  const queries: string[] = [];
  let skippedNoName = 0;

  const tradeLabel = tradeArg
    ? (NAF_LABEL[TRADE_NAF[tradeArg.toLowerCase()]?.[0]] ?? tradeArg)
    : 'all trades';

  for (const row of rows) {
    const siren = (row.siren || '').trim();

    // Name: SIRENE field → DuckDB fallback → skip
    let name = buildName(row);
    if (!name) {
      const fallback = uniteLegaleNames.get(siren);
      if (fallback) {
        // Format: "PRENOM NOM electricien CITY" (no postal — individual, likely no number)
        const city = cleanField(row.city) ?? '';
        const parts = [fallback, tradeLabel, city].filter(Boolean);
        queries.push(`${parts.join(' ')} #!#${siren}`);
        continue;
      }
      skippedNoName++;
      continue;
    }

    const city   = cleanField(row.city)        ?? '';
    const postal = cleanField(row.postal_code) ?? '';   // rule 2+3: [ND] → omit, don't skip

    // Format: "{NAME} {CITY} {POSTAL_CODE} #!#{SIREN}"
    const parts = [name];
    if (city)   parts.push(city);
    if (postal) parts.push(postal);

    queries.push(`${parts.join(' ')} #!#${siren}`);
  }

  fs.writeFileSync(OUTPUT_FILE, queries.join('\n'), 'utf-8');

  console.log(`\n✅ ${queries.length} queries generated (${skippedNoName} skipped — no name)`);
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

  if (!fs.existsSync(UNITE_LEGALE_PARQUET) && !fs.existsSync(UNITE_LEGALE_CSV)) {
    console.log(`💡 Tip: place StockUniteLegale_utf8.parquet in D:\\outbound-data\\ to recover`);
    console.log(`   names for the ${skippedNoName} skipped entries.`);
    console.log(`   Download: https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/\n`);
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
