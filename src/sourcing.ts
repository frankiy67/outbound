/**
 * Module 1 — Company Sourcing
 * Downloads SIRENE open dataset, indexes into SQLite, queries by NAF/city/headcount.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as https from 'https';
import * as http from 'http';
import * as unzipper from 'unzipper';
import { DatabaseSync } from 'node:sqlite';
import * as readline from 'readline';
import pino from 'pino';
import { CompanyRecord } from './types';
import { DATA_DIR } from './config';

const logger = pino({ level: 'info' });

const SIRENE_DB_PATH = path.join(DATA_DIR, 'sirene.db');
const SIRENE_ZIP_URL = 'https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockEtablissement_utf8.zip';
const MIN_ZIP_BYTES = 100 * 1024 * 1024; // 100 MB

// ZIP end-of-central-directory signature (last 22 bytes of a valid zip start with this).
const EOCD_SIGNATURE = Buffer.from([0x50, 0x4b, 0x05, 0x06]);

// Headcount tranche codes
const TRANCHE_MAP: Record<string, { min: number; max: number }> = {
  '00': { min: 0, max: 0 },
  '01': { min: 1, max: 2 },
  '02': { min: 3, max: 5 },
  '03': { min: 6, max: 9 },
  '11': { min: 10, max: 19 },
  '12': { min: 20, max: 49 },
  '21': { min: 50, max: 99 },
  '22': { min: 100, max: 199 },
  '31': { min: 200, max: 249 },
  '32': { min: 250, max: 499 },
  '41': { min: 500, max: 999 },
  '42': { min: 1000, max: 1999 },
  '51': { min: 2000, max: 4999 },
  '52': { min: 5000, max: 9999 },
  '53': { min: 10000, max: Infinity },
};

// Read the last 22 bytes of a file and verify the ZIP end-of-central-directory signature.
function isValidZip(filePath: string): boolean {
  try {
    const { size } = fs.statSync(filePath);
    if (size < 22) return false;
    const buf = Buffer.alloc(22);
    const fd = fs.openSync(filePath, 'r');
    fs.readSync(fd, buf, 0, 22, size - 22);
    fs.closeSync(fd);
    return buf.slice(0, 4).equals(EOCD_SIGNATURE);
  } catch {
    return false;
  }
}


function downloadFile(url: string, dest: string): Promise<void> {
  return new Promise((resolve, reject) => {
    logger.info({ url, dest }, 'Starting download...');
    const file = fs.createWriteStream(dest);
    let bytesReceived = 0;

    const request = (u: string, redirectCount = 0) => {
      if (redirectCount > 10) { reject(new Error('Too many redirects')); return; }
      const proto = u.startsWith('https') ? https : http;
      proto.get(u, (res) => {
        logger.info(
          { status: res.statusCode, contentType: res.headers['content-type'], contentLength: res.headers['content-length'] },
          'HTTP response headers'
        );

        if (res.statusCode === 301 || res.statusCode === 302 || res.statusCode === 307 || res.statusCode === 308) {
          const location = res.headers.location;
          if (!location) { reject(new Error('Redirect with no Location header')); return; }
          // Resolve relative Location headers against the current URL's origin
          const resolved = location.startsWith('/')
            ? new URL(u).origin + location
            : location;
          logger.info({ location, resolved }, 'Following redirect...');
          res.resume(); // drain and discard
          request(resolved, redirectCount + 1);
          return;
        }
        if (res.statusCode !== 200) {
          // Drain and capture a snippet of the body to help diagnose the error
          let snippet = '';
          res.on('data', (c: Buffer) => { if (snippet.length < 500) snippet += c.toString(); });
          res.on('end', () => {
            reject(new Error(`HTTP ${res.statusCode} — body snippet: ${snippet}`));
          });
          return;
        }

        res.on('data', (chunk: Buffer) => { bytesReceived += chunk.length; });
        res.pipe(file);
        file.on('finish', () => {
          file.close(() => {
            logger.info({ bytesReceived }, 'Download complete');
            const sizeMB = (bytesReceived / 1024 / 1024).toFixed(1);
            if (bytesReceived < MIN_ZIP_BYTES) {
              reject(new Error(
                `Downloaded file is only ${sizeMB} MB — likely truncated or not a real zip. ` +
                `Delete data/sirene.zip and retry.`
              ));
            } else {
              logger.info({ sizeMB }, 'File size OK');
              resolve();
            }
          });
        });
      }).on('error', reject);
    };

    request(url);
  });
}

// Find a previously extracted CSV in destDir (non-recursive, flat check).
function findExtractedCsv(destDir: string): string | null {
  if (!fs.existsSync(destDir)) return null;
  const csv = fs.readdirSync(destDir).find(f => f.toLowerCase().endsWith('.csv'));
  return csv ? path.join(destDir, csv) : null;
}

function extractZip(zipPath: string, destDir: string): Promise<string> {
  return new Promise((resolve, reject) => {
    logger.info({ zipPath, destDir }, 'Streaming zip extraction with unzipper...');
    let csvPath: string | null = null;

    fs.createReadStream(zipPath)
      .pipe(unzipper.Parse())
      .on('entry', (entry: unzipper.Entry) => {
        const fileName = entry.path;
        logger.info({ fileName }, 'DIAGNOSTIC: zip entry');
        if (fileName.toLowerCase().endsWith('.csv')) {
          csvPath = path.join(destDir, path.basename(fileName));
          logger.info({ csvPath }, 'Extracting CSV entry...');
          entry.pipe(fs.createWriteStream(csvPath));
        } else {
          entry.autodrain();
        }
      })
      .on('finish', () => {
        if (csvPath) {
          logger.info({ csvPath }, 'Extraction complete');
          resolve(csvPath);
        } else {
          reject(new Error('No .csv file found inside SIRENE zip'));
        }
      })
      .on('error', reject);
  });
}

function buildDatabase(csvPath: string, dbPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    if (fs.existsSync(dbPath) && fs.statSync(dbPath).size > 10000000) {
      logger.info('SIRENE database already exists, skipping rebuild');
      resolve();
      return;
    }

    logger.info({ csvPath, dbPath }, 'Building SQLite index from SIRENE CSV (readline)...');
    const db = new DatabaseSync(dbPath);

    // WAL mode + relaxed durability for fast bulk ingestion.
    db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA cache_size = 100000;
    `);

    // Create table without indexes — indexes are built after all rows are inserted.
    db.exec(`
      CREATE TABLE IF NOT EXISTS etablissements (
        siren TEXT,
        denominationUsuelleEtablissement TEXT,
        enseigne1Etablissement TEXT,
        activitePrincipaleEtablissement TEXT,
        trancheEffectifsEtablissement TEXT,
        libelleCommuneEtablissement TEXT,
        codePostalEtablissement TEXT,
        etatAdministratifEtablissement TEXT,
        etablissementSiege TEXT
      );
    `);

    const insert = db.prepare(`
      INSERT INTO etablissements
        (siren, denominationUsuelleEtablissement, enseigne1Etablissement,
         activitePrincipaleEtablissement, trancheEffectifsEtablissement,
         libelleCommuneEtablissement, codePostalEtablissement,
         etatAdministratifEtablissement, etablissementSiege)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const beginTx = db.prepare('BEGIN');
    const commitTx = db.prepare('COMMIT');

    const flushBatch = (rows: string[][]) => {
      beginTx.run();
      for (const r of rows) insert.run(...r);
      commitTx.run();
    };

    const rl = readline.createInterface({
      input: fs.createReadStream(csvPath),
      crlfDelay: Infinity,
    });

    let columns: string[] = [];
    let colIndex: Record<string, number> = {};
    let batch: string[][] = [];
    let total = 0;
    let skipped = 0;
    let lineNum = 0;

    rl.on('line', (raw: string) => {
      // Strip BOM from the very first line if present
      const line = lineNum === 0 ? raw.replace(/^\uFEFF/, '') : raw;
      lineNum++;

      if (lineNum === 1) {
        // Header row — split on ';' to get column names
        columns = line.split(',').map(c => c.trim());
        columns.forEach((name, i) => { colIndex[name] = i; });
        logger.info({ first5: columns.slice(0, 5), total: columns.length }, 'DIAGNOSTIC: CSV columns');
        return;
      }

      const values = line.split(',');

      const siret    = values[colIndex['siret']]?.trim() ?? '';
      const siren    = siret.substring(0, 9);
      const denomUs  = values[colIndex['denominationUsuelleEtablissement']]?.trim() ?? '';
      const enseigne = values[colIndex['enseigne1Etablissement']]?.trim() ?? '';
      const naf      = values[colIndex['activitePrincipaleEtablissement']]?.trim() ?? '';
      const tranche  = values[colIndex['trancheEffectifsEtablissement']]?.trim() ?? '';
      const city     = values[colIndex['libelleCommuneEtablissement']]?.trim() ?? '';
      const postal   = values[colIndex['codePostalEtablissement']]?.trim() ?? '';
      const etat     = values[colIndex['etatAdministratifEtablissement']]?.trim() ?? '';
      const siege    = values[colIndex['etablissementSiege']]?.trim() ?? '';

      if (!siren || !naf || etat !== 'A') { skipped++; return; }

      batch.push([siren, denomUs, enseigne, naf, tranche, city, postal, etat, siege]);
      total++;

      if (batch.length >= 10000) {
        flushBatch(batch);
        batch = [];
        if (total % 500000 === 0) logger.info(`Indexed ${total} records (skipped ${skipped})...`);
      }
    });

    rl.on('close', () => {
      try {
        if (batch.length > 0) flushBatch(batch);

        // Build indexes after bulk insert — far faster than maintaining them during ingestion.
        logger.info('Building indexes...');
        db.exec(`
          CREATE INDEX IF NOT EXISTS idx_naf    ON etablissements(activitePrincipaleEtablissement);
          CREATE INDEX IF NOT EXISTS idx_city   ON etablissements(libelleCommuneEtablissement);
          CREATE INDEX IF NOT EXISTS idx_tranche ON etablissements(trancheEffectifsEtablissement);
        `);

        db.close();
        logger.info({ total, skipped }, 'SIRENE database built successfully');
        resolve();
      } catch (err) {
        reject(err);
      }
    });

    rl.on('error', reject);
  });
}

const UNITE_LEGALE_PARQUET =
  'https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockUniteLegale_utf8.parquet';

async function enrichNamesWithDuckDB(sirens: string[]): Promise<Map<string, string>> {
  const nameMap = new Map<string, string>();
  if (sirens.length === 0) return nameMap;

  // duckdb has no ESM export — require() is intentional here.
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const duckdb = require('duckdb');
  const db = new duckdb.Database(':memory:');

  return new Promise((resolve, reject) => {
    const sirenList = sirens.map(s => `'${s}'`).join(', ');

    const sql = `
      INSTALL httpfs;
      LOAD httpfs;
      SELECT
        siren,
        COALESCE(
          NULLIF(TRIM(denominationUniteLegale), ''),
          NULLIF(TRIM(CONCAT(
            COALESCE(prenomUsuelUniteLegale, ''), ' ', COALESCE(nomUniteLegale, '')
          )), ' ')
        ) AS company_name
      FROM '${UNITE_LEGALE_PARQUET}'
      WHERE siren IN (${sirenList})
        AND etatAdministratifUniteLegale = 'A'
    `;

    db.all(sql, (err: Error | null, rows: Array<{ siren: string; company_name: string | null }>) => {
      db.close();
      if (err) { reject(err); return; }
      for (const row of rows) {
        const name = row.company_name?.trim();
        if (name) nameMap.set(row.siren, name);
      }
      resolve(nameMap);
    });
  });
}

function getFirstLine(filePath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const rl = readline.createInterface({ input: fs.createReadStream(filePath), crlfDelay: Infinity });
    rl.once('line', (line) => { rl.close(); resolve(line.replace(/^\uFEFF/, '')); });
    rl.once('error', reject);
  });
}

async function ensureDatabase(): Promise<void> {
  // Find the already-extracted CSV in DATA_DIR (no download or extraction needed).
  const csvPath = findExtractedCsv(DATA_DIR);
  if (!csvPath) {
    throw new Error(`No CSV found in ${DATA_DIR}. Place StockEtablissement_utf8.csv there and retry.`);
  }

  // Diagnose delimiter before touching the database.
  const firstLine = await getFirstLine(csvPath);
  const cols = firstLine.split(',');
  logger.info({ count: cols.length, first5: cols.slice(0, 5) }, 'DIAGNOSTIC: first line split by ","');
  if (cols.length === 1) {
    logger.warn({ firstLine: firstLine.slice(0, 200) }, 'DIAGNOSTIC: count=1 means wrong delimiter or encoding — raw first line');
  }

  // If the DB exists but is missing a required column, delete it so buildDatabase rebuilds.
  if (fs.existsSync(SIRENE_DB_PATH)) {
    const db = new DatabaseSync(SIRENE_DB_PATH);
    const cols = db.prepare('PRAGMA table_info(etablissements)').all() as Array<{ name: string }>;
    db.close();
    const colNames = cols.map(c => c.name);
    if (!colNames.includes('enseigne1Etablissement')) {
      logger.info('Schema outdated — rebuilding database');
      fs.unlinkSync(SIRENE_DB_PATH);
    }
  }

  logger.info({ csvPath }, 'CSV found — building SQLite database...');
  await buildDatabase(csvPath, SIRENE_DB_PATH);
}

function tranchesForRange(min: number, max: number): string[] {
  return Object.entries(TRANCHE_MAP)
    .filter(([, range]) => range.min >= min && range.max <= max)
    .map(([code]) => code);
}

export async function sourceCompanies(params: {
  naf_codes: string[];
  city?: string | null;
  headcount_min?: number;
  headcount_max?: number;
  limit?: number;
}): Promise<CompanyRecord[]> {
  await ensureDatabase();

  const db = new DatabaseSync(SIRENE_DB_PATH);

  // Only filter by tranche when a headcount range is explicitly supplied.
  // Artisan pipelines omit it so self-employed entries (tranche 'NN') are included.
  const tranches = (params.headcount_min !== undefined && params.headcount_max !== undefined)
    ? tranchesForRange(params.headcount_min, params.headcount_max)
    : null;

  if (tranches !== null && tranches.length === 0) {
    logger.warn('No matching headcount tranches found for the given range');
    return [];
  }

  // --- Diagnostic queries ---
  const totalRows = (db.prepare('SELECT COUNT(*) AS n FROM etablissements').get() as { n: number }).n;
  logger.info({ totalRows }, 'DIAGNOSTIC: total rows in DB');

  const nafSamples = db.prepare(
    'SELECT DISTINCT activitePrincipaleEtablissement AS v FROM etablissements LIMIT 20'
  ).all() as unknown as Array<{ v: string }>;
  logger.info({ nafSamples: nafSamples.map(r => r.v) }, 'DIAGNOSTIC: NAF format samples');

  const trancheSamples = db.prepare(
    'SELECT DISTINCT trancheEffectifsEtablissement AS v FROM etablissements LIMIT 20'
  ).all() as unknown as Array<{ v: string }>;
  logger.info({ trancheSamples: trancheSamples.map(r => r.v) }, 'DIAGNOSTIC: headcount tranche samples');

  const cityNorm = params.city ? params.city.toUpperCase() : null;

  if (cityNorm) {
    const citySamples = db.prepare(
      'SELECT DISTINCT libelleCommuneEtablissement AS v FROM etablissements WHERE UPPER(libelleCommuneEtablissement) LIKE ? LIMIT 10'
    ).all(`%${cityNorm}%`) as unknown as Array<{ v: string }>;
    logger.info({ citySamples: citySamples.map(r => r.v) }, `DIAGNOSTIC: city samples matching "%${cityNorm}%"`);
  }

  logger.info({ filtering: { naf_codes: params.naf_codes, tranches, city: cityNorm ?? 'ALL' } }, 'DIAGNOSTIC: filter values being used');
  // --- End diagnostics ---

  const nafPlaceholders  = params.naf_codes.map(() => '?').join(', ');
  const cityClause       = cityNorm  ? 'AND UPPER(libelleCommuneEtablissement) LIKE ?' : '';
  const trancheClause    = tranches  ? `AND trancheEffectifsEtablissement IN (${tranches.map(() => '?').join(', ')})` : '';

  const sql = `
    SELECT
      siren,
      COALESCE(
        NULLIF(TRIM(denominationUsuelleEtablissement), ''),
        NULLIF(TRIM(enseigne1Etablissement), '')
      )                                AS company_name,
      libelleCommuneEtablissement      AS city,
      codePostalEtablissement          AS postal_code,
      activitePrincipaleEtablissement  AS naf_code,
      trancheEffectifsEtablissement    AS headcount_tranche
    FROM etablissements
    WHERE activitePrincipaleEtablissement IN (${nafPlaceholders})
      ${cityClause}
      ${trancheClause}
      AND etatAdministratifEtablissement = 'A'
  `;

  const rows = db.prepare(sql).all(
    ...params.naf_codes,
    ...(cityNorm  ? [`%${cityNorm}%`] : []),
    ...(tranches  ? tranches : []),
  ) as unknown as Array<{
    siren: string;
    company_name: string;
    city: string;
    postal_code: string;
    naf_code: string;
    headcount_tranche: string;
  }>;

  db.close();

  logger.info({ count: rows.length }, 'Companies sourced from SIRENE');

  const companies: CompanyRecord[] = rows.map(r => ({
    siren: r.siren,
    siret: r.siren,   // siret not stored; siren used as fallback identifier
    company_name: r.company_name ?? '',
    city: r.city,
    postal_code: r.postal_code ?? '',
    naf_code: r.naf_code,
    headcount_tranche: r.headcount_tranche,
    domain: null,
    mx_found: false,
    phone: null,
  }));

  // Enrich empty names via DuckDB querying StockUniteLegale parquet directly.
  // Physical persons (experts-comptables indépendants) have no denomination in
  // StockEtablissement — their name lives only in StockUniteLegale.
  const unnamed = companies.filter(c => !c.company_name);
  if (unnamed.length > 0) {
    logger.info({ count: unnamed.length }, 'Enriching names via DuckDB + StockUniteLegale parquet...');
    try {
      const nameMap = await enrichNamesWithDuckDB(unnamed.map(c => c.siren));
      for (const company of unnamed) {
        const name = nameMap.get(company.siren);
        if (name) company.company_name = name;
      }
      const resolved = unnamed.filter(c => c.company_name).length;
      logger.info({ resolved, total: unnamed.length }, 'DuckDB name enrichment complete');
    } catch (err) {
      logger.warn({ err }, 'DuckDB name enrichment failed — continuing with empty names');
    }
  }

  // Remove AGAs and management associations — they share NAF 69.20Z but are not accounting firms.
  const AGA_KEYWORDS = [
    'ASSOCIATION DE GESTION',
    'CENTRE DE GESTION',
    'AGA ',
    'GROUPEMENT DE GESTION',
    'ASSOCIATION AGREEE',
  ];
  const filtered = companies.filter(c => {
    const name = c.company_name.toUpperCase();
    if (name.startsWith('ASSOCIATION')) return false;
    return !AGA_KEYWORDS.some(kw => name.includes(kw));
  });
  const removed = companies.length - filtered.length;
  if (removed > 0) logger.info({ removed }, 'Filtered out AGA/association false positives');

  return filtered;
}
