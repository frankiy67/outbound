const { DatabaseSync } = require('node:sqlite');
const db = new DatabaseSync('D:/outbound-data/sirene.db');

const labels = {
  '43.21A': 'Electricien',
  '43.22A': 'Plombier',
  '43.22B': 'Chauffagiste',
  '43.31Z': 'Platrier',
  '43.32A': 'Menuisier',
  '43.32B': 'Menuisier metal',
  '43.33Z': 'Carreleur',
  '43.34Z': 'Peintre',
  '43.39Z': 'Finition',
  '43.91A': 'Charpentier',
  '43.99B': 'Macon'
};

const codes = Object.keys(labels).map(c => "'" + c + "'").join(',');

function queryCounts(whereClause) {
  return db.prepare(
    'SELECT activitePrincipaleEtablissement as n, COUNT(*) as c ' +
    'FROM etablissements ' +
    'WHERE ' + whereClause + ' ' +
    'AND activitePrincipaleEtablissement IN (' + codes + ') ' +
    'GROUP BY n ORDER BY c DESC'
  ).all();
}

function printSection(title, rows) {
  console.log('\n=== ' + title + ' ===');
  let total = 0;
  rows.forEach(function(x) {
    console.log((labels[x.n] || x.n) + ' : ' + x.c);
    total += x.c;
  });
  console.log('TOTAL: ' + total);
}

const franceRows = queryCounts("etatAdministratifEtablissement = 'A'");
const parisRows  = queryCounts("etatAdministratifEtablissement = 'A' AND UPPER(libelleCommuneEtablissement) LIKE '%PARIS%'");

printSection('FRANCE ENTIERE', franceRows);
printSection('PARIS SEULEMENT', parisRows);
