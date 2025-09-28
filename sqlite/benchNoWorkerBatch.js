const Database = require('better-sqlite3');

const db = new Database('test.db');
db.pragma('journal_mode = WAL');
db.exec('PRAGMA synchronous = OFF');
db.exec('DROP TABLE IF EXISTS data');
db.exec('CREATE TABLE data(ts INTEGER, a REAL, b REAL, c REAL, d REAL, e REAL)');

const stmt = db.prepare('INSERT INTO data VALUES (?,?,?,?,?,?)');

const TOTAL = 1e6;
const BATCH_SIZE = 1e3;

console.time('main-thread batch insert');

for (let i = 0; i < TOTAL; i += BATCH_SIZE) {
  const batch = [];
  for (let j = 0; j < BATCH_SIZE && i + j < TOTAL; j++) {
    batch.push([Date.now(), Math.random(), Math.random(), Math.random(), Math.random(), Math.random()]);
  }

  db.transaction(() => {
    for (const row of batch) stmt.run(row);
  })();
}

console.timeEnd('main-thread batch insert');
console.log(`Inserted ${TOTAL} rows`);
