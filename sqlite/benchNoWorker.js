const Database = require('better-sqlite3');

const db = new Database('test.db');
db.pragma('journal_mode = WAL');
db.exec('PRAGMA synchronous = OFF');
db.exec('DROP TABLE IF EXISTS data');
db.exec('CREATE TABLE data(ts INTEGER, a REAL, b REAL, c REAL, d REAL, e REAL)');

const stmt = db.prepare('INSERT INTO data VALUES (?,?,?,?,?,?)');

const TOTAL = 1e6;
console.time('main-thread insert');

db.exec('BEGIN TRANSACTION');
for (let i = 0; i < TOTAL; i++) {
  stmt.run(Date.now(), Math.random(), Math.random(), Math.random(), Math.random(), Math.random());
}
db.exec('COMMIT');

console.timeEnd('main-thread insert');
console.log(`Inserted ${TOTAL} rows`);
