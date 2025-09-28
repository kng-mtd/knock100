const { parentPort } = require('worker_threads');
const Database = require('better-sqlite3');

const db = new Database('test.db');
db.exec('PRAGMA journal_mode = WAL');
db.exec('PRAGMA synchronous = OFF');
db.exec('DROP TABLE IF EXISTS data');
db.exec(`
  CREATE TABLE IF NOT EXISTS data(
    id INTEGER PRIMARY KEY,
    ts INTEGER,
    a REAL, b REAL, c REAL, d REAL, e REAL
  )
`);

const insert = db.prepare('INSERT INTO data(ts, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ?)');
const insertMany = db.transaction((rows) => {
  for (const r of rows) insert.run(r.ts, r.a, r.b, r.c, r.d, r.e);
});

parentPort.on('message', (batch) => {
  insertMany(batch);
  parentPort.postMessage({ written: batch.length });
});
