const { parentPort } = require('worker_threads');
const Database = require('better-sqlite3');

const db = new Database('test.db', { readonly: true });

parentPort.on('message', (msg) => {
  if (msg === 'count') {
    const row = db.prepare('SELECT COUNT(1) AS n FROM data').get();
    console.log(`Recorded count: ${row.n}`);
  }
});
