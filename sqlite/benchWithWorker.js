const { Worker } = require('worker_threads');
const writer = new Worker('./writer.js');

const TOTAL = 1e6;
let batch = [];
let batchSize = 1e3;
let written = 0;

writer.on('message', (n) => {
  written += n.written;
  if (written >= TOTAL) {
    console.timeEnd('worker insert');
    console.log(`Inserted ${written} rows`);
    writer.terminate();
    process.exit(0);
  }
});

console.time('worker insert');

for (let i = 0; i < TOTAL; i++) {
  batch.push({
    ts: Date.now(),
    a: Math.random(),
    b: Math.random(),
    c: Math.random(),
    d: Math.random(),
    e: Math.random(),
  });
  if (batch.length >= batchSize) {
    writer.postMessage(batch);
    batch = [];
  }
}

if (batch.length > 0) {
  writer.postMessage(batch);
}
