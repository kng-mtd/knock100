const { Worker } = require('worker_threads');

const writer = new Worker('./writer.js');
const counter = new Worker('./reader.js');

const BATCH_SIZE = 1000;
let batch = [];
let generated = 0;
let written = 0;

writer.on('message', (msg) => {
  if (msg.written) written += msg.written;
});

function generate() {
  batch.push({
    ts: Date.now(),
    a: Math.random(),
    b: Math.random(),
    c: Math.random(),
    d: Math.random(),
    e: Math.random(),
  });
  generated++;

  if (batch.length >= BATCH_SIZE) {
    writer.postMessage(batch);
    batch = [];
  }

  setImmediate(generate);
}
generate();

setInterval(() => {
  console.log(`Generated: ${generated}, Confirmed written: ${written}, Lag: ${generated - written}`);
  counter.postMessage('count');
}, 5000);
