const { Worker } = require('worker_threads');

const writer = new Worker('./writer.js');
const reader = new Worker('./reader.js');

const BATCH_SIZE = 1000;
let batch = [];
let generated = 0;

setInterval(() => {
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
}, 1);

setInterval(() => {
  if (batch.length > 0) {
    writer.postMessage(batch);
    batch = [];
  }
}, 1000);

let written = 0;
writer.on('message', (msg) => {
  written += msg.written;
});

setInterval(() => {
  console.log(`Sent count: ${generated}, Confirmed written: ${written}`);
  reader.postMessage('count');
}, 5000);
