'use strict';

const http = require('node:http');
const crypto = require('node:crypto');

function inspectData(data, expected) {
  if (!Array.isArray(data) || data.length !== 16) throw new Error('Expected 16 grouped result rows');
  const fields = ['RouterHaRollup.rowCount', 'RouterHaRollup.totalAmount', 'RouterHaRollup.idChecksum'];
  const totals = [0n, 0n, 0n];
  const buckets = new Set();
  for (const row of data) {
    const bucket = row['RouterHaRollup.bucket'];
    if (typeof bucket !== 'string' || buckets.has(bucket)) throw new Error('Invalid or duplicate bucket');
    buckets.add(bucket);
    fields.forEach((field, index) => {
      const value = String(row[field]);
      if (!/^\d+$/.test(value)) throw new Error(`Non-integer aggregate: ${field}`);
      totals[index] += BigInt(value);
    });
  }
  if (totals.some((total, index) => total !== BigInt(expected[index]))) throw new Error('Business aggregate mismatch');
  const canonical = data.map(row => [row['RouterHaRollup.bucket'], ...fields.map(field => String(row[field]))])
    .sort((a, b) => a[0].localeCompare(b[0]));
  return { totals: totals.map(String), hash: crypto.createHash('sha256').update(JSON.stringify(canonical)).digest('hex') };
}

function load(token, timeout) {
  const body = JSON.stringify({ query: {
    measures: ['RouterHaRollup.rowCount', 'RouterHaRollup.totalAmount', 'RouterHaRollup.idChecksum'],
    dimensions: ['RouterHaRollup.bucket'],
    order: { 'RouterHaRollup.bucket': 'asc' },
  } });
  return new Promise((resolve, reject) => {
    const request = http.request({ hostname: '127.0.0.1', port: 4000, path: '/cubejs-api/v1/load', method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) } }, response => {
      let text = '';
      response.setEncoding('utf8');
      response.on('data', chunk => {
        text += chunk;
        if (text.length > 1024 * 1024) request.destroy(new Error('Response exceeds evidence budget'));
      });
      response.on('error', reject);
      response.on('end', () => {
        try {
          const result = JSON.parse(text);
          if (response.statusCode !== 200 || result.error) throw new Error(`API unavailable (${response.statusCode}): ${result.error || 'non-success'}`);
          resolve(result.data);
        } catch (error) { reject(error); }
      });
    });
    const deadline = setTimeout(() => request.destroy(new Error('API request deadline exceeded')), timeout);
    request.on('close', () => clearTimeout(deadline));
    request.on('error', reject);
    request.end(body);
  });
}

async function observe() {
  const run = process.env.HA_RUN;
  const duration = Number(process.env.OBSERVE_SECONDS || 60);
  if (!/^r[a-f0-9]{16}$/.test(run || '') || !Number.isInteger(duration) || duration < 10 || duration > 600) throw new Error('Invalid run or observation duration');
  const expected = [process.env.EXPECTED_ROWS, process.env.EXPECTED_AMOUNT, process.env.EXPECTED_CHECKSUM];
  if (!expected.every(value => /^\d+$/.test(value || ''))) throw new Error('Explicit expected aggregates are required');
  const jwt = require('jsonwebtoken');
  const token = jwt.sign({ haRun: run, haScheduled: true, haTransfer: 'rows' }, process.env.CUBEJS_API_SECRET, { expiresIn: duration + 60 });
  const start = Date.now();
  let good = 0; let unavailable = 0; let mismatch = 0; let baselineHash; let lastGood = false;
  let failureStart; let maxRecoveryMs = 0;
  while (Date.now() - start < duration * 1000) {
    let data;
    let loaded = false;
    const elapsedMs = Date.now() - start;
    try {
      data = await load(token, Math.max(1, Math.min(5000, duration * 1000 - elapsedMs)));
      loaded = true;
    } catch (error) {
      unavailable++; lastGood = false;
      failureStart ??= Date.now();
      console.log(JSON.stringify({ event: 'unavailable', elapsedMs, message: error.message }));
    }
    if (loaded) {
      try {
        const observed = inspectData(data, expected);
        if (baselineHash && baselineHash !== observed.hash) throw new Error('Grouped result hash changed');
        baselineHash = observed.hash;
        good++; lastGood = true;
        if (failureStart !== undefined) maxRecoveryMs = Math.max(maxRecoveryMs, Date.now() - failureStart);
        failureStart = undefined;
        console.log(JSON.stringify({ event: 'query', elapsedMs, ...observed }));
      } catch (error) {
        mismatch++; lastGood = false;
        console.log(JSON.stringify({ event: 'data_mismatch', elapsedMs, message: error.message }));
      }
    }
    await new Promise(resolve => setTimeout(resolve, Math.min(1000, Math.max(0, duration * 1000 - (Date.now() - start)))));
  }
  const passed = good >= 2 && mismatch === 0 && lastGood;
  console.log(JSON.stringify({ event: 'result', status: passed ? 'PASS' : 'FAIL', run, good, unavailable, mismatch,
    maxRecoveryMs, hash: baselineHash, scope: 'Cube API consumption of an existing rollup; cache may serve reads; not fresh-build recovery or per-query Router execution proof' }));
  if (!passed) process.exitCode = 1;
}

module.exports = { inspectData };
if (!module.parent) observe().catch(error => { console.error(error.message); process.exitCode = 1; });
