'use strict';

// PURE HARNESS TESTS: mocked HTTP upstream, never real Cube/CubeStore evidence.
const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const crypto = require('node:crypto');
const { gzipSync } = require('node:zlib');
const WebSocket = require('ws');
const { startProxy, requestJson } = require('./ha-fault-proxy');

const deferred = () => {
  let resolve;
  let reject;
  const promise = new Promise((ok, no) => { resolve = ok; reject = no; });
  promise.catch(() => {});
  return { promise, resolve, reject };
};
const payload = gzipSync('id,amount,bucket\n1,20,north\n2,37,south\n');
const sha256 = crypto.createHash('sha256').update(payload).digest('hex');
const name = `${sha256}.csv.gz`;
const uploadPath = `/upload-temp-file?name=${name}&sha256=${sha256}`;
const statusPath = `/upload-temp-file-status?name=${name}&sha256=${sha256}`;

async function fixture(t, mode) {
  const barrier = deferred();
  const prefixReceived = deferred();
  const failed = deferred();
  let uploaded = false;
  let postCount = 0;
  let receivedBytes = 0;
  let inFlight = 0;
  let draining = false;
  const drainStarted = deferred();
  const drainResponses = [];
  const finishDrains = () => {
    if (inFlight === 0) for (const res of drainResponses.splice(0)) {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ draining: true, drained: true, inFlight: 0 }));
    }
  };
  const peers = new Set();
  const remote = http.createServer((req, res) => {
    if (req.url === '/router/drain') {
      draining = true; drainResponses.push(res); drainStarted.resolve(); finishDrains(); return;
    }
    if (req.method === 'POST') {
      inFlight++;
      req.on('close', () => { inFlight--; finishDrains(); });
      postCount++;
      const chunks = [];
      req.on('data', chunk => { receivedBytes += chunk.length; chunks.push(chunk); prefixReceived.resolve(); });
      req.on('end', () => {
        const bytes = Buffer.concat(chunks);
        assert.deepEqual(bytes, payload);
        uploaded = true;
        res.writeHead(200); res.end('');
      });
    } else {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify(uploaded ? { state: 'uploaded', sha256, size: payload.length } : { state: 'missing' }));
    }
  });
  remote.on('connection', socket => { peers.add(socket); socket.on('close', () => peers.delete(socket)); });
  const wsServer = new WebSocket.Server({ server: remote });
  wsServer.on('connection', ws => ws.on('message', bytes => ws.send(bytes)));
  await new Promise(resolve => remote.listen(0, '127.0.0.1', resolve));
  const upstream = new URL(`http://127.0.0.1:${remote.address().port}`);
  const proxy = await startProxy({ upstream, run: 'unit-only', mode, port: 0, controlPort: 0,
    onBarrier: detail => barrier.resolve(detail), onRelease: async () => {},
    onStale: async () => { assert.equal(draining, true); return { rejected: true }; }, onFailure: failed.reject,
  });
  t.after(() => {
    proxy.close();
    for (const peer of peers) peer.destroy();
    wsServer.close(); remote.close();
  });
  const base = new URL(`http://127.0.0.1:${proxy.port}`);
  const control = new URL(`http://127.0.0.1:${proxy.controlPort}`);
  function upload(path = uploadPath) {
    const result = new Promise((resolve, reject) => {
      const req = http.request(new URL(path, base), { method: 'POST', headers: { 'content-length': payload.length } }, res => {
        res.resume(); res.on('end', () => resolve({ status: res.statusCode }));
      });
      req.on('error', reject);
      req.end(payload);
    });
    return result.then(value => ({ value }), error => ({ error }));
  }
  return { proxy, base, barrier, prefixReceived, failed, upload, drainStarted,
    drain: () => requestJson(new URL('/router/drain', upstream), { method: 'POST' }),
    stats: () => ({ uploaded, postCount, receivedBytes, inFlight }),
    release: () => requestJson(new URL('/release', control), { method: 'POST', body: {}, headers: { 'x-ha-run': 'unit-only' } }),
    control,
  };
}

test('PURE: in-flight upload forwards bytes, withholds completion, then closes old WS without replay', { timeout: 10000 }, async t => {
  const f = await fixture(t, 'upload-failover');
  const ws = new WebSocket(`ws://127.0.0.1:${f.proxy.port}/ws`);
  t.after(() => ws.terminate());
  await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  const echoed = new Promise(resolve => ws.once('message', resolve));
  ws.send(Buffer.from([0, 1, 2, 255]));
  assert.deepEqual(await echoed, Buffer.from([0, 1, 2, 255]));
  const wsClosed = new Promise(resolve => ws.once('close', resolve));
  const pending = f.upload();
  const fault = await Promise.race([f.barrier.promise, f.failed.promise]);
  await f.prefixReceived.promise;
  assert.equal(fault.bytesForwarded, payload.length - 1);
  assert.equal(fault.blockedLastByte, true);
  assert.equal(f.stats().receivedBytes, payload.length - 1);
  assert.equal(f.stats().uploaded, false);
  await f.release();
  assert.ok((await pending).error, 'Caller must see response loss');
  await wsClosed;
  assert.equal(f.stats().postCount, 1, 'Proxy must never replay a request');
});

test('PURE: remote upload receipt exists before response loss and is observed after release', { timeout: 10000 }, async t => {
  const f = await fixture(t, 'receipt-loss');
  const pending = f.upload();
  const fault = await Promise.race([f.barrier.promise, f.failed.promise]);
  assert.equal(f.stats().uploaded, true);
  assert.deepEqual(fault.receipt, { state: 'uploaded', sha256, size: payload.length });
  assert.equal(fault.upstreamStatus, 200);
  await f.release();
  assert.ok((await pending).error);
  const receipt = await requestJson(new URL(statusPath, f.base));
  assert.equal(receipt.sha256, sha256);
  assert.equal(f.proxy.state.receipts[0].afterFault, true);
  assert.equal(f.stats().postCount, 1);
});

test('PURE: invalid compressed-byte identity cannot reach upstream or pass a fault barrier', { timeout: 10000 }, async t => {
  const f = await fixture(t, 'receipt-loss');
  const pending = f.upload('/upload-temp-file?name=wrong.csv.gz&sha256=wrong');
  await assert.rejects(f.failed.promise, /immutable name and SHA256/);
  assert.ok((await pending).error);
  assert.equal(f.stats().postCount, 0);
  assert.equal(f.proxy.state.fault, null);
});

test('PURE: unauthenticated control cannot release a barrier', { timeout: 10000 }, async t => {
  const f = await fixture(t, 'upload-failover');
  await assert.rejects(requestJson(new URL('/release', f.control), { method: 'POST', body: {} }), /HTTP 403/);
  assert.equal(f.proxy.state.released, false);
});

test('PURE: async drain exits after upstream abort while downstream upload remains gated until cutover', { timeout: 10000 }, async t => {
  const f = await fixture(t, 'drain-failover');
  let callerFinished = false;
  const pending = f.upload().then(result => { callerFinished = true; return result; });
  await Promise.race([f.barrier.promise, f.failed.promise]);
  await f.prefixReceived.promise;
  assert.equal(f.stats().inFlight, 1);
  const drained = f.drain();
  await f.drainStarted.promise;
  const post = route => requestJson(new URL(route, f.control), { method: 'POST', body: {}, headers: { 'x-ha-run': 'unit-only' } });
  await post('/stale');
  const aborted = await post('/drain-upload-abort');
  assert.equal(aborted.downstreamHeld, true);
  assert.equal(aborted.released, false);
  assert.deepEqual(await drained, { draining: true, drained: true, inFlight: 0 });
  assert.equal(callerFinished, false, 'Driver must stay gated until actual cutover release');
  assert.equal(f.proxy.state.released, false);
  assert.equal(f.proxy.state.drainUploadAborted, true);
  assert.equal(f.stats().uploaded, false);
  assert.equal(f.stats().receivedBytes, payload.length - 1);
  await f.release();
  assert.ok((await pending).error);
  assert.equal(f.stats().postCount, 1, 'No SQL/upload replay by the proxy');
});

test('PURE: upstream drain abort is rejected without stale-writer evidence', { timeout: 10000 }, async t => {
  const f = await fixture(t, 'drain-failover');
  const pending = f.upload();
  await f.barrier.promise;
  await assert.rejects(requestJson(new URL('/drain-upload-abort', f.control), {
    method: 'POST', body: {}, headers: { 'x-ha-run': 'unit-only' },
  }), /HTTP 500/);
  assert.equal(f.proxy.state.drainUploadAborted, undefined);
  assert.equal(f.proxy.state.released, false);
  await f.release();
  await pending;
});

test('PURE: upstream drain abort is single-use and cannot release the downstream gate twice', { timeout: 10000 }, async t => {
  const f = await fixture(t, 'drain-failover');
  const pending = f.upload();
  await f.barrier.promise;
  await f.prefixReceived.promise;
  const drained = f.drain();
  await f.drainStarted.promise;
  const post = route => requestJson(new URL(route, f.control), { method: 'POST', body: {}, headers: { 'x-ha-run': 'unit-only' } });
  await post('/stale');
  await post('/drain-upload-abort');
  await drained;
  await assert.rejects(post('/drain-upload-abort'), /HTTP 500/);
  await assert.rejects(f.failed.promise, /requires an unreleased fault/);
  assert.equal(f.proxy.state.released, false);
  await f.release();
  await pending;
});
