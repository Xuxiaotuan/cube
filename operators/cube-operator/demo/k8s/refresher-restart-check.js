'use strict';

const assert = require('node:assert/strict');
const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync, spawn } = require('node:child_process');
const MODES = ['uploaded', 'create-sent', 'physical-ready-ledger-unacked'];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const hash = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const validRun = run => /^r[a-f0-9]{16}$/.test(run);

function integer(value, fallback, max) {
  const n = Number(value === undefined ? fallback : value);
  assert.ok(Number.isInteger(n) && n > 0 && n <= max, 'Invalid bounded integer');
  return n;
}

async function until(fn, deadline, label) {
  while (Date.now() < deadline) {
    const result = await fn();
    if (result) return result;
    await sleep(250);
  }
  throw new Error(`Deadline: ${label}`);
}

function json(url, method = 'GET', body, headers = {}) {
  return new Promise((resolve, reject) => {
    const data = body === undefined ? undefined : JSON.stringify(body);
    const req = http.request(url, { method, headers: { ...headers,
      ...(data ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) } : {}) } }, res => {
      let text = '';
      res.on('data', chunk => { text += chunk; if (text.length > 8 * 1024 * 1024) req.destroy(new Error('Response limit')); });
      res.on('error', reject);
      res.on('end', () => {
        try { assert.equal(res.statusCode, 200, text.slice(0, 500)); resolve(JSON.parse(text)); } catch (e) { reject(e); }
      });
    });
    const timer = setTimeout(() => req.destroy(new Error('HTTP deadline')), 8000);
    req.on('close', () => clearTimeout(timer));
    req.on('error', reject);
    req.end(data);
  });
}

// SQL is already formatted by CubeStoreDriver. Inspect only the first CACHE key,
// never search JSON values/comments for a phase or mistake them for a command.
function classify(sql, trace, run) {
  assert.ok(validRun(run));
  const phase = /^CACHE SET NX '(PRE_AGG_PHASE_V1:([a-zA-Z0-9_.]+):(create|ready))' /i.exec(sql);
  if (phase && phase[2].startsWith(`ha_${run}_rollups.`)) return { kind: phase[3].toLowerCase(), buildId: phase[2] };
  const create = /^CREATE TABLE\s+([`a-zA-Z0-9_.]+)\s/i.exec(sql);
  const buildId = create?.[1].replace(/`/g, '');
  if (buildId?.startsWith(`ha_${run}_rollups.`) && trace?.preAggregationBuildId === buildId) return { kind: 'dispatch', buildId };
  return null;
}

class Gate {
  constructor(mode, run) { assert.ok(MODES.includes(mode)); this.mode = mode; this.run = run; this.hit = null; this.released = false; }
  inspect(sql, trace, processUid) {
    if (this.hit && !this.released) return { forward: false, hit: false };
    const match = classify(sql, trace, this.run);
    const target = { uploaded: 'create', 'create-sent': 'dispatch', 'physical-ready-ledger-unacked': 'ready' }[this.mode];
    if (!this.released && match?.kind === target) {
      assert.ok(processUid, 'Wire x-process-id missing');
      this.hit = { ...match, processUid };
      return { forward: target === 'dispatch', hit: true };
    }
    return { forward: true, hit: false };
  }
  release() { assert.ok(this.hit && !this.released); this.released = true; }
}

function recovered(before, after, physical) {
  assert.equal(after.identity.buildId, before.identity.buildId);
  assert.deepEqual(after.identity, before.identity, 'Identity changed');
  assert.deepEqual(after.manifest, before.manifest, 'Manifest changed');
  assert.ok(after.ready && !after.failed && !after.retired, 'Missing successful terminal ledger');
  assert.equal(after.ready.buildId, before.identity.buildId);
  assert.equal(physical.state, 'ready');
  assert.notEqual(physical.tableId, undefined);
  assert.equal(String(after.tableId), String(physical.tableId));
  assert.equal(String(after.ready.tableId), String(physical.tableId));
  if (before.tableId != null) assert.equal(String(after.tableId), String(before.tableId));
  if (before.physical?.tableId != null) assert.equal(String(after.tableId), String(before.physical.tableId));
  assert.deepEqual(physical.locations, before.manifest.create.params);
}

function schedulerEvidence(events, uid, run, processUid) {
  const matches = events.filter(e => e.role === 'refresher' && e.podUid === uid && e.run === run &&
    e.processUid && (!processUid || e.processUid === processUid));
  assert.ok(matches.some(e => e.message === 'Refresh Scheduler Run' && /^scheduler-/.test(e.params?.requestId)), 'Missing real scheduler-process evidence');
  return matches;
}

function assertUnchangedPods(before, after, excludedUid) {
  for (const pod of before.filter(p => p.metadata.uid !== excludedUid)) {
    const current = after.find(p => p.metadata.uid === pod.metadata.uid);
    assert.ok(current && !current.metadata.deletionTimestamp, `Unrelated pod changed: ${pod.metadata.name}`);
    const completedJob = current.status.phase === 'Succeeded' &&
      current.metadata.ownerReferences?.some(owner => owner.kind === 'Job');
    const serving = !['Succeeded', 'Failed'].includes(current.status.phase) &&
      current.status.conditions?.some(c => c.type === 'Ready' && c.status === 'True');
    assert.ok(completedJob || serving, `Unrelated pod not ready or completed Job: ${pod.metadata.name}`);
    assert.deepEqual(current.status.containerStatuses.map(c => [c.name, c.imageID, c.restartCount]),
      pod.status.containerStatuses.map(c => [c.name, c.imageID, c.restartCount]), 'Unrelated process/image changed');
  }
}

async function startWireProxy({ upstream, run, mode, bind, onBarrier, fail, port = 13332 }) {
  const WebSocket = require('ws');
  const flatbuffers = require('flatbuffers');
  const { HttpMessage, HttpQuery, HttpCommand } = require('@cubejs-backend/cubestore-driver/dist/codegen');
  const gate = new Gate(mode, run);
  const sockets = new Set();
  const peers = new Set();
  const state = { creates: [], connections: [] };
  const server = http.createServer((req, res) => {
    // HTTP metadata/uploads are real streamed requests. Barriers are on SQL,
    // after every compressed file has a durable receipt, not a partial upload.
    const remote = http.request(new URL(req.url, upstream), { method: req.method,
      headers: { ...req.headers, host: upstream.host }, agent: false }, response => {
      res.writeHead(response.statusCode, response.headers); response.pipe(res);
    });
    remote.setTimeout(10000, () => remote.destroy(new Error('Proxy HTTP idle deadline')));
    remote.on('error', e => res.destroy(e));
    req.on('aborted', () => remote.destroy());
    req.pipe(remote);
  });
  server.on('connection', s => { sockets.add(s); s.on('close', () => sockets.delete(s)); });
  const wss = new WebSocket.Server({ noServer: true, maxPayload: 8 * 1024 * 1024 });
  server.on('upgrade', (req, socket, head) => {
    const url = new URL(req.url, upstream); url.protocol = 'ws:';
    const headers = {};
    for (const key of ['x-process-id', 'authorization']) if (req.headers[key]) headers[key] = req.headers[key];
    const peer = new WebSocket(url, { headers, handshakeTimeout: 5000, maxPayload: 8 * 1024 * 1024 });
    peers.add(peer);
    peer.on('error', e => { socket.destroy(); fail(e); });
    peer.once('open', () => wss.handleUpgrade(req, socket, head, client => {
      peers.add(client);
      state.connections.push({ processUid: headers['x-process-id'], afterRelease: gate.released });
      client.on('message', data => {
        try {
          const message = HttpMessage.getRootAsHttpMessage(new flatbuffers.ByteBuffer(new Uint8Array(data)));
          assert.equal(message.commandType(), HttpCommand.HttpQuery, 'Unsupported wire command');
          const query = message.command(new HttpQuery());
          assert.equal(query.parametersLength(), 0, 'Unsupported parameterized wire layout; no fault injected');
          const trace = JSON.parse(query.traceObj() || '{}');
          const action = gate.inspect(query.query(), trace, headers['x-process-id']);
          if (classify(query.query(), trace, run)?.kind === 'dispatch' && action.forward) {
            state.creates.push({ buildId: trace.preAggregationBuildId, afterRelease: gate.released });
          }
          if (action.forward) peer.send(data, { binary: true }, error => {
            if (error) fail(error);
            else if (action.hit) Promise.resolve(onBarrier(gate.hit)).catch(fail);
          });
          else if (action.hit) Promise.resolve(onBarrier(gate.hit)).catch(fail);
          // Held messages are discarded, never queued for replay on release.
        } catch (e) { fail(e); }
      });
      peer.on('message', data => {
        if ((!gate.hit || gate.released) && client.readyState === WebSocket.OPEN) client.send(data, { binary: true });
      });
      client.on('error', () => {});
      // Death is expected: the gate belongs to this independent API process.
      client.on('close', () => { peers.delete(client); peer.terminate(); });
      peer.on('close', () => { peers.delete(peer); client.terminate(); });
    }));
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, bind, resolve); });
  return { gate, state, port: server.address().port,
    release() { gate.release(); for (const p of peers) p.terminate(); },
    close() { for (const p of peers) p.terminate(); for (const s of sockets) s.destroy(); wss.close(); server.close(); } };
}

async function runtime() {
  const { CubeStoreDriver } = require('@cubejs-backend/cubestore-driver');
  const helper = require('/cube/conf/ha-scheduled-contexts');
  const run = process.env.HA_RUN;
  const mode = process.env.HA_MODE;
  assert.ok(validRun(run) && MODES.includes(mode));
  assert.equal(process.env.CUBEJS_REFRESH_WORKER, 'false');
  assert.equal(process.env.CUBEJS_HA_SCHEDULED_DEMO, 'true');
  assert.equal(process.env.CUBEJS_HA_RESTART_PROXY_HOST, process.env.HA_API_IP);
  const deadline = Date.now() + integer(process.env.HA_TIMEOUT_SECONDS, 180, 600) * 1000;
  const driver = new CubeStoreDriver();
  const upstream = new URL(process.env.HA_UPSTREAM);
  assert.equal(upstream.protocol, 'http:');
  const emit = (event, fields = {}) => console.log(JSON.stringify({ kind: 'refresher-restart', event, run, mode, time: Date.now(), ...fields }));
  const get = async key => { const rows = await driver.query('CACHE GET ?', [key]); return rows.length ? JSON.parse(rows[0].value) : null; };
  const registrationKey = `${helper.PREFIX}${run}`;
  const keys = () => driver.query('CACHE KEYS ?', [`PRE_AGG_BUILD_V1:ha_${run}_rollups.`]);
  const snapshot = async buildId => {
    const value = {};
    for (const [field, key] of Object.entries({ identity: `PRE_AGG_BUILD_V1:${buildId}`, manifest: `PRE_AGG_MANIFEST_V1:${buildId}`,
      tableId: `PRE_AGG_TABLE_ID_V1:${buildId}`, uploaded: `PRE_AGG_PHASE_V1:${buildId}:uploaded`, create: `PRE_AGG_PHASE_V1:${buildId}:create`,
      ready: `PRE_AGG_PHASE_V1:${buildId}:ready`, failed: `PRE_AGG_PHASE_V1:${buildId}:failed`, retired: `PRE_AGG_PHASE_V1:${buildId}:retired` })) value[field] = await get(key);
    return value;
  };
  let proxy, control, before, registration, finished = false;
  const fail = error => { emit('failure', { status: 'evidence_incomplete', error: error.stack || String(error) }); process.exit(1); };
  const timer = setTimeout(() => fail(new Error('Total runtime deadline')), Math.max(1, deadline - Date.now()));
  try {
    assert.equal((await helper.scheduledContexts((s, p) => driver.query(s, p))).length, 0, 'Another scheduled harness is active');
    // Shared recovery scans all identities. Require a quiescent ledger before
    // installing a barrier; otherwise another context could repair this build.
    for (const { key } of await driver.query('CACHE KEYS ?', ['PRE_AGG_BUILD_V1:'])) {
      const build = await get(key);
      const s = await snapshot(build.buildId);
      assert.ok(s.ready || s.failed || s.retired, 'Unrelated unfinished build; isolate/resolve before running');
    }
    proxy = await startWireProxy({ upstream, run, mode, bind: process.env.HA_API_IP, fail,
      onBarrier: async hit => {
        assert.equal((await keys()).length, 1, 'Expected exactly one scheduler build');
        before = await snapshot(hit.buildId);
        assert.ok(before.identity && before.manifest && before.uploaded && !before.ready && !before.failed && !before.retired);
        assert.ok(before.manifest.uploads?.length && before.manifest.manifestHash, 'Missing immutable uploaded manifest');
        before.receipts = [];
        for (const upload of before.manifest.uploads) {
          const receipt = await json(new URL(`/upload-temp-file-status?name=${encodeURIComponent(upload.name)}&sha256=${upload.sha256}`, upstream));
          assert.equal(receipt.state, 'uploaded'); assert.equal(receipt.sha256, upload.sha256); assert.equal(receipt.size, upload.size);
          before.receipts.push(receipt);
        }
        before.physical = await until(async () => {
          const status = await driver.getPreAggregationBuildStatus(hit.buildId);
          assert.ok(status, 'Runtime lacks build-status capability');
          return mode === 'uploaded' || ['building', 'ready'].includes(status.state) ? status : false;
        }, Math.min(deadline, Date.now() + 10000), 'CREATE admission');
        if (mode === 'uploaded') { assert.equal(before.physical.state, 'absent'); assert.equal(before.create, null); }
        else assert.ok(before.create);
        if (mode === 'physical-ready-ledger-unacked') assert.equal(before.physical.state, 'ready');
        emit('fault_ready', { hit, before, registration, transport: proxy.state });
      } });
    const source = `ha_${run}_source.events`;
    await driver.query(`CREATE SCHEMA ha_${run}_source`, []);
    await driver.query(`CREATE TABLE ${source} (id int, amount int, bucket varchar(32), checksum int)`, []);
    const expected = Array.from({ length: 32 }, (_, i) => ({ id: i + 1, amount: (i + 1) * 7, bucket: `b${i % 4}`, checksum: (i + 1) * 11 }));
    await driver.query(`INSERT INTO ${source} VALUES ${expected.map(r => `(${r.id},${r.amount},'${r.bucket}',${r.checksum})`).join(',')}`, []);
    registration = { expiresAt: deadline, context: { securityContext: { haRun: run, haTransfer: 'rows', haScheduled: true, haRestart: true } } };
    control = http.createServer((req, res) => {
      (async () => {
        assert.equal(req.headers['x-ha-run'], run);
        assert.equal(req.method, 'POST');
        let text = '';
        for await (const chunk of req) { text += chunk; assert.ok(text.length <= 4 * 1024 * 1024); }
        const proof = JSON.parse(text || '{}');
        if (req.url === '/release') {
          assert.ok(before, 'Barrier is not yet proven');
          assert.equal(proof.oldUid, process.env.HA_OLD_UID);
          assert.ok(proof.newUid && proof.newUid !== proof.oldUid && proof.oldGone === true);
          schedulerEvidence(proof.oldEvents, proof.oldUid, run, proxy.gate.hit.processUid);
          assert.deepEqual(await get(registrationKey), registration, 'Durable context lost');
          proxy.release(); emit('released', { proof });
        } else if (req.url === '/verify') {
          assert.ok(proxy.gate.released);
          const events = schedulerEvidence(proof.newEvents, proof.newUid, run);
          assert.ok(events.some(e => e.processUid !== proxy.gate.hit.processUid && proxy.state.connections.some(c => c.afterRelease && c.processUid === e.processUid)), 'No replacement scheduler wire connection');
          const after = await snapshot(before.identity.buildId);
          const physical = await driver.getPreAggregationBuildStatus(before.identity.buildId);
          recovered(before, after, physical);
          assert.equal((await keys()).length, 1, 'Build fork');
          assert.equal(proxy.state.creates.length, 1, 'Missing or duplicated CREATE dispatch');
          const data = await driver.query(`SELECT * FROM ${before.identity.buildId}`, []);
          const normalized = data.map(r => ({ id: Number(r.router_ha_rollup__id), amount: Number(r.router_ha_rollup__total_amount),
            bucket: r.router_ha_rollup__bucket, checksum: Number(r.router_ha_rollup__id_checksum), count: Number(r.router_ha_rollup__row_count) })).sort((a, b) => a.id - b.id);
          assert.deepEqual(normalized, expected.map(r => ({ ...r, count: 1 })), 'Physical rollup result mismatch');
          assert.deepEqual(await driver.query(`SELECT * FROM ${before.identity.buildId}`, []), data, 'Repeated read changed');
          assert.deepEqual(await get(registrationKey), registration);
          await driver.query('CACHE REMOVE ?', [registrationKey]);
          emit('result', { status: 'PASS', scope: 'single-configured-context-refresher-pod-replacement', before, after, physical,
            expectedHash: hash(expected.map(r => ({ ...r, count: 1 }))), actualHash: hash(normalized), transport: proxy.state, proof });
          finished = true;
        } else throw new Error('Unknown controller command');
        res.writeHead(200, { 'content-type': 'application/json' }); res.end('{"ok":true}');
        if (finished) setTimeout(() => process.exit(0), 100);
      })().catch(error => { res.writeHead(500); res.end(JSON.stringify({ error: error.message })); fail(error); });
    });
    await new Promise((resolve, reject) => { control.once('error', reject); control.listen(13333, '127.0.0.1', resolve); });
    await driver.query('CACHE SET NX ? ?', [registrationKey, JSON.stringify(registration)]);
    assert.deepEqual(await get(registrationKey), registration);
    emit('registered', { registration, source, expectedHash: hash(expected), scriptHash: process.env.HA_SCRIPT_HASH });
    await new Promise(() => {});
  } catch (error) { fail(error); }
  finally { clearTimeout(timer); proxy?.close(); control?.close(); await driver.release(); }
}

async function controller() {
  assert.equal(process.env.HA_ALLOW_REFRESHER_DELETE, '1', 'Set HA_ALLOW_REFRESHER_DELETE=1 only for parent-authorized live execution');
  const context = 'orbstack', namespace = 'cube-ha-remediation';
  const timeout = integer(process.env.HA_TIMEOUT_SECONDS, 180, 600);
  const mode = process.env.HA_MODE;
  assert.ok(MODES.includes(mode), `HA_MODE must be one of ${MODES.join(', ')}`);
  const expectedImage = process.env.HA_EXPECTED_IMAGE_ID;
  assert.match(expectedImage || '', /sha256:[a-f0-9]{64}$/, 'Full API runtime image ID is required, not 93e4...');
  const run = `r${crypto.randomBytes(8).toString('hex')}`;
  const dir = fs.mkdtempSync(path.join(process.env.EVIDENCE_DIR || '/tmp', 'refresher-restart-'));
  const args = ['--context', context, '--namespace', namespace, '--request-timeout=8s'];
  const k = (rest, input) => execFileSync(process.env.KUBECTL || 'kubectl', [...args, ...rest], { input, encoding: 'utf8', timeout: 12000, maxBuffer: 8 * 1024 * 1024 });
  const get = rest => JSON.parse(k(['get', ...rest, '-o', 'json']));
  const save = (name, value) => fs.writeFileSync(path.join(dir, name), JSON.stringify(value, null, 2));
  const ready = pod => !pod.metadata.deletionTimestamp && pod.status.conditions?.some(c => c.type === 'Ready' && c.status === 'True');
  const select = name => {
    const dep = get(['deployment', name]); assert.equal(dep.spec.replicas, 1);
    assert.ok(!dep.spec.selector.matchExpressions?.length, 'Unsupported deployment selector');
    const selector = Object.entries(dep.spec.selector.matchLabels).map(([a, b]) => `${a}=${b}`).join(',');
    assert.ok(selector);
    const pods = get(['pods', '-l', selector]).items.filter(ready);
    assert.equal(pods.length, 1, 'Expected one ready pod'); return { pod: pods[0], selector, deployment: dep };
  };
  const api = select(process.env.API_DEPLOYMENT || 'analytics-api');
  const old = select(process.env.REFRESHER_DEPLOYMENT || 'analytics-refresher');
  assert.notEqual(api.pod.metadata.uid, old.pod.metadata.uid);
  const container = (pod, name) => { const c = name || pod.spec.containers[0].name;
    assert.equal(pod.status.containerStatuses.find(x => x.name === c)?.imageID, expectedImage); return c; };
  const ac = container(api.pod, process.env.API_CONTAINER), rc = container(old.pod, process.env.REFRESHER_CONTAINER);
  const allBefore = get(['pods']);
  save('before.json', { api, old, allPods: allBefore, mode, run });
  const env = { HA_RUN: run, HA_MODE: mode, HA_TIMEOUT_SECONDS: String(timeout), HA_API_IP: api.pod.status.podIP,
    HA_OLD_UID: old.pod.metadata.uid, HA_UPSTREAM: process.env.HA_UPSTREAM || 'http://analytics-router-leader:3030',
    HA_SCRIPT_HASH: crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex') };
  const eventsPath = path.join(dir, 'events.jsonl');
  const out = fs.openSync(eventsPath, 'wx'); const err = fs.openSync(path.join(dir, 'runtime.stderr'), 'wx');
  const child = spawn(process.env.KUBECTL || 'kubectl', [...args, 'exec', '-i', api.pod.metadata.name, '-c', ac, '--', 'env',
    ...Object.entries(env).map(([a, b]) => `${a}=${b}`), 'node', '-', 'runtime'], { stdio: ['pipe', out, err] });
  fs.closeSync(out); fs.closeSync(err);
  child.stdin.on('error', () => {}); child.stdin.end(fs.readFileSync(__filename));
  let exitCode = null; child.on('exit', code => { exitCode = code; }); child.on('error', () => { exitCode = 1; });
  const deadline = Date.now() + timeout * 1000;
  const events = () => fs.readFileSync(eventsPath, 'utf8').split('\n').flatMap(line => { try { const e = JSON.parse(line); return e.kind === 'refresher-restart' ? [e] : []; } catch { return []; } });
  const alive = () => { assert.equal(exitCode, null, 'Independent runtime exited'); assert.ok(!events().some(e => e.event === 'failure'), 'Runtime failed'); };
  const runtimeEvents = (pod, c) => JSON.parse(k(['exec', pod, '-c', c, '--', 'node', '-e',
    `console.log(JSON.stringify(require('/cube/conf/ha-scheduled-contexts').readRuntimeEvents(${JSON.stringify(run)})))`]));
  const control = (endpoint, body) => k(['exec', '-i', api.pod.metadata.name, '-c', ac, '--', 'node', '-e',
    `const fs=require('fs');(${json.toString()})(new URL('http://127.0.0.1:13333/${endpoint}'),'POST',JSON.parse(fs.readFileSync(0,'utf8')),{'x-ha-run':'${run}'}).then(()=>process.exit(0)).catch(e=>{console.error(e);process.exit(1)});`.replace("const fs=require('fs');", "const fs=require('fs');const http=require('http');const assert=require('assert/strict');")], JSON.stringify(body));
  try {
    await until(() => { alive(); return events().find(e => e.event === 'fault_ready'); }, deadline, 'scheduler barrier');
    const oldEvents = runtimeEvents(old.pod.metadata.name, rc); save('old-runtime.json', oldEvents);
    const barrier = events().find(e => e.event === 'fault_ready');
    schedulerEvidence(oldEvents, old.pod.metadata.uid, run, barrier.hit.processUid);
    assert.equal(get(['pod', old.pod.metadata.name]).metadata.uid, old.pod.metadata.uid);
    // UID precondition prevents deleting a replacement with a recycled name.
    k(['delete', '--raw', `/api/v1/namespaces/${namespace}/pods/${old.pod.metadata.name}`, '-f', '-'], JSON.stringify({
      apiVersion: 'v1', kind: 'DeleteOptions', gracePeriodSeconds: 0, preconditions: { uid: old.pod.metadata.uid } }));
    const replacement = await until(() => {
      alive(); const pods = get(['pods', '-l', old.selector]).items;
      if (pods.some(p => p.metadata.uid === old.pod.metadata.uid)) return false;
      const candidates = pods.filter(ready); return candidates.length === 1 && candidates[0].metadata.uid !== old.pod.metadata.uid ? candidates[0] : false;
    }, deadline, 'replacement pod ready / old UID absent');
    container(replacement, rc);
    control('release', { oldUid: old.pod.metadata.uid, newUid: replacement.metadata.uid, oldGone: true, oldEvents });
    // Never invoke an API load or scheduler method. Wait for authoritative CACHE
    // marker by a read-only CLI in the unaffected API pod.
    await until(() => {
      alive();
      return k(['exec', api.pod.metadata.name, '-c', ac, '--', 'node', '-e',
        `const {CubeStoreDriver}=require('@cubejs-backend/cubestore-driver');const d=new CubeStoreDriver();d.query('CACHE GET ?', [${JSON.stringify(`PRE_AGG_PHASE_V1:${barrier.hit.buildId}:ready`)}]).then(r=>{console.log(r.length?'READY':'WAIT');return d.release()}).catch(()=>process.exit(1));`]).trim() === 'READY';
    }, deadline, 'scheduler durable ready');
    const newEvents = runtimeEvents(replacement.metadata.name, rc); save('new-runtime.json', newEvents);
    const allAfter = get(['pods']); save('after.json', allAfter);
    assertUnchangedPods(allBefore.items, allAfter.items, old.pod.metadata.uid);
    control('verify', { newUid: replacement.metadata.uid, newEvents });
    await until(() => events().find(e => e.event === 'result' && e.status === 'PASS'), deadline, 'final evidence');
    save('summary.json', { status: 'PASS', scope: 'single-node single-configured-context pod replacement, not node fencing/DR', run, mode, expectedImage });
    console.log(`PASS ${mode}; evidence: ${dir}`);
  } catch (e) { save('summary.json', { status: 'evidence_incomplete', error: e.stack, run, mode }); throw e; }
  finally { child.kill(); console.log(`Evidence retained: ${dir}; failed registrations expire; no table/object cleanup performed.`); }
}

module.exports = { Gate, classify, recovered, schedulerEvidence, integer, until, startWireProxy, assertUnchangedPods };
if (require.main === module) {
  const task = process.argv[2] === 'controller' ? controller : process.argv[2] === 'runtime' ? runtime : null;
  if (!task) { console.error('Use controller or runtime'); process.exitCode = 1; }
  else task().catch(e => { console.error(e.stack); process.exitCode = 1; });
}
