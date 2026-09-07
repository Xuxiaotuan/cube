'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const http = require('node:http');
const { once } = require('node:events');
const { Gate, classify, recovered, schedulerEvidence, integer, until, startWireProxy, assertUnchangedPods,
  replacementRunning, requireFinalReady, assertProxyEndpoint, normalizeApiData, assertApiDidNotBuild } = require('./refresher-restart-check');

const run = 'r0123456789abcdef';
const buildId = `ha_${run}_rollups.router_ha_rollup_by_id_c_s_123`;
const phase = name => `CACHE SET NX 'PRE_AGG_PHASE_V1:${buildId}:${name}' '{}'`;
const create = `CREATE TABLE \`${buildId.split('.')[0]}\`.\`${buildId.split('.')[1]}\` (id int)`;
const trace = { preAggregationBuildId: buildId };

function topologyPod(uid, phase = 'Running', isReady = true, ownerKind = 'ReplicaSet') {
  return {
    metadata: { uid, name: uid, ownerReferences: [{ kind: ownerKind, uid: `${uid}-owner` }] },
    status: { phase, conditions: [{ type: 'Ready', status: isReady ? 'True' : 'False' }],
      containerStatuses: [{ name: 'main', imageID: 'sha256:fixture', restartCount: 0 }] },
  };
}

test('topology permits unchanged Succeeded Job without Ready, alongside ready services', () => {
  const before = [topologyPod('api'), topologyPod('router'), topologyPod('old-refresher'),
    topologyPod('cube-router-object-store-init', 'Succeeded', false, 'Job')];
  const after = structuredClone(before.filter(p => p.metadata.uid !== 'old-refresher'));
  after.push(topologyPod('new-refresher'));
  assertUnchangedPods(before, after, 'old-refresher');
});

test('topology does not exempt Failed Job even with a stale Ready condition', () => {
  for (const isReady of [false, true]) {
    const pods = [topologyPod('failed-job', 'Failed', isReady, 'Job')];
    assert.throws(() => assertUnchangedPods(pods, structuredClone(pods), 'old-refresher'), /not ready or completed Job/);
  }
});

test('topology does not exempt a running not-Ready Job', () => {
  const pods = [topologyPod('running-job', 'Running', false, 'Job')];
  assert.throws(() => assertUnchangedPods(pods, structuredClone(pods), 'old-refresher'), /not ready or completed Job/);
});

test('topology does not exempt Succeeded service or ownerless Pod', () => {
  for (const isReady of [false, true]) {
    const service = topologyPod('service', 'Succeeded', isReady);
    assert.throws(() => assertUnchangedPods([service], [structuredClone(service)], 'old-refresher'), /not ready or completed Job/);
    delete service.metadata.ownerReferences;
    assert.throws(() => assertUnchangedPods([service], [structuredClone(service)], 'old-refresher'), /not ready or completed Job/);
  }
});

test('topology rejects missing service and replacement with the same name but different UID', () => {
  const api = topologyPod('api');
  assert.throws(() => assertUnchangedPods([api], [], 'old-refresher'), /Unrelated pod changed/);
  const replacement = structuredClone(api); replacement.metadata.uid = 'replacement-api';
  assert.throws(() => assertUnchangedPods([api], [replacement], 'old-refresher'), /Unrelated pod changed/);
});

test('completed Job exemption preserves presence, deletion, image and restart checks', () => {
  const job = topologyPod('completed-job', 'Succeeded', false, 'Job');
  assert.throws(() => assertUnchangedPods([job], [], 'old-refresher'), /Unrelated pod changed/);
  for (const change of [
    p => { p.metadata.deletionTimestamp = '2026-09-07T00:00:00Z'; },
    p => { p.status.containerStatuses[0].imageID = 'sha256:changed'; },
    p => { p.status.containerStatuses[0].restartCount = 1; },
  ]) {
    const changed = structuredClone(job); change(changed);
    assert.throws(() => assertUnchangedPods([job], [changed], 'old-refresher'));
  }
});

test('topology still rejects an ordinary running service losing readiness', () => {
  const api = topologyPod('api');
  const after = topologyPod('api', 'Running', false);
  assert.throws(() => assertUnchangedPods([api], [after], 'old-refresher'), /not ready or completed Job/);
});

function startedReplacement(isReady = false) {
  const pod = topologyPod('replacement', 'Running', isReady);
  Object.assign(pod.status.containerStatuses[0], { containerID: 'containerd://replacement',
    state: { running: { startedAt: '2026-09-07T00:00:00Z' } } });
  return pod;
}

test('restart sequence releases for a started Running container before Ready, then requires final Ready', () => {
  const pod = startedReplacement();
  assert.equal(replacementRunning([pod], 'old', 'main', 'sha256:fixture'), pod);
  assert.throws(() => requireFinalReady(pod, 'replacement', 'main', 'sha256:fixture'), /finally Ready/);
  pod.status.conditions[0].status = 'True';
  requireFinalReady(pod, 'replacement', 'main', 'sha256:fixture');
});

test('release refuses old UID still present, pending pod, or unstarted target container', () => {
  const pod = startedReplacement();
  assert.equal(replacementRunning([topologyPod('old'), pod], 'old', 'main', 'sha256:fixture'), null);
  const pending = structuredClone(pod); pending.status.phase = 'Pending';
  assert.equal(replacementRunning([pending], 'old', 'main', 'sha256:fixture'), null);
  const waiting = structuredClone(pod); waiting.status.containerStatuses[0].state = { waiting: {} };
  assert.equal(replacementRunning([waiting], 'old', 'main', 'sha256:fixture'), null);
  const missingID = structuredClone(pod); delete missingID.status.containerStatuses[0].containerID;
  assert.equal(replacementRunning([missingID], 'old', 'main', 'sha256:fixture'), null);
});

test('release/final Ready reject ambiguous identity or changed runtime image', () => {
  const pod = startedReplacement(true);
  assert.throws(() => replacementRunning([pod, topologyPod('other')], 'old', 'main', 'sha256:fixture'), /Multiple/);
  assert.throws(() => replacementRunning([pod], 'old', 'main', 'sha256:wrong'), /image changed/);
  assert.throws(() => requireFinalReady(pod, 'different', 'main', 'sha256:fixture'), /changed after release/);
});

function networkFixture() {
  const api = topologyPod('api');
  Object.assign(api.metadata, { namespace: 'cube-ha-remediation', labels: { role: 'api' } });
  api.status.podIP = '10.0.0.5';
  const service = { metadata: { name: 'proxy', namespace: api.metadata.namespace },
    spec: { clusterIP: '10.96.0.9', selector: { role: 'api' }, ports: [{ port: 13332, targetPort: 13332, protocol: 'TCP' }] } };
  const slices = [{ metadata: { labels: { 'kubernetes.io/service-name': 'proxy' } }, ports: [{ port: 13332 }],
    endpoints: [{ addresses: ['10.0.0.5'], conditions: { ready: true }, targetRef: { kind: 'Pod', uid: 'api', name: 'api' } }] }];
  return { api, service, slices };
}

test('stable proxy endpoint accepts ClusterIP Service targeting the pinned API', () => {
  const f = networkFixture(); assertProxyEndpoint(f.service, f.slices, f.api);
});

test('stable proxy endpoint fails closed for wrong routing, stale IP or multiple pods', () => {
  for (const change of [
    f => { f.service.spec.clusterIP = 'None'; },
    f => { f.service.spec.ports[0].targetPort = 4000; },
    f => { f.service.spec.selector.role = 'refresher'; },
    f => { f.slices[0].endpoints[0].targetRef.uid = 'other-api'; },
    f => { f.slices[0].endpoints[0].addresses = ['10.0.0.6']; },
    f => { f.slices[0].endpoints[0].conditions.ready = false; },
    f => { f.slices[0].endpoints[0].conditions.terminating = true; },
    f => { f.slices[0].endpoints.push(structuredClone(f.slices[0].endpoints[0])); },
  ]) {
    const f = networkFixture(); change(f); assert.throws(() => assertProxyEndpoint(f.service, f.slices, f.api));
  }
});

test('Cube API result normalization preserves every expected metric, independent of row order', () => {
  const row = id => ({ 'RouterHaRollup.id': String(id), 'RouterHaRollup.totalAmount': id * 7,
    'RouterHaRollup.bucket': 'b0', 'RouterHaRollup.idChecksum': String(id * 11), 'RouterHaRollup.rowCount': '1' });
  assert.deepEqual(normalizeApiData([row(2), row(1)]), [1, 2].map(id => ({ id, amount: id * 7, bucket: 'b0', checksum: id * 11, count: 1 })));
  for (const value of [null, undefined, '', 'NaN', Infinity]) {
    assert.throws(() => normalizeApiData([{ ...row(1), 'RouterHaRollup.totalAmount': value }]));
  }
  assert.throws(() => normalizeApiData(undefined));
});

test('post-recovery API evidence rejects API-triggered builds and wrong runtime identity', () => {
  const event = { run, role: 'api', podUid: 'api', message: 'Executing SQL', params: { query: 'SELECT 1' } };
  assertApiDidNotBuild([event], run, 'api');
  for (const patch of [{ role: 'refresher' }, { podUid: 'other-api' },
    { message: 'Uploading external pre-aggregation' }, { message: 'Performing query', params: { newVersionEntry: {} } }]) {
    assert.throws(() => assertApiDidNotBuild([{ ...event, ...patch }], run, 'api'));
  }
  assert.throws(() => assertApiDidNotBuild([], run, 'api'));
});

test('classifier matches exact run, CACHE key and CREATE tracing, not embedded text', () => {
  assert.deepEqual(classify(phase('create'), {}, run), { kind: 'create', buildId });
  assert.deepEqual(classify(phase('ready'), {}, run), { kind: 'ready', buildId });
  assert.deepEqual(classify(create, trace, run), { kind: 'dispatch', buildId });
  for (const sql of [phase('uploaded'), `SELECT '${phase('ready')}'`, `-- ${phase('ready')}`,
    phase('ready').replace(run, 'rfedcba9876543210'), "CACHE SET NX 'other' 'PRE_AGG_PHASE_V1:ready'"]) {
    assert.equal(classify(sql, {}, run), null);
  }
  assert.equal(classify(create, {}, run), null);
  assert.throws(() => classify(create, trace, 'unsafe'));
});

for (const [mode, sql, forward] of [
  ['uploaded', phase('create'), false],
  ['create-sent', create, true],
  ['physical-ready-ledger-unacked', phase('ready'), false],
]) {
  test(`${mode}: gate holds subsequent/reconnected traffic until explicit release`, () => {
    const gate = new Gate(mode, run);
    assert.deepEqual(gate.inspect('SELECT 1', {}, 'old'), { forward: true, hit: false });
    assert.deepEqual(gate.inspect(sql, trace, 'old'), { forward, hit: true });
    assert.equal(gate.hit.processUid, 'old');
    assert.deepEqual(gate.inspect(sql, trace, 'replacement'), { forward: false, hit: false });
    gate.release();
    assert.deepEqual(gate.inspect(sql, trace, 'replacement'), { forward: true, hit: false });
    assert.throws(() => gate.release());
  });
}

test('gate rejects missing process attribution, invalid modes and premature release', () => {
  assert.throws(() => new Gate('router-failover', run));
  const gate = new Gate('uploaded', run);
  assert.throws(() => gate.release());
  assert.throws(() => gate.inspect(phase('create'), {}, undefined), /x-process-id/);
  assert.equal(gate.hit, null);
});

function fixture() {
  const identity = { buildId, phase: 'selected' };
  const manifest = { create: { sql: create, params: ['temp://bytes.csv.gz'] }, manifestHash: 'immutable' };
  const before = { identity, manifest, physical: { state: 'ready', tableId: '7' } };
  const after = { identity: { ...identity }, manifest: structuredClone(manifest), tableId: '7',
    ready: { buildId, phase: 'ready', tableId: '7' }, failed: null, retired: null };
  const physical = { state: 'ready', tableId: 7, locations: ['temp://bytes.csv.gz'] };
  return { before, after, physical };
}

test('recovery evidence accepts stable identity, immutable manifest and real table identity', () => {
  const { before, after, physical } = fixture();
  recovered(before, after, physical);
});

test('recovery evidence rejects fork, manifest drift, missing ack and table replacement', () => {
  const changes = [
    x => { x.after.identity.buildId += '_fork'; },
    x => { x.after.manifest.manifestHash = 'changed'; },
    x => { x.after.ready = null; },
    x => { x.after.failed = {}; },
    x => { x.after.retired = {}; },
    x => { x.after.tableId = '8'; },
    x => { x.physical.state = 'building'; },
    x => { x.physical.locations = ['temp://different']; },
    x => { x.before.physical.tableId = '8'; },
  ];
  for (const change of changes) {
    const f = fixture(); change(f);
    assert.throws(() => recovered(f.before, f.after, f.physical));
  }
});

test('scheduler proof refuses API activity and wrong pod/process/run', () => {
  const event = { role: 'refresher', podUid: 'old-pod', run, processUid: 'old-process',
    message: 'Refresh Scheduler Run', params: { requestId: 'scheduler-real-tick' } };
  assert.equal(schedulerEvidence([event], 'old-pod', run, 'old-process').length, 1);
  for (const patch of [{ role: 'api' }, { podUid: 'other' }, { processUid: 'other' },
    { run: 'rfedcba9876543210' }, { params: { requestId: 'api-load' } }]) {
    assert.throws(() => schedulerEvidence([{ ...event, ...patch }], 'old-pod', run, 'old-process'));
  }
});

test('bounded integer and deadline helpers fail rather than fabricate completion', async () => {
  assert.equal(integer(undefined, 180, 600), 180);
  for (const n of ['abc', 0, -1, 601, 1.5]) assert.throws(() => integer(n, 180, 600));
  assert.equal(await until(async () => 'evidence', Date.now() + 100, 'test'), 'evidence');
  await assert.rejects(until(async () => false, Date.now() - 1, 'missing evidence'), /Deadline/);
});

test('controller authorization is exact and checked before any kubectl call', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'restart-auth-test-'));
  const fake = path.join(dir, 'kubectl');
  const marker = path.join(dir, 'called');
  fs.writeFileSync(fake, '#!/bin/sh\nprintf called >> "$MOCK_KUBECTL_MARKER"\nprintf "MOCK_K8S_ONLY\\n" >&2\nexit 77\n', { mode: 0o700 });
  const script = path.join(__dirname, 'refresher-restart-check.js');
  const env = { ...process.env, KUBECTL: fake, MOCK_KUBECTL_MARKER: marker,
    HA_MODE: 'uploaded', HA_EXPECTED_IMAGE_ID: `docker-pullable://fixture@sha256:${'a'.repeat(64)}` };
  try {
    for (const value of [undefined, '0', 'true', '01']) {
      const current = { ...env }; delete current.HA_ALLOW_REFRESHER_DELETE;
      if (value !== undefined) current.HA_ALLOW_REFRESHER_DELETE = value;
      const result = spawnSync(process.execPath, [script, 'controller'], { env: current, encoding: 'utf8', timeout: 5000 });
      assert.equal(result.status, 1);
      assert.match(result.stderr, /HA_ALLOW_REFRESHER_DELETE=1/);
      assert.equal(fs.existsSync(marker), false);
    }
    const allowed = spawnSync(process.execPath, [script, 'controller'], {
      env: { ...env, EVIDENCE_DIR: dir, HA_ALLOW_REFRESHER_DELETE: '1' }, encoding: 'utf8', timeout: 5000,
    });
    assert.equal(allowed.status, 1); // Deliberate mock discovery failure, not a scenario PASS.
    assert.equal(fs.readFileSync(marker, 'utf8'), 'called');
    assert.match(allowed.stderr, /MOCK_K8S_ONLY/);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

// Real local WS connections and real FlatBuffers encoding; ONLY the upstream
// transport peer is a fixture. These are not CubeStore/Kubernetes E2E tests.
for (const [mode, sql, expectedBefore] of [
  ['uploaded', phase('create'), 0],
  ['create-sent', create, 1],
  ['physical-ready-ledger-unacked', phase('ready'), 0],
]) {
  test(`${mode}: external wire gate survives client death and discards held writes`, { timeout: 10000 }, async t => {
    const WebSocket = require('ws');
    const flatbuffers = require('flatbuffers');
    const { HttpMessage, HttpQuery, HttpCommand, QueryResultFormat } = require('@cubejs-backend/cubestore-driver/dist/codegen');
    const server = http.createServer();
    const wss = new WebSocket.Server({ server });
    const received = [];
    wss.on('connection', peer => peer.on('message', bytes => { received.push(Buffer.from(bytes)); peer.send(bytes); }));
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    let hit;
    const barrier = new Promise(resolve => { hit = resolve; });
    const failures = [];
    const proxy = await startWireProxy({ upstream: new URL(`http://127.0.0.1:${server.address().port}`),
      run, mode, bind: '127.0.0.1', port: 0, onBarrier: hit, fail: error => failures.push(error) });
    const clients = [];
    t.after(() => {
      clients.forEach(c => c.terminate()); proxy.close();
      wss.clients.forEach(c => c.terminate()); wss.close(); server.close();
    });
    function wire(text, id) {
      const b = new flatbuffers.Builder(1024);
      const q = b.createString(text), tracing = b.createString(JSON.stringify(trace));
      const query = HttpQuery.createHttpQuery(b, q, tracing, 0, 0, QueryResultFormat.Legacy);
      const connection = b.createString('fixture-connection');
      const msg = HttpMessage.createHttpMessage(b, id, HttpCommand.HttpQuery, query, connection);
      HttpMessage.finishHttpMessageBuffer(b, msg);
      return Buffer.from(b.asUint8Array());
    }
    async function connect(uid) {
      const client = new WebSocket(`ws://127.0.0.1:${proxy.port}/ws`, { headers: { 'x-process-id': uid } });
      clients.push(client); await once(client, 'open'); return client;
    }
    const old = await connect('old-process');
    old.send(wire(sql, 1));
    await barrier;
    await until(() => received.length === expectedBefore, Date.now() + 1000, 'forwarded CREATE');
    const closed = once(old, 'close'); old.terminate(); await closed;
    assert.ok(proxy.gate.hit); assert.equal(proxy.gate.released, false);
    const premature = await connect('replacement-before-release');
    premature.send(wire('SELECT 99', 2));
    // Message handling remains blocked even on a distinct replacement socket.
    await new Promise(resolve => setTimeout(resolve, 30));
    assert.equal(received.length, expectedBefore);
    proxy.release();
    const replacement = await connect('replacement-after-release');
    const response = once(replacement, 'message');
    const fresh = wire('SELECT 1', 3); replacement.send(fresh);
    assert.deepEqual(Buffer.from((await response)[0]), fresh);
    assert.equal(received.length, expectedBefore + 1, 'No held statement may be replayed');
    assert.deepEqual(failures, []);
  });
}

{
  const { test: evidenceTest } = require('node:test');
  const evidenceAssert = require('node:assert/strict');
  const { evidenceReplacer } = require('./refresher-restart-check');
  evidenceTest('evidence omits pod credentials and nested proof annotations without mutating runtime objects', () => {
    const pod = { metadata: { uid: 'pinned-uid', annotations: { lastApplied: 'sensitive' }, managedFields: ['sensitive'] },
      spec: { containers: [{ name: 'api', image: 'frozen-image', env: [{ name: 'API_KEY', value: 'sensitive' }],
        envFrom: [{ secretRef: { name: 'sensitive' } }] }] }, status: { phase: 'Running' } };
    const input = { before: { pod }, proof: { replacement: pod }, authorization: 'sensitive',
      token: 'sensitive', secret: 'sensitive', password: 'sensitive', manifest: { manifestHash: 'keep-hash' } };
    const serialized = JSON.stringify(input, evidenceReplacer);
    evidenceAssert.ok(!serialized.includes('sensitive'));
    const saved = JSON.parse(serialized);
    evidenceAssert.equal(saved.proof.replacement.metadata.uid, 'pinned-uid');
    evidenceAssert.equal(saved.before.pod.spec.containers[0].image, 'frozen-image');
    evidenceAssert.equal(saved.manifest.manifestHash, 'keep-hash');
    evidenceAssert.equal(pod.spec.containers[0].env[0].value, 'sensitive');
  });
}
