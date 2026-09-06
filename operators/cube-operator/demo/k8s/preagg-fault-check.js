'use strict';

// Executed by kubectl exec -i <pinned API pod> -- node - < this-file.
const assert = require('assert/strict');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const { CubeStoreDriver } = require('@cubejs-backend/cubestore-driver');
const { WebSocketConnection } = require('@cubejs-backend/cubestore-driver/dist/src/WebSocketConnection');
const { QueryResultFormat, HttpMessage, HttpCommand, HttpError, HttpQuery } = require('@cubejs-backend/cubestore-driver/dist/codegen');
const flatbuffers = require('flatbuffers');
const { startProxy, requestJson } = require('/cube/conf/ha-fault-proxy');

const run = process.env.HA_RUN;
const mode = process.env.HA_MODE;
const scheduledMode = mode === 'refresher-failover';
const transfer = process.env.HA_TRANSFER || 'rows';
const rows = Number(process.env.HA_ROWS || 4096);
const timeout = Number(process.env.HA_TIMEOUT_SECONDS || 600) * 1000;
// Evidence only: mirror current settings without configuring any timer/driver.
const reconcileSetting = process.env.CUBE_STORE_PRE_AGGREGATION_RECONCILE_TIMEOUT_MS;
const effectiveBudgets = {
  totalRun: { milliseconds: timeout, source: process.env.HA_TIMEOUT_SECONDS ? 'HA_TIMEOUT_SECONDS' : 'runner-default' },
  failoverObservation: {
    milliseconds: process.env.FAILOVER_TIMEOUT_SECONDS ? Number(process.env.FAILOVER_TIMEOUT_SECONDS) * 1000 : null,
    source: process.env.FAILOVER_TIMEOUT_SECONDS ? 'controller-forwarded-FAILOVER_TIMEOUT_SECONDS' : 'not-forwarded-unknown',
  },
  uploadRequest: { milliseconds: 60000, source: 'driver-fixed-per-POST' },
  createReconcile: {
    milliseconds: Math.max(1, Number(reconcileSetting) || 120000),
    configuredValue: reconcileSetting ?? null,
    source: Number(reconcileSetting) ? 'CUBE_STORE_PRE_AGGREGATION_RECONCILE_TIMEOUT_MS' : 'driver-default',
    scope: 'per-CREATE-reconciliation-call',
  },
  loadTransientRetry: { milliseconds: 120000, source: 'runner-fixed-from-first-transient-clipped-by-total-deadline' },
};
const sourceSchema = `ha_${run}_source`;
const rollupSchema = `ha_${run}_rollups`;
const prefix = `PRE_AGG_BUILD_V1:${rollupSchema}.`;
const sourceTable = `${sourceSchema}.events`;
const emit = (event, detail = {}) => process.stdout.write(`${JSON.stringify({ kind: 'cube-ha-preagg', time: new Date().toISOString(), run, mode, event, ...detail })}\n`);
const digest = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
let driver;
let staleConnection;
let proxy;
let before;
let releaseProof;
let staleWriter;
let queryCompleted = false;
let passed = false;
let failure;
let scheduledHelpers;
let registrationKey;
let registrationRemoved = false;
let queueEvidence;
let readinessBefore;
let apiLoadStarted = false;
const apiAvailability = { count: 0, firstAt: null, lastAt: null, recoveredAt: null, windowMs: 0 };
const observationAvailability = { count: 0, firstAt: null, lastAt: null, recoveredAt: null, windowMs: 0 };
let failRun;
const failurePromise = new Promise((resolve, reject) => { failRun = reject; });
// Attach immediately; the same promise participates in the main race below.
failurePromise.catch(() => {});
const deadline = Date.now() + timeout;
const timer = setTimeout(() => failRun(new Error('E2E deadline exceeded; no unobserved fault can pass')), timeout);

function identity(record) {
  return { buildId: record.buildId, versionEntry: record.versionEntry, create: record.create,
    uploads: (record.uploads || []).map(({ name, sha256, size }) => ({ name, sha256, size })) };
}
async function manifests() {
  // CubeStore CACHE KEYS selects the colon-delimited namespace, not a
  // Redis-style arbitrary key prefix. Only inspect this run's returned keys.
  const keys = await observeCache('CACHE KEYS ?', ['PRE_AGG_BUILD_V1:*']);
  const result = [];
  for (const { key } of keys) {
    if (!key.startsWith(prefix)) continue;
    const data = await observeCache('CACHE GET ?', [key]);
    assert.equal(data.length, 1, 'Manifest disappeared');
    const buildIdentity = JSON.parse(data[0].value);
    assert.equal(key, `PRE_AGG_BUILD_V1:${buildIdentity.buildId}`);
    const read = async cacheKey => {
      const value = await observeCache('CACHE GET ?', [cacheKey]);
      return value.length ? JSON.parse(value[0].value) : null;
    };
    const immutableManifest = await read(`PRE_AGG_MANIFEST_V1:${buildIdentity.buildId}`);
    const markers = {};
    for (const phase of ['selected', 'uploading', 'uploaded', 'create', 'failed', 'ready', 'retired']) {
      const marker = await read(`PRE_AGG_PHASE_V1:${buildIdentity.buildId}:${phase}`);
      if (marker) markers[phase] = marker;
    }
    // Match PreAggregationBuildStore.read: retirement is terminal, then ready.
    const phase = ['retired', 'ready', 'failed', 'create', 'uploaded', 'uploading', 'selected'].find(item => markers[item]);
    const tableIdentity = await read(`PRE_AGG_TABLE_ID_V1:${buildIdentity.buildId}`);
    const record = { ...(markers[phase] || buildIdentity), ...(tableIdentity === null ? {} : { tableId: tableIdentity }) };
    result.push({ key, buildIdentity, immutableManifest, markers, tableIdentity, record });
  }
  return result.sort((a, b) => a.key.localeCompare(b.key));
}
function normalized(data) {
  const fields = ['RouterHaRollup.bucket', 'RouterHaRollup.rowCount', 'RouterHaRollup.totalAmount', 'RouterHaRollup.idChecksum'];
  return data.map(row => Object.fromEntries(fields.map(field => {
    assert.notEqual(row[field], undefined, `Missing ${field}`);
    return [field, String(row[field])];
  }))).sort((a, b) => a[fields[0]].localeCompare(b[fields[0]]));
}

// Load-only retry policy. Never used for SQL, uploads, or other mutations.
function transientLoadError(error) {
  const message = String(error.message || error);
  const status = error.statusCode;
  if (status === 401 || status === 403) return false;
  if (status === 502 || status === 503) return true;
  if (status && status !== 400 && status !== 500 && status !== 200) return false;
  if (/\b(?:ECONNRESET|ECONNREFUSED|EPIPE|ETIMEDOUT|EHOSTUNREACH|ENETUNREACH|EAI_AGAIN)\b/.test(error.code || '')) return true;
  return /^(?:(?:Error|Cube API):\s*)?(?:ConnectionError:\s*CubeStore connection error:|CubeStore connection error:|WrongConnection:\s*Router is draining\b|MutationUnknown(?:Error)?\b|NotLeader\b|LeaderNotReady\b)/i.test(message);
}

function requestLoad(url, options) {
  // Unlike the shared JSON helper, retain a 502/503 even for an HTML/empty body.
  return new Promise((resolve, reject) => {
    const bytes = Buffer.from(JSON.stringify(options.body));
    const req = require(url.protocol === 'https:' ? 'https' : 'http').request(url, {
      method: 'POST', headers: { ...options.headers, 'content-type': 'application/json', 'content-length': bytes.length },
    }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('error', reject);
      res.on('end', () => {
        const statusCode = res.statusCode;
        const text = Buffer.concat(chunks).toString();
        let value;
        try { value = JSON.parse(text); } catch (cause) {
          return reject(Object.assign(new Error(`HTTP ${statusCode}: invalid JSON response`), { statusCode }));
        }
        if (statusCode < 200 || statusCode >= 300 || value?.error) {
          return reject(Object.assign(new Error(typeof value?.error === 'string' ? value.error : `HTTP ${statusCode}: ${text.slice(0, 1000)}`), { statusCode }));
        }
        options.onSuccess?.(statusCode, value);
        resolve(value);
      });
    });
    const timer = setTimeout(() => req.destroy(Object.assign(new Error('Load connection timed out'), { code: 'ETIMEDOUT' })), options.timeout);
    req.on('close', () => clearTimeout(timer));
    req.on('error', reject);
    req.end(bytes);
  });
}

async function pollLoadResponse({ request, deadline, emit, availability, now = Date.now, sleep = pause }) {
  let attempts = 0;
  let retryDeadline = deadline;
  while (now() < retryDeadline) {
    attempts++;
    let result;
    try {
      result = await request(Math.min(45000, retryDeadline - now()));
      if (result?.error) throw Object.assign(new Error(result.error), { statusCode: 200 });
    } catch (error) {
      if (/^continue\s*wait$/i.test(error.message) && (!error.statusCode || [200, 400].includes(error.statusCode))) {
        await sleep(Math.min(250, Math.max(0, retryDeadline - now())));
        continue;
      }
      if (!transientLoadError(error)) throw error;
      const time = now();
      if (retryDeadline === deadline) retryDeadline = Math.min(deadline, time + 120000);
      availability.count++;
      availability.firstAt ??= time;
      availability.lastAt = time;
      availability.windowMs = time - availability.firstAt;
      emit('api_transient_unavailable', { ...availability, attempt: attempts,
        httpStatus: error.statusCode, code: error.code, error: error.message,
        retryDeadline: new Date(retryDeadline).toISOString() });
      await sleep(Math.min(250, Math.max(0, retryDeadline - now())));
      continue;
    }
    if (availability.count) {
      availability.recoveredAt = now();
      availability.windowMs = availability.recoveredAt - availability.firstAt;
      emit('api_availability_recovered', { ...availability, attempts });
    }
    return { result, attempts };
  }
  throw new Error(`Cube API load deadline exceeded (transient retries: ${availability.count}; maximum retry window 120000ms)`);
}

function transientObservationError(error) {
  const text = String(error.message || error);
  if (/permission|unauthori[sz]ed|forbidden|access denied|authentication/i.test(text)) return false;
  if (error.name === 'AssertionError' || error.name === 'MutationUnknownError' ||
      ['MUTATION_UNKNOWN', 'MUTATION_NOT_DISPATCHED'].includes(error.code)) return false;
  if ([502, 503].includes(error.statusCode)) return true;
  if (error.statusCode && ![200, 400, 500].includes(error.statusCode)) return false;
  if (/^HTTP (?:502|503) \/(?:router\/(?:status|build-status)|upload-temp-file-status):/.test(text)) return true;
  if (/^(?:ECONNRESET|ECONNREFUSED|EPIPE|ETIMEDOUT|EHOSTUNREACH|ENETUNREACH|EAI_AGAIN)$/.test(error.code || '')) return true;
  return /^(?:ConnectionError:\s*)?CubeStore connection error:\s*(?:WrongConnection:\s*(?:Router is draining|NotLeader|LeaderNotReady)\b|(?:ECONNRESET|ECONNREFUSED|EPIPE|ETIMEDOUT|EHOSTUNREACH|ENETUNREACH|EAI_AGAIN)\b)/i.test(text);
}

async function retryObservation({ operation, request, deadline, availability, emit, now = Date.now, sleep = pause }) {
  const cacheRead = operation.kind === 'cache' && ['CACHE GET ?', 'CACHE KEYS ?'].includes(operation.sql);
  const statusRead = operation.kind === 'status' && operation.method === 'GET' &&
    ['/router/build-status', '/router/status', '/upload-temp-file-status'].includes(operation.path);
  assert.ok(cacheRead || statusRead, 'Observation retries are restricted to explicit read-only operations');
  let failures = 0;
  while (now() < deadline) {
    try {
      const value = await request();
      if (failures) {
        availability.recoveredAt = now();
        availability.windowMs = availability.recoveredAt - availability.firstAt;
        emit('observation_recovered', { operation, failures, ...availability });
      }
      return value;
    } catch (error) {
      if (!transientObservationError(error)) {
        emit('observation_failed', { operation, error: error.message, name: error.name, code: error.code });
        throw error;
      }
      failures++;
      availability.count++;
      availability.firstAt ??= now();
      availability.lastAt = now();
      availability.windowMs = availability.lastAt - availability.firstAt;
      emit('observation_transient_unavailable', { operation, failures, ...availability,
        error: error.message, name: error.name, code: error.code, deadline });
      await sleep(Math.min(250, Math.max(0, deadline - now())));
    }
  }
  throw new Error(`Read-only observation deadline exceeded: ${JSON.stringify(operation)}`);
}

function observeCache(sql, values) {
  return retryObservation({ operation: { kind: 'cache', sql, key: values[0] },
    request: () => driver.query(sql, values), deadline, availability: observationAvailability, emit });
}

function assertStaleWriterFrameProof(proof) {
  assert.ok(proof.error, 'Old writer was accepted after drain');
  assert.equal(proof.sent?.command, 'HttpQuery', 'Missing actual outbound mutation frame');
  assert.equal(proof.sent.sql, proof.sql, 'Outbound mutation SQL changed');
  assert.equal(proof.sent.messageId, proof.messageId, 'Outbound mutation messageId changed');
  assert.equal(proof.response?.command, 'HttpError', 'A transport close/timeout is not a server error frame');
  assert.equal(proof.response.messageId, proof.messageId, 'Server refusal belongs to a different request');
  assert.equal(proof.response.error, 'WrongConnection: Router is draining', 'Server frame is not an explicit drain refusal');
  assert.equal(proof.error.name, 'ConnectionError');
  assert.equal(proof.error.message, `CubeStore connection error: ${proof.response.error}`);
  assert.ok(!proof.error.code, 'Transport/unknown mutation error codes are not drain fencing');
  assert.ok(!proof.captureError, 'Wire observation failed');
}

async function observeStaleWriter(connection, sql) {
  const socket = connection.webSocket;
  assert.ok(socket && socket.readyState === 1, 'Original pre-drain websocket must still be open');
  const proof = { sql, messageId: connection.messageCounter, sent: null, response: null, error: null };
  const decode = bytes => HttpMessage.getRootAsHttpMessage(new flatbuffers.ByteBuffer(Buffer.from(bytes)));
  const receive = bytes => {
    try {
      const message = decode(bytes);
      if (message.messageId() !== proof.messageId) return;
      proof.response = { messageId: message.messageId(), command: message.commandType() === HttpCommand.HttpError ? 'HttpError' : 'other',
        error: message.commandType() === HttpCommand.HttpError ? message.command(new HttpError()).error() : null,
        frameBase64: Buffer.from(bytes).toString('base64') };
    } catch (error) { proof.captureError = error.message; }
  };
  const send = socket.send;
  socket.send = function observedSend(bytes, ...args) {
    try {
      const message = decode(bytes);
      if (message.messageId() === proof.messageId) {
        proof.sent = { messageId: message.messageId(), command: message.commandType() === HttpCommand.HttpQuery ? 'HttpQuery' : 'other',
          sql: message.commandType() === HttpCommand.HttpQuery ? message.command(new HttpQuery()).query() : null,
          frameSha256: crypto.createHash('sha256').update(Buffer.from(bytes)).digest('hex') };
      }
    } catch (error) { proof.captureError = error.message; }
    return send.call(this, bytes, ...args); // Observe only: do not alter/replay bytes.
  };
  socket.on('message', receive);
  try {
    await connection.query(sql, [], { responseFormat: QueryResultFormat.Legacy });
  } catch (error) {
    proof.error = { name: error.name, message: error.message, code: error.code, stack: error.stack,
      cause: error.cause && { name: error.cause.name, message: error.cause.message, code: error.cause.code } };
  } finally {
    socket.removeListener('message', receive);
    socket.send = send;
  }
  return proof;
}

async function main() {
  emit('effective_budgets', { budgets: effectiveBudgets });
  assert.match(run || '', /^r[a-f0-9]{16}$/);
  assert.ok(['upload-failover', 'receipt-loss', 'receipt-failover', 'drain-failover', 'refresher-failover'].includes(mode));
  assert.ok(['rows', 'stream'].includes(transfer));
  assert.ok(Number.isInteger(rows) && rows >= 32 && rows <= 100000);
  assert.ok(Number.isFinite(timeout) && timeout >= 10000 && timeout <= 3600000);
  assert.equal(process.env.CUBEJS_HA_DEMO, 'true', 'API deployment must opt into demo configuration');
  assert.equal(process.env.CUBEJS_CACHE_AND_QUEUE_DRIVER, 'cubestore', 'Memory/Redis cache is not shared CubeStore cache proof');
  assert.ok(process.env.CUBEJS_API_SECRET, 'Use the operator-generated API secret already in the pod environment');
  const upstream = new URL(`http://${process.env.CUBEJS_CUBESTORE_HOST}:${process.env.CUBEJS_CUBESTORE_PORT || 3030}`);
  const buildStatus = table => retryObservation({ operation: { kind: 'status', method: 'GET', path: '/router/build-status', table },
    request: () => requestJson(new URL(`/router/build-status?table=${encodeURIComponent(table)}`, upstream)),
    deadline, availability: observationAvailability, emit });
  driver = new CubeStoreDriver();
  const apiBase = new URL(process.env.HA_API_URL || 'http://127.0.0.1:4000');
  if (scheduledMode) {
    // Lazy loading preserves compatibility with the already deployed r1 image.
    scheduledHelpers = require('/cube/conf/ha-scheduled-contexts');
    assert.equal(process.env.CUBEJS_HA_SCHEDULED_DEMO, 'true');
    assert.equal(process.env.CUBEJS_REFRESH_WORKER, 'true', 'Runner/proxy must execute in the real refresher pod');
    assert.ok(process.env.HA_EXECUTOR_UID && process.env.HA_API_POD_UID);
    assert.notEqual(process.env.HA_EXECUTOR_UID, process.env.HA_API_POD_UID, 'Refresher and API must be distinct pods');
    assert.equal(process.env.CUBEJS_HA_POD_UID, process.env.HA_EXECUTOR_UID, 'Downward API UID must match pinned executor');
    assert.notEqual(apiBase.hostname, '127.0.0.1', 'API observation must target the separate real API pod');
    readinessBefore = {
      api: await requestJson(new URL('/readyz', apiBase)),
      refresher: await requestJson(new URL('http://127.0.0.1:4000/readyz')),
    };
    assert.equal(readinessBefore.api.recoveryCapabilities?.fileImportBuildScheduling, false, 'Scheduled-mode API should consume, not schedule builds');
    assert.equal(readinessBefore.refresher.recoveryCapabilities?.fileImportBuildScheduling, true, 'Refresher must advertise real queue/build capability');
    assert.deepEqual(await scheduledHelpers.scheduledContexts(observeCache), [], 'A previous scheduled fault context is still active');
    emit('readiness_observed', { readinessBefore, apiPod: process.env.HA_API_POD_NAME, apiUid: process.env.HA_API_POD_UID,
      executorPod: process.env.HA_EXECUTOR_POD, executorUid: process.env.HA_EXECUTOR_UID, trigger: 'scheduled-refresh-warmup' });
  }
  assert.deepEqual(await manifests(), [], 'Run identity already has build manifests; refusing reuse');
  const absent = await buildStatus(`${rollupSchema}.never_created`);
  assert.equal(absent.state, 'absent', 'Authoritative build-status endpoint is required');
  await driver.query(`CREATE SCHEMA ${sourceSchema}`, []);
  await driver.query(`CREATE TABLE ${sourceTable} (id bigint, amount bigint, bucket text, checksum bigint)`, []);
  const expectedGroups = new Map();
  const salt = run.slice(-8);
  for (let offset = 0; offset < rows; offset += 256) {
    const values = [];
    const placeholders = [];
    for (let i = offset + 1; i <= Math.min(offset + 256, rows); i++) {
      const bucket = `${salt}_${String(i % 16).padStart(2, '0')}`;
      const amount = i * 17 + 3;
      values.push(i, amount, bucket, i * i);
      placeholders.push('(?, ?, ?, ?)');
      const group = expectedGroups.get(bucket) || { count: 0, amount: 0, checksum: 0 };
      group.count++; group.amount += amount; group.checksum += i * i;
      expectedGroups.set(bucket, group);
    }
    // Ordinary INSERT is dispatched once, with no caller retry or replay flag.
    await driver.query(`INSERT INTO ${sourceTable} (id, amount, bucket, checksum) VALUES ${placeholders.join(', ')}`, values);
  }
  const expected = normalized([...expectedGroups].map(([bucket, group]) => ({
    'RouterHaRollup.bucket': bucket, 'RouterHaRollup.rowCount': group.count,
    'RouterHaRollup.totalAmount': group.amount, 'RouterHaRollup.idChecksum': group.checksum,
  })));
  const raw = await driver.query(`SELECT bucket, COUNT(*) AS n, SUM(amount) AS total, SUM(checksum) AS checksum FROM ${sourceTable} GROUP BY bucket`, []);
  assert.deepEqual(normalized(raw.map(row => ({
    'RouterHaRollup.bucket': row.bucket, 'RouterHaRollup.rowCount': row.n,
    'RouterHaRollup.totalAmount': row.total, 'RouterHaRollup.idChecksum': row.checksum,
  }))), expected, 'Source table does not match independently generated data');
  emit('source_ready', { sourceTable, rollupSchema, rows, transfer, expectedHash: digest(expected), cache: 'cubestore' });

  if (mode === 'drain-failover') {
    assert.ok(process.env.HA_OLD_LEADER_IP, 'A pinned old leader IP is required');
    staleConnection = new WebSocketConnection(`ws://${process.env.HA_OLD_LEADER_IP}:3030/ws`);
    await staleConnection.query('SELECT 1', [], { responseFormat: QueryResultFormat.Legacy });
  }
  proxy = await startProxy({ upstream, run, mode,
    port: Number(process.env.CUBEJS_HA_PROXY_PORT || 13330),
    controlPort: Number(process.env.HA_CONTROL_PORT || 13331),
    onFailure: failRun,
    async onBarrier(upload) {
      assert.equal(queryCompleted, false, 'Query completed before the observed fault');
      before = await manifests();
      assert.equal(before.length, 1, 'Exactly one real rollup build must be active');
      const record = before[0].record;
      assert.equal(record.phase, 'uploading', 'Fault must happen during a real upload, not after pre-seeding');
      assert.ok(before[0].immutableManifest, 'Missing immutable NX upload/CREATE manifest');
      assert.ok(record.create && record.create.sql.includes(record.buildId));
      assert.ok(record.uploads.some(item => item.name === upload.name && item.sha256 === upload.sha256 && item.size === upload.size));
      const status = await buildStatus(record.buildId);
      assert.ok(['absent', 'building'].includes(status.state), `Rollup must not be ready at the fault: ${status.state}`);
      if (scheduledMode) {
        assert.equal(apiLoadStarted, false, 'API must not trigger the scheduled warmup build');
        const events = scheduledHelpers.readRuntimeEvents(run);
        assert.ok(events.some(event => event.message === 'Refresh Scheduler Run' && event.params.securityContext?.haRun === run), 'Missing real scheduler execution context');
        queueEvidence = events.find(event => event.message === 'Performing query' && scheduledHelpers.targetForVersion(event.params.newVersionEntry) === record.buildId);
        assert.ok(queueEvidence, 'No real acquired queue task matches the durable build target');
        assert.equal(queueEvidence.role, 'refresher');
        assert.equal(queueEvidence.podUid, process.env.HA_EXECUTOR_UID);
        assert.equal(queueEvidence.podName, process.env.HA_EXECUTOR_POD);
        assert.ok(queueEvidence.processUid && queueEvidence.params.queuePrefix && queueEvidence.params.queryKey);
        assert.notEqual(queueEvidence.params.processingId, undefined);
        assert.notEqual(queueEvidence.params.queueId, undefined);
      }
      emit('fault_ready', { upload, manifest: record, key: before[0].key, buildStatus: status, queryCompleted,
        trigger: scheduledMode ? 'scheduled-refresh-warmup' : 'api-on-demand', apiLoadStarted,
        executorPod: process.env.HA_EXECUTOR_POD, executorUid: process.env.HA_EXECUTOR_UID, queueEvidence });
    },
    async onStale() {
      assert.equal(mode, 'drain-failover');
      assert.ok(staleConnection && before, 'Old connection must exist before drain');
      let drainingStatus;
      const observeDeadline = Math.min(deadline, Date.now() + 5000);
      do {
        drainingStatus = await requestJson(new URL(`http://${process.env.HA_OLD_LEADER_IP}:3030/router/status`));
        if (drainingStatus.draining === true) break;
        await pause(100);
      } while (Date.now() < observeDeadline);
      assert.equal(drainingStatus.draining, true, 'Pinned old router must authoritatively report draining');
      assert.equal(drainingStatus.writeReady, false, 'Draining old router must reject admission');
      emit('draining_observed', { oldLeader: process.env.HA_OLD_LEADER, oldLeaderIp: process.env.HA_OLD_LEADER_IP, status: drainingStatus });
      const wireProof = await observeStaleWriter(staleConnection,
        `INSERT INTO ${sourceTable} (id, amount, bucket, checksum) VALUES (99999999, 1, 'stale_writer', 1)`);
      emit('stale_writer_response', { wireProof });
      assertStaleWriterFrameProof(wireProof);
      staleWriter = { rejected: true, error: wireProof.error.message, wireProof,
        oldLeaderIp: process.env.HA_OLD_LEADER_IP, connectionOpenedBeforeDrain: true, drainingStatus };
      emit('stale_writer_rejected', staleWriter);
      return staleWriter;
    },
    async onRelease(proof) {
      assert.ok(before && !queryCompleted, 'Release must precede successful API response');
      assert.equal(proof.oldLeader, process.env.HA_OLD_LEADER);
      assert.equal(Number(proof.oldEpoch), Number(process.env.HA_OLD_EPOCH));
      if (mode !== 'receipt-loss') {
        assert.notEqual(proof.newLeader, proof.oldLeader);
        assert.ok(Number(proof.newEpoch) > Number(proof.oldEpoch));
        assert.equal(proof.endpoint, proof.newLeader);
      }
      if (mode === 'drain-failover') {
        assert.ok(staleWriter && staleWriter.rejected);
        assert.equal(proxy.state.drainUploadAborted, true, 'Old admitted upload must end before drain completion');
        assert.equal(proof.drain?.cliExit, 0, 'HTTP 503/nonzero CLI is not drain completion');
        assert.equal(proof.drain.drainHttpSuccess, true, 'Independent old-pod drain POST must succeed');
        assert.equal(proof.drain.status.drained, true);
        assert.equal(proof.drain.status.draining, true);
        assert.equal(proof.drain.status.inFlight, 0);
        assert.equal(proof.drain.oldLeaderIp, process.env.HA_OLD_LEADER_IP);
        emit('drain_completed', { proof: proof.drain });
      }
      releaseProof = proof;
      emit('fault_released', { proof });
    },
  });
  if (scheduledMode) {
    registrationKey = `${scheduledHelpers.PREFIX}${run}`;
    const context = { securityContext: { haRun: run, haTransfer: transfer, haScheduled: true }, requestId: `ha-${run}` };
    const registration = { context, expiresAt: deadline, sourceTable, expectedHash: digest(expected) };
    // Publish only after the deterministic source and real worker proxy exist.
    // This is a real shared CacheStore write, not a synthetic scheduler call.
    await driver.query('CACHE SET NX TTL ? ? ?', [Math.ceil(timeout / 1000), registrationKey, JSON.stringify(registration)]);
    const registered = await observeCache('CACHE GET ?', [registrationKey]);
    assert.deepEqual(JSON.parse(registered[0].value), registration);
    emit('scheduled_context_registered', { registrationKey, registration });
    let ready = false;
    while (Date.now() < deadline) {
      const records = await manifests();
      if (records.some(item => ['failed', 'retired'].includes(item.record.phase))) throw new Error('Scheduled build reached a failed/retired phase');
      if (records.some(item => item.record.phase === 'ready')) {
        assert.ok(before && releaseProof && proxy.state.released, 'Scheduled build finished without the observed fault');
        ready = true;
        break;
      }
      await pause(250);
    }
    assert.ok(ready, 'Scheduled refresher did not finish within deadline');
    // Stop future scheduled cycles for this test before asking the API to read.
    await driver.query('CACHE REMOVE ?', [registrationKey]);
    registrationRemoved = true;
    emit('scheduled_warmup_ready', { buildId: before[0].record.buildId, queueEvidence });
  }
  const token = jwt.sign({ haRun: run, haTransfer: transfer, ...(scheduledMode ? { haScheduled: true } : {}) }, process.env.CUBEJS_API_SECRET, { expiresIn: Math.ceil(timeout / 1000) + 60 });
  const query = {
    measures: ['RouterHaRollup.rowCount', 'RouterHaRollup.totalAmount', 'RouterHaRollup.idChecksum'],
    dimensions: ['RouterHaRollup.bucket'], order: { 'RouterHaRollup.bucket': 'asc' },
  };
  async function load() {
      const requestId = `ha-${run}-load`;
      const { result, attempts } = await pollLoadResponse({ deadline, emit, availability: apiAvailability,
        request: requestTimeout => requestLoad(new URL('/cubejs-api/v1/load', apiBase), {
          body: { query }, headers: { authorization: `Bearer ${token}`, 'x-request-id': requestId }, timeout: requestTimeout,
          onSuccess: (httpStatus, response) => emit('api_response', { httpStatus, requestId,
            fields: response && Object.keys(response), dataRows: Array.isArray(response?.data) ? response.data.length : null,
            external: response?.external,
            hasUsedPreAggregations: Object.prototype.hasOwnProperty.call(response || {}, 'usedPreAggregations'),
            usedPreAggregations: response?.usedPreAggregations }),
        }),
      });
      assert.ok(Array.isArray(result.data), 'Cube API must return query data');
      assert.ok(before, 'Query succeeded without observing the requested upload fault');
      const helpers = require('/cube/conf/ha-scheduled-contexts');
      let apiEvents = [];
      if (!Object.prototype.hasOwnProperty.call(result, 'usedPreAggregations')) {
        if (scheduledMode) {
          // The controller copies logs from the pinned API pod, never the
          // refresher's own build logs. No production evidence endpoint added.
          const file = `/tmp/cube-ha-api-${run}.jsonl`;
          const evidenceDeadline = Math.min(deadline, Date.now() + 30000);
          while (Date.now() < evidenceDeadline) {
            if (require('fs').existsSync(file)) {
              apiEvents = require('fs').readFileSync(file, 'utf8').split('\n').filter(Boolean).map(line => JSON.parse(line));
              if (helpers.runtimeSqlEvidence(apiEvents, { run, buildId: before[0].record.buildId, requestId, podUid: process.env.HA_API_POD_UID })) break;
            }
            await pause(250);
          }
        } else apiEvents = helpers.readRuntimeEvents(run);
      }
      const provenance = helpers.preAggregationEvidence(result, apiEvents, {
        run, buildId: before[0].record.buildId, requestId,
        podUid: scheduledMode ? process.env.HA_API_POD_UID : process.env.CUBEJS_HA_POD_UID,
      });
      emit('preaggregation_evidence', { provenance });
      assert.deepEqual(normalized(result.data), expected, 'Pre-aggregation count/sum/hash mismatch');
      return { data: normalized(result.data), usedPreAggregations: result.usedPreAggregations, provenance, attempts };
  }
  apiLoadStarted = true;
  emit('query_started', { query, trigger: scheduledMode ? 'api-consumes-scheduled-rollup' : 'api-on-demand', apiUrl: apiBase.origin });
  const first = await load();
  queryCompleted = true;
  assert.ok(releaseProof && proxy.state.released, 'Requested fault was not completed');
  const after = await manifests();
  assert.deepEqual(after.map(item => item.key), before.map(item => item.key), 'Build identity forked during recovery');
  assert.deepEqual(after[0].buildIdentity, before[0].buildIdentity, 'Immutable CACHE build identity changed');
  assert.deepEqual(after[0].immutableManifest, before[0].immutableManifest, 'Immutable NX upload/CREATE fingerprint changed');
  assert.deepEqual(identity(after[0].record), identity(before[0].record), 'Immutable build manifest changed during recovery');
  assert.equal(after[0].record.phase, 'ready', 'Successful API response requires a ready durable manifest');
  assert.ok(after[0].markers.ready, 'Missing authoritative PRE_AGG_PHASE_V1:<target>:ready marker');
  assert.ok(!after[0].markers.retired, 'Retired build must not pass even with a historical ready marker');
  assert.equal(after[0].markers.ready.buildId, after[0].record.buildId);
  assert.equal(after[0].markers.ready.phase, 'ready');
  const status = await buildStatus(after[0].record.buildId);
  assert.equal(status.state, 'ready');
  assert.notEqual(status.tableId, undefined);
  assert.equal(String(after[0].record.tableId), String(status.tableId), 'Durable manifest/table identity mismatch');
  assert.equal(after[0].tableIdentity, String(status.tableId), 'Missing or changed immutable CACHE table identity');
  const imported = await driver.query(`SELECT COUNT(*) AS n FROM ${after[0].record.buildId}`, []);
  assert.equal(Number(imported[0].n), rows, 'Real rollup table is incomplete');
  const sourceCount = await driver.query(`SELECT COUNT(*) AS n FROM ${sourceTable}`, []);
  assert.equal(Number(sourceCount[0].n), rows, 'Stale writer changed source data');
  const fault = proxy.state.fault;
  const recoveredReceipt = proxy.state.receipts.find(item => item.afterFault && item.name === fault.name && item.state === 'uploaded' && item.sha256 === fault.sha256 && item.size === fault.size);
  assert.ok(recoveredReceipt, 'Driver did not reconcile the upload using a real post-fault receipt');
  if (mode.startsWith('receipt-')) {
    assert.equal(proxy.state.uploads.filter(item => item.name === fault.name).length, 1, 'Confirmed remote upload was blindly replayed after response loss');
  } else {
    assert.ok(proxy.state.uploads.some(item => item.name === fault.name && item.afterFault), 'Incomplete upload was not resumed');
  }
  const second = await load();
  assert.equal(digest(first.data), digest(second.data));
  const stable = await manifests();
  assert.deepEqual(stable, after, 'Completed build manifest changed on a renewed query');
  let readinessAfter;
  if (scheduledMode) {
    readinessAfter = { api: await requestJson(new URL('/readyz', apiBase)),
      refresher: await requestJson(new URL('http://127.0.0.1:4000/readyz')) };
    assert.equal(readinessAfter.api.recoveryCapabilities?.fileImportBuildScheduling, false);
    assert.equal(readinessAfter.refresher.recoveryCapabilities?.fileImportBuildScheduling, true);
  }
  emit('checks_passed', { expectedHash: digest(expected), actualHash: digest(first.data), rows,
    usedPreAggregations: first.usedPreAggregations, preAggregationEvidence: first.provenance,
    repeatedQueryEvidence: second.provenance, manifestBefore: before, manifestAfter: after,
    buildStatus: status, recoveredReceipt, transport: proxy.state, staleWriter: staleWriter || null, releaseProof,
    trigger: scheduledMode ? 'scheduled-refresh-warmup-then-api-consumption' : 'api-on-demand',
    queueEvidence, readinessBefore, readinessAfter, apiAvailability, observationAvailability,
    runtimeEvents: scheduledMode ? scheduledHelpers.readRuntimeEvents(run) : undefined });
  // Never enumerate/drop arbitrary tables. On failure keep evidence and data.
  // Cache manifests and content-addressed objects remain for parent inspection.
  if (process.env.HA_CLEANUP === '1') {
    for (const table of [after[0].record.buildId, sourceTable]) {
      assert.ok(table === sourceTable || table.startsWith(`${rollupSchema}.`));
      assert.match(table, /^[a-zA-Z0-9_.]+$/);
      await driver.query(`DROP TABLE ${table}`, []);
    }
    emit('cleanup', { droppedTables: [after[0].record.buildId, sourceTable], retained: 'schemas, CACHE manifests and uploaded objects' });
  }
  passed = true;
}

Promise.race([main(), failurePromise]).catch(error => {
  failure = error;
  emit('failure', { status: 'evidence_incomplete', error: error.stack || String(error),
    manifestBefore: before, transport: proxy && proxy.state, releaseProof, apiAvailability, observationAvailability });
}).finally(async () => {
  clearTimeout(timer);
  if (registrationKey && !registrationRemoved && driver) {
    await Promise.race([driver.query('CACHE REMOVE ?', [registrationKey]), pause(2000).then(() => { throw new Error('Context removal deadline'); })])
      .catch(error => emit('context_cleanup_incomplete', { registrationKey, error: error.message, expiresAt: deadline }));
  }
  if (proxy) proxy.close();
  if (staleConnection) staleConnection.close();
  if (driver) await Promise.race([driver.release(), pause(2000)]).catch(() => {});
  emit('result', { status: passed && !failure ? 'PASS' : 'FAIL', evidence: passed && !failure ? 'runtime_verified' : 'evidence_incomplete', effectiveBudgets });
  process.exit(passed && !failure ? 0 : 1);
});
