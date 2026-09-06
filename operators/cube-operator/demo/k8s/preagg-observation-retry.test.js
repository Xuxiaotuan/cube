'use strict';

// PURE observer fixtures, never scheduler/CubeStore runtime evidence.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, 'preagg-fault-check.js'), 'utf8');
const sandbox = { assert };
vm.runInNewContext(source.slice(source.indexOf('function transientObservationError('), source.indexOf('function observeCache(')), sandbox);
const draining = () => Object.assign(new Error('CubeStore connection error: WrongConnection: Router is draining'), { name: 'ConnectionError' });
const operation = { kind: 'cache', sql: 'CACHE GET ?', key: 'PRE_AGG_PHASE_V1:ha_run.table:ready' };
function fixture(values, op = operation, deadline = 1000) {
  let time = 0; let calls = 0;
  const events = [];
  const availability = { count: 0, firstAt: null, lastAt: null, recoveredAt: null, windowMs: 0 };
  return { events, availability, calls: () => calls, run: () => sandbox.retryObservation({
    operation: op, deadline, availability, emit: (event, details) => events.push({ event, details }),
    now: () => time, sleep: async ms => { time += ms; },
    request: async () => { const v = values[Math.min(calls++, values.length - 1)]; if (v instanceof Error) throw v; return v; },
  }) };
}

test('PURE observation: real draining read retries same key and records recovery under original deadline', async () => {
  const rows = [{ value: '{"phase":"ready"}' }];
  const f = fixture([draining(), rows]);
  assert.equal(await f.run(), rows);
  assert.equal(f.calls(), 2);
  assert.equal(f.availability.count, 1);
  assert.equal(f.availability.windowMs, 250);
  assert.equal(f.events[0].event, 'observation_transient_unavailable');
  assert.equal(f.events[0].details.operation, operation);
  assert.equal(f.events[1].event, 'observation_recovered');
});

test('PURE observation: mutations are rejected before invoking any request', async () => {
  for (const op of [
    { kind: 'cache', sql: 'CACHE REMOVE ?', key: 'run' },
    { kind: 'cache', sql: 'CACHE SET NX ? ?', key: 'run' },
    { kind: 'cache', sql: 'INSERT INTO source VALUES (?)' },
    { kind: 'status', method: 'POST', path: '/router/drain' },
  ]) { const f = fixture([[]], op); await assert.rejects(f.run(), /restricted to explicit read-only/); assert.equal(f.calls(), 0); }
});

test('PURE observation: original deadline bounds repeated transients, GET status is allowed', async () => {
  const f = fixture([draining()]);
  await assert.rejects(f.run(), /observation deadline exceeded/);
  assert.equal(f.calls(), 4);
  const ready = { state: 'ready', tableId: 1 };
  const g = fixture([Object.assign(new Error('upstream unavailable'), { statusCode: 503 }), ready],
    { kind: 'status', method: 'GET', path: '/router/build-status', table: 'ha_run.table' });
  assert.equal(await g.run(), ready);
});

test('PURE observation: permission/identity/SQL/mutation unknown errors fail immediately; failed states remain visible', async () => {
  for (const error of [
    new Error('Permission denied'), new Error('Immutable manifest conflict'), new Error('Table identity changed'), new Error('SQL syntax error'),
    Object.assign(draining(), { name: 'MutationUnknownError', code: 'MUTATION_UNKNOWN' }),
    Object.assign(new Error('forbidden'), { statusCode: 403 }),
    new Error('CubeStore connection error: permission denied'),
    Object.assign(new Error('ready mismatch'), { name: 'AssertionError' }),
  ]) { const f = fixture([error]); await assert.rejects(f.run()); assert.equal(f.calls(), 1); assert.equal(f.availability.count, 0); }
  for (const state of ['failed', 'retired']) {
    const value = { state }; const f = fixture([value]); assert.equal(await f.run(), value); assert.equal(f.calls(), 1);
  }
});
