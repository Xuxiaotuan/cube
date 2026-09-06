'use strict';

// PURE configuration/registry tests. No mocked endpoint is reported as runtime
// Cube API, scheduler, queue or pre-aggregation evidence.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const helpers = require('./ha-scheduled-contexts');
const run = 'r0123456789abcdef';
const context = { securityContext: { haRun: run, haTransfer: 'rows', haScheduled: true }, requestId: `ha-${run}` };
const registryQuery = entries => async (sql, values) => {
  if (sql === 'CACHE KEYS ?') {
    // CubeStore indexes the namespace before the final colon, not arbitrary
    // Redis-style key prefixes. parse_path_to_prefix accepts both ':' and ':*'.
    const namespace = values[0].replace(/:\*?$/, '');
    return Object.keys(entries).filter(key => {
      const separator = key.lastIndexOf(':');
      return (separator < 0 ? '' : key.slice(0, separator)) === namespace;
    }).map(key => ({ key }));
  }
  if (sql === 'CACHE GET ?') return entries[values[0]] ? [{ value: JSON.stringify(entries[values[0]]) }] : [];
  throw new Error(`Unexpected registry mutation: ${sql}`);
};

test('PURE registry: CubeStore namespace lookup accepts colon/star but not a partial key', async () => {
  const key = `${helpers.PREFIX}${run}`;
  const query = registryQuery({
    [key]: { context, expiresAt: 200 },
    [`${helpers.PREFIX}nested:${run}`]: { context, expiresAt: 200 },
  });
  assert.deepEqual(await query('CACHE KEYS ?', [helpers.PREFIX]), [{ key }]);
  assert.deepEqual(await query('CACHE KEYS ?', [`${helpers.PREFIX}*`]), [{ key }]);
  assert.deepEqual(await query('CACHE KEYS ?', [`${helpers.PREFIX}${run.slice(0, 5)}`]), []);
  assert.deepEqual(await helpers.scheduledContexts(query, 100), [context]);
});

test('PURE registry: returns only the registered unexpired scheduled context', async () => {
  const entries = {
    [`${helpers.PREFIX}${run}`]: { context, expiresAt: 200 },
    [`${helpers.PREFIX}r1111111111111111`]: { expiresAt: 99 },
    UNRELATED: { context, expiresAt: 1000 },
  };
  assert.deepEqual(await helpers.scheduledContexts(registryQuery(entries), 100), [context]);
  assert.deepEqual(await helpers.scheduledContexts(registryQuery(entries), 300), []);
});

test('PURE registry: malformed and concurrent registrations fail closed', async () => {
  await assert.rejects(helpers.scheduledContexts(registryQuery({ [`${helpers.PREFIX}${run}`]: { context: {}, expiresAt: 200 } }), 100), /Invalid scheduled/);
  const second = 'r1111111111111111';
  await assert.rejects(helpers.scheduledContexts(registryQuery({
    [`${helpers.PREFIX}${run}`]: { context, expiresAt: 200 },
    [`${helpers.PREFIX}${second}`]: { context: { securityContext: { ...context.securityContext, haRun: second } }, expiresAt: 200 },
  }), 100), /serially/);
});

test('PURE evidence: target derivation matches durable version identity, logger keeps queue and actor', () => {
  const version = { table_name: `ha_${run}_rollups.by_id`, content_version: 'cv', structure_version: 'sv', naming_version: 2, last_updated_at: 1000 };
  assert.equal(helpers.targetForVersion(version), `ha_${run}_rollups.by_id_cv_sv_1`);
  assert.equal(helpers.targetForVersion({ ...version, naming_version: 1 }), `ha_${run}_rollups.by_id_cv_sv_1000`);
  assert.equal(helpers.targetForVersion({}), null);
  const captured = [];
  const logger = helpers.runtimeLogger({ role: 'refresher', podName: 'builder', podUid: 'uid', processUid: 'process' }, (id, event) => captured.push({ id, event }));
  logger('Performing query', { newVersionEntry: version, queueId: 'qid', processingId: 'pid', queuePrefix: `SQL_PRE_AGGREGATIONS_${run}`, queryKey: ['build'] });
  assert.equal(captured.length, 1);
  assert.equal(captured[0].id, run);
  assert.equal(captured[0].event.podUid, 'uid');
  assert.equal(captured[0].event.params.queueId, 'qid');
  assert.equal(helpers.targetForVersion(captured[0].event.params.newVersionEntry), `ha_${run}_rollups.by_id_cv_sv_1`);
});

const configurationSource = fs.readFileSync(path.join(__dirname, 'cube.js'), 'utf8');
function configuration(env) {
  class Driver { constructor(config) { this.config = config; } async query() { return []; } }
  class BaseDriver {}
  const sandbox = { module: { exports: {} }, process: { env }, require(name) {
    if (name === '@cubejs-backend/cubestore-driver') return { CubeStoreDriver: Driver };
    if (name === '@cubejs-backend/base-driver') return { BaseDriver };
    if (name === '@cubejs-backend/shared') return { getProcessUid: () => 'test-process' };
    if (name === './ha-scheduled-contexts') return helpers;
    if (name === 'stream') return require('node:stream');
    throw new Error(`Unexpected configuration dependency: ${name}`);
  } };
  vm.runInNewContext(configurationSource, sandbox);
  return sandbox.module.exports;
}

test('PURE config: scheduled API consumes directly, does not schedule; original on-demand still uses proxy', () => {
  const config = configuration({ CUBEJS_HA_DEMO: 'true', CUBEJS_HA_SCHEDULED_DEMO: 'true', CUBEJS_REFRESH_WORKER: 'false' });
  assert.equal(config.scheduledRefreshTimer, false);
  assert.equal(config.orchestratorOptions(context).preAggregationsOptions.externalRefresh, true);
  assert.equal(config.orchestratorOptions({}).preAggregationsOptions.externalRefresh, true);
  assert.equal(config.externalDriverFactory(context).config, undefined);
  const onDemand = { securityContext: { haRun: run } };
  assert.equal(config.orchestratorOptions(onDemand).preAggregationsOptions.externalRefresh, false);
  assert.equal(config.externalDriverFactory(onDemand).config.host, '127.0.0.1');
});

test('PURE config: real refresher role enables timer/builds and uses its own loopback proxy', async () => {
  const config = configuration({ CUBEJS_HA_DEMO: 'true', CUBEJS_HA_SCHEDULED_DEMO: 'true', CUBEJS_REFRESH_WORKER: 'true' });
  assert.equal(config.scheduledRefreshTimer, 2);
  assert.equal(config.orchestratorOptions(context).preAggregationsOptions.externalRefresh, false);
  assert.equal(config.orchestratorOptions({}).preAggregationsOptions.externalRefresh, false);
  assert.equal(config.externalDriverFactory(context).config.host, '127.0.0.1');
  assert.deepEqual(await config.scheduledRefreshContexts(), []);
});

test('PURE config: legacy four-mode configuration does not require scheduled registry', () => {
  const config = configuration({ CUBEJS_HA_DEMO: 'true' });
  assert.equal(config.scheduledRefreshContexts, undefined);
  assert.equal(config.orchestratorOptions, undefined);
  assert.equal(typeof config.logger, 'function');
  assert.equal(config.externalDriverFactory({ securityContext: { haRun: run } }).config.host, '127.0.0.1');
});

test('PURE SQL provenance: real logger joins completed external SELECT, never a preaggregation build', () => {
  const buildId = `ha_${run}_rollups.by_id_cv_sv_1`;
  const requestId = `ha-${run}-load`;
  const query = `SELECT bucket, sum(amount) FROM "ha_${run}_rollups"."by_id_cv_sv_1" GROUP BY bucket`;
  const queryKey = [query, []];
  const events = [];
  const logger = helpers.runtimeLogger({ role: 'api', podName: 'api-pod', podUid: 'api-uid', processUid: 'api-process' }, (_run, event) => events.push(event));
  const params = { queuePrefix: `SQL_QUERY_EXT_${run}`, queueId: 'q1', processingId: 'p1', requestId, queryKey };
  logger('Performing query', params);
  logger('Executing SQL', { query, queryKey, requestId, values: [] });
  logger('Performing query completed', params);
  const expected = { run, buildId, requestId, podUid: 'api-uid' };
  const evidence = helpers.preAggregationEvidence({ data: [] }, events, expected);
  assert.equal(evidence.source, 'runtime-sql');
  assert.equal(evidence.external, true);
  assert.equal(evidence.buildId, buildId);
  assert.equal(evidence.sql, query);
  assert.equal(evidence.usedPreAggregations, undefined);
  assert.equal(events[1].run, run);
  for (const mutate of [
    e => e.filter(item => item.message !== 'Performing query completed'),
    e => e.map(item => ({ ...item, role: 'refresher' })),
    e => e.map(item => ({ ...item, podUid: 'other' })),
    e => e.map(item => ({ ...item, params: { ...item.params, requestId: 'other' } })),
    e => e.map(item => ({ ...item, params: { ...item.params, queuePrefix: `SQL_PRE_AGGREGATIONS_${run}` } })),
    e => e.map(item => ({ ...item, params: { ...item.params, queuePrefix: `SQL_QUERY_${run}` } })),
    e => e.map(item => ({ ...item, params: { ...item.params, query: `SELECT 'FROM ${buildId}' FROM source.events` } })),
    e => e.map(item => ({ ...item, params: { ...item.params, query: `SELECT * FROM ${buildId}_wrong` } })),
    e => e.map(item => ({ ...item, params: { ...item.params, query: `CREATE TABLE ${buildId} AS SELECT 1` } })),
  ]) assert.throws(() => helpers.preAggregationEvidence({ data: [] }, mutate(events), expected), /Missing exact/);
  assert.throws(() => helpers.preAggregationEvidence({ data: [] }, [], expected), /Missing exact/);
  assert.throws(() => helpers.preAggregationEvidence({ usedPreAggregations: {} }, events, expected), /debug metadata/);
  assert.throws(() => helpers.preAggregationEvidence({ usedPreAggregations: { wrong: true } }, events, expected), /debug metadata/);
  assert.equal(helpers.preAggregationEvidence({ usedPreAggregations: { [buildId]: {} } }, [], expected).source, 'api-debug');
});

const rollupSource = fs.readFileSync(path.join(__dirname, 'schema/cubes/RouterHaRollup.js'), 'utf8');
test('CAPTURED runtime fixture: CubeStore skip-queue SELECT requires ordered successful execution', () => {
  const fixture = require('./runtime-sql-receipt.fixture.json');
  const expected = fixture.expected;
  const evidence = helpers.preAggregationEvidence({ data: [], external: true }, fixture.events, expected);
  assert.equal(evidence.source, 'runtime-sql');
  assert.equal(evidence.executionMode, 'skip-queue');
  assert.equal(evidence.sharedQueueAcquired, false);
  assert.equal(evidence.external, true);
  assert.equal(evidence.buildId, expected.buildId);
  assert.equal(evidence.queueId, 0);
  assert.equal(evidence.processingId, undefined);
  assert.equal(evidence.usedPreAggregations, undefined);
  for (const mutate of [
    e => e.slice(0, 2),
    e => [e[2], e[1], e[0]],
    e => e.map(item => ({ ...item, run: 'r1111111111111111' })),
    e => e.map(item => ({ ...item, params: { ...item.params, requestId: 'other-request' } })),
    e => e.map(item => ({ ...item, params: { ...item.params, queuePrefix: `SQL_PRE_AGGREGATIONS_${expected.run}` } })),
    e => e.map(item => ({ ...item, params: { ...item.params, queueId: 42 } })),
    e => e.map(item => ({ ...item, params: { ...item.params, queueSize: 1 } })),
    e => e.map((item, i) => i === 2 ? { ...item, processUid: 'other-process' } : item),
    e => e.map((item, i) => i === 2 ? { ...item, params: { ...item.params, queryKey: ['wrong-sql', []] } } : item),
    e => e.map((item, i) => i === 1 ? { ...item, params: { ...item.params, query: item.params.query.replace(expected.buildId, `${expected.buildId}_wrong`) } } : item),
  ]) assert.throws(() => helpers.preAggregationEvidence({ data: [], external: true }, mutate(fixture.events), expected), /Missing exact/);
});

const probeSource = fs.readFileSync(path.join(__dirname, 'schema/cubes/RouterHaProbe.js'), 'utf8');
test('PURE schema: only registered scheduled runs enable refresh, legacy fixture is excluded for every run', () => {
  const compile = securityContext => {
    const cubes = {};
    const symbols = { COMPILE_CONTEXT: { securityContext }, cube: (name, model) => { cubes[name] = model; }, rowCount: 'rowCount', totalAmount: 'totalAmount', idChecksum: 'idChecksum', id: 'id', bucket: 'bucket' };
    vm.runInNewContext(rollupSource, symbols);
    vm.runInNewContext(probeSource, symbols);
    return cubes;
  };
  assert.equal(compile(context.securityContext).RouterHaRollup.preAggregations.byId.scheduledRefresh, true);
  assert.equal(compile({ haRun: run }).RouterHaRollup.preAggregations.byId.scheduledRefresh, false);
  assert.equal(compile(context.securityContext).RouterHaProbe, undefined);
  assert.ok(compile({}).RouterHaProbe);
});
