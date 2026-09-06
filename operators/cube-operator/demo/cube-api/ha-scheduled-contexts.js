'use strict';

// Demo configuration only. The scheduler, queue and build implementation remain
// the real Cube code. No HTTP endpoint or simulated API result is added.
const fs = require('fs');
const path = require('path');
const PREFIX = 'HA_DEMO_CONTEXT_V1:';
const validRun = run => /^r[a-f0-9]{16}$/.test(run);
const eventFile = run => {
  if (!validRun(run)) throw new Error('Invalid scheduled demo run');
  return path.join('/tmp', `cube-ha-runtime-${run}.jsonl`);
};

async function scheduledContexts(query, now = Date.now()) {
  const keys = await query('CACHE KEYS ?', [PREFIX]);
  const contexts = [];
  for (const { key } of keys) {
    if (!key.startsWith(PREFIX) || !validRun(key.slice(PREFIX.length))) throw new Error('Invalid demo context key');
    const rows = await query('CACHE GET ?', [key]);
    if (!rows.length) continue;
    const registration = JSON.parse(rows[0].value);
    if (!Number.isFinite(registration.expiresAt)) throw new Error('Invalid demo context expiry');
    if (registration.expiresAt <= now) continue;
    const context = registration.context;
    if (!context || context.securityContext?.haRun !== key.slice(PREFIX.length) ||
        context.securityContext.haScheduled !== true ||
        !['rows', 'stream'].includes(context.securityContext.haTransfer)) {
      throw new Error('Invalid scheduled demo context');
    }
    contexts.push(context);
  }
  // Fixed loopback proxy port deliberately permits one active fault run only.
  if (contexts.length > 1) throw new Error('Run scheduled fault tests serially; multiple active contexts found');
  return contexts;
}

function targetForVersion(version) {
  if (!version || !version.table_name || !version.content_version || !version.structure_version || !Number.isFinite(version.last_updated_at)) return null;
  const stamp = version.naming_version === 2
    ? Math.floor(version.last_updated_at / 1000).toString(32) : version.last_updated_at;
  return `${version.table_name}_${version.content_version}_${version.structure_version}_${stamp}`;
}

function runtimeLogger(actor, write = (run, event) => {
  const file = eventFile(run);
  if (fs.existsSync(file) && fs.statSync(file).size > 4 * 1024 * 1024) throw new Error('Demo runtime log limit exceeded');
  fs.appendFileSync(file, `${JSON.stringify(event)}\n`, { mode: 0o600 });
}) {
  return (message, params = {}) => {
    const event = { kind: 'cube-ha-runtime', time: new Date().toISOString(), ...actor, message, params };
    try {
      const text = JSON.stringify(event, (_key, value) => typeof value === 'bigint' ? String(value) : value);
      console.log(text);
      if (!/Performing query|Executing SQL|Uploading external pre-aggregation|Refresh Scheduler Run|Refresh Scheduler Error/.test(message)) return;
      const runs = new Set(text.match(/r[a-f0-9]{16}(?![a-f0-9])/g) || []);
      for (const run of runs) write(run, { ...JSON.parse(text), run });
    } catch (error) {
      // Logging must not change queue execution; the harness fails closed when
      // this independently captured execution evidence is absent.
      console.error(`HA demo evidence log failure: ${error.message}`);
    }
  };
}

function readRuntimeEvents(run) {
  const file = eventFile(run);
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).map(line => JSON.parse(line));
}

function runtimeSqlEvidence(events, { run, buildId, requestId, podUid }) {
  if (!validRun(run) || !buildId.startsWith(`ha_${run}_rollups.`) || !/^[a-zA-Z0-9_.]+$/.test(buildId)) return null;
  const escaped = buildId.replace(/\./g, '\\.');
  const table = new RegExp(`\\b(?:FROM|JOIN)\\s+${escaped}(?![a-zA-Z0-9_.$])`, 'i');
  const belongs = event => event.run === run && event.role === 'api' && event.processUid && event.podName &&
    (!podUid || event.podUid === podUid) && typeof event.params?.requestId === 'string' &&
    (event.params.requestId === requestId || event.params.requestId.startsWith(`${requestId}-`));
  for (const executing of events.filter(event => event.message === 'Executing SQL' && belongs(event))) {
    const sql = executing.params.query;
    if (typeof sql !== 'string') continue;
    // Ignore comments/string literals: mentioning a table is not reading it.
    const select = sql.replace(/'(?:''|[^'])*'/g, "''").replace(/\/\*[\s\S]*?\*\//g, ' ')
      .replace(/--[^\n]*/g, ' ').replace(/["`]/g, '');
    if (!/^\s*SELECT\b/i.test(select) || !table.test(select)) continue;
    const same = event => belongs(event) && event.processUid === executing.processUid &&
      event.podName === executing.podName && event.params.requestId === executing.params.requestId &&
      JSON.stringify(event.params.queryKey) === JSON.stringify(executing.params.queryKey);
    const skipQueue = event => event.params.queueId === 0 && event.params.processingId === undefined &&
      event.params.queueSize === 0 && event.params.timeInQueue === 0;
    const started = events.find(event => event.message === 'Performing query' && same(event) &&
      event.params.queryKey !== undefined && event.params.newVersionEntry == null &&
      event.params.queuePrefix?.startsWith('SQL_QUERY_EXT_') && event.params.queuePrefix.includes(run) &&
      event.params.queueId != null && (event.params.processingId != null || skipQueue(event)) &&
      events.indexOf(event) < events.indexOf(executing));
    if (!started) continue;
    const completed = events.find(event => event.message === 'Performing query completed' && same(event) &&
      event.params.queuePrefix === started.params.queuePrefix && event.params.queueId === started.params.queueId &&
      event.params.processingId === started.params.processingId &&
      (started.params.processingId != null || skipQueue(event)) &&
      events.indexOf(event) > events.indexOf(executing));
    if (!completed) continue;
    return { source: 'runtime-sql', run, buildId, external: true, externalSource: 'SQL_QUERY_EXT_ queue',
      // CubeStore reads default to QueryQueue.processQuerySkipQueue. Its
      // completed event is emitted only after the real SQL handler succeeds,
      // but it must not be represented as a shared-queue lock acquisition.
      executionMode: skipQueue(started) ? 'skip-queue' : 'queued',
      sharedQueueAcquired: !skipQueue(started),
      requestId: executing.params.requestId, podName: executing.podName, podUid: executing.podUid,
      processUid: executing.processUid, sql, queuePrefix: started.params.queuePrefix,
      queueId: started.params.queueId, processingId: started.params.processingId,
      execution: { started, executing, completed } };
  }
  return null;
}

function preAggregationEvidence(result, events, expected) {
  if (Object.prototype.hasOwnProperty.call(result, 'usedPreAggregations')) {
    if (!result.usedPreAggregations || !Object.keys(result.usedPreAggregations).length ||
        !JSON.stringify(result.usedPreAggregations).includes(expected.buildId)) {
      throw new Error('API debug metadata must use the exact durable build target');
    }
    return { source: 'api-debug', buildId: expected.buildId, usedPreAggregations: result.usedPreAggregations };
  }
  const evidence = runtimeSqlEvidence(events, expected);
  if (!evidence) throw new Error('Missing exact external rollup SELECT execution evidence for this run/request');
  return evidence;
}

module.exports = { PREFIX, validRun, eventFile, scheduledContexts, targetForVersion, runtimeLogger, readRuntimeEvents,
  runtimeSqlEvidence, preAggregationEvidence };
