module.exports = {
  schemaPath: 'schema'
};

// Opt-in, signed per-request demo context. Ordinary RouterHaProbe requests keep
// their normal driver and schema. No fault behavior is installed in production.
if (process.env.CUBEJS_HA_DEMO === 'true') {
  const { CubeStoreDriver } = require('@cubejs-backend/cubestore-driver');
  const { BaseDriver } = require('@cubejs-backend/base-driver');
  const { Readable } = require('stream');
  const scheduledDemo = process.env.CUBEJS_HA_SCHEDULED_DEMO === 'true';
  const refresher = process.env.CUBEJS_REFRESH_WORKER === 'true';
  const { getProcessUid } = require('@cubejs-backend/shared');
  const { scheduledContexts, runtimeLogger } = require('./ha-scheduled-contexts');
  const runOf = ({ securityContext = {} }) => {
    if (!securityContext.haRun) return null;
    if (!/^r[a-f0-9]{16}$/.test(securityContext.haRun)) throw new Error('Invalid HA demo run');
    return securityContext.haRun;
  };
  class ReadOnlySource extends CubeStoreDriver {
    constructor(context) {
      super({ readOnly: true });
      this.transfer = context.securityContext.haTransfer || 'rows';
    }
    readOnly() { return true; }
    async downloadQueryResults(sql, params, options) {
      // Exercise the real rows -> gzip file recovery path. The optional stream
      // variant exercises the same external driver without synthetic SQL data.
      const result = await BaseDriver.prototype.downloadQueryResults.call(this, sql, params, options);
      return this.transfer === 'stream'
        ? { types: result.types, rowStream: Readable.from(result.rows) }
        : result;
    }
  }
  Object.assign(module.exports, {
    logger: runtimeLogger({
      role: refresher ? 'refresher' : 'api',
      podName: process.env.CUBEJS_HA_POD_NAME || process.env.HOSTNAME,
      podUid: process.env.CUBEJS_HA_POD_UID,
      processUid: getProcessUid(),
    }),
    contextToAppId: context => runOf(context) || 'STANDALONE',
    contextToOrchestratorId: context => runOf(context) || 'STANDALONE',
    preAggregationsSchema: context => runOf(context)
      ? `ha_${runOf(context)}_rollups` : 'dev_pre_aggregations',
    driverFactory: context => runOf(context) ? new ReadOnlySource(context) : new CubeStoreDriver(),
    externalDriverFactory: context => runOf(context)
      && !(context.securityContext.haScheduled === true && !refresher)
      ? new CubeStoreDriver({
        host: context.securityContext.haRestart === true
          ? (() => {
            if (!refresher || context.securityContext.haScheduled !== true ||
                !process.env.CUBEJS_HA_RESTART_PROXY_HOST) {
              throw new Error('Restart harness requires scheduled refresher and external proxy host');
            }
            return process.env.CUBEJS_HA_RESTART_PROXY_HOST;
          })() : '127.0.0.1',
        port: context.securityContext.haRestart === true ? '13332' : process.env.CUBEJS_HA_PROXY_PORT || '13330',
      })
      : new CubeStoreDriver(),
  });
  if (scheduledDemo) {
    const registryDriver = new CubeStoreDriver();
    Object.assign(module.exports, {
      // The API has no scheduler. Non-scheduled signed demo runs deliberately
      // retain the original four on-demand tests, even in this split topology.
      orchestratorOptions: context => ({ preAggregationsOptions: {
        externalRefresh: !refresher && (!runOf(context) || context.securityContext.haScheduled === true),
      } }),
      scheduledRefreshTimer: refresher ? 2 : false,
      scheduledRefreshContexts: () => refresher
        ? scheduledContexts((sql, values) => registryDriver.query(sql, values)) : Promise.resolve([]),
      scheduledRefreshConcurrency: 1,
      scheduledRefreshBatchSize: 1,
      scheduledRefreshTimeZones: ['UTC'],
    });
  }
}
