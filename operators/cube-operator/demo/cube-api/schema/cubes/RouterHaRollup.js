// COMPILE_CONTEXT is the schema sandbox's supported per-request context.
const run = COMPILE_CONTEXT.securityContext && COMPILE_CONTEXT.securityContext.haRun;
if (run) {
  if (!/^r[a-f0-9]{16}$/.test(run)) throw new Error('Invalid HA demo run');
  cube('RouterHaRollup', {
    sql: `SELECT id, amount, bucket, checksum FROM ha_${run}_source.events`,
    measures: {
      rowCount: { type: 'count', sql: 'id' },
      totalAmount: { type: 'sum', sql: 'amount' },
      idChecksum: { type: 'sum', sql: 'checksum' },
    },
    dimensions: {
      id: { sql: 'id', type: 'number', primaryKey: true, public: true },
      bucket: { sql: 'bucket', type: 'string' },
    },
    preAggregations: {
      byId: {
        type: 'rollup',
        external: true,
        readOnly: true,
        scheduledRefresh: COMPILE_CONTEXT.securityContext.haScheduled === true,
        refreshKey: { sql: 'SELECT 1' },
        measures: [rowCount, totalAmount, idChecksum],
        dimensions: [id, bucket],
      },
    },
  });
}
