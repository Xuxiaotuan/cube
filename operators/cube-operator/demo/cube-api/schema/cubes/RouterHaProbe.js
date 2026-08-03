// Cube schema files run inside Cube's sandbox, where Node's `process` global
// is intentionally unavailable. Keep the E2E fixture deterministic instead
// of reading environment variables from the schema compiler.
const schema = 'router_ha_probe';
const table = 'router_ha_data';

cube('RouterHaProbe', {
  sql: `SELECT id, amount, region, payload FROM ${schema}.${table}`,

  measures: {
    rowCount: { type: `count`, sql: `id` },
    totalAmount: { type: `sum`, sql: `amount` }
  },

  dimensions: {
    id: { sql: `id`, type: `number`, primaryKey: true, public: true },
    amount: { sql: `amount`, type: `number` },
    region: { sql: `region`, type: `string` },
    payload: { sql: `payload`, type: `string` }
  }
});
