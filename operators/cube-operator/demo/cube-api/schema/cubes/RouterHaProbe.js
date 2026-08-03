const schema = process.env.ROUTER_HA_SCHEMA || 'router_ha_probe';
const table = process.env.ROUTER_HA_TABLE || 'router_ha_data';

cube('RouterHaProbe', {
  sql: `SELECT id, amount FROM ${schema}.${table}`,

  measures: {
    rowCount: { type: `count`, sql: `id` },
    totalAmount: { type: `sum`, sql: `amount` }
  },

  dimensions: {
    id: { sql: `id`, type: `number`, primaryKey: true },
    amount: { sql: `amount`, type: `number` }
  }
});
