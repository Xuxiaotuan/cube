cube('RouterHaProbe', {
  sql: `SELECT 1 AS value`,

  measures: {
    total: {
      sql: `value`,
      type: `sum`
    }
  },

  dimensions: {
    value: {
      sql: `value`,
      type: `number`
    }
  }
});
