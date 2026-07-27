cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`,
      title: `总金额`
    },
    avgAmount: {
      sql: `amount`,
      type: `avg`,
      title: `平均金额`
    },
    maxAmount: {
      sql: `amount`,
      type: `max`
    },
    minAmount: {
      sql: `amount`,
      type: `min`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    status: {
      sql: `status`,
      type: `string`,
      title: `状态`
    },
    amount: {
      sql: `amount`,
      type: `number`,
      title: `金额`
    },
    createdAt: {
      sql: `created_at`,
      type: `time`,
      title: `创建时间`
    }
  },

  segments: {
    completed: {
      sql: `${CUBE}.status = 'completed'`,
      title: `已完成`
    },
    cancelled: {
      sql: `${CUBE}.status = 'cancelled'`,
      title: `已取消`
    }
  }
});
