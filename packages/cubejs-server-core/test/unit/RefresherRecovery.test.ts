import { RefreshScheduler } from '../../src/core/RefreshScheduler';
import { OrchestratorApi } from '../../src/core/OrchestratorApi';

jest.mock('@cubejs-backend/query-orchestrator', () => ({
  QueryOrchestrator: jest.fn(),
  ContinueWaitError: class extends Error {},
  PreAggregationPartitionRangeLoader: {},
}));

describe('single refresher durable recovery scheduling', () => {
  function harness() {
    const api = {
      recoverPreAggregationBuilds: jest.fn().mockResolvedValue(true),
      updateRefreshEndReached: jest.fn(),
    };
    const core = { logger: jest.fn(), getOrchestratorApi: jest.fn().mockResolvedValue(api), getCompilerApi: jest.fn().mockResolvedValue({}) };
    const scheduler = new RefreshScheduler(core as any);
    jest.spyOn(scheduler as any, 'refreshPreAggregations').mockResolvedValue(undefined);
    jest.spyOn(scheduler as any, 'forceReconcile').mockResolvedValue(undefined);
    return { api, core, scheduler };
  }

  it('never invokes global recovery before context-selected scheduled work', async () => {
    const { api, scheduler } = harness();
    api.recoverPreAggregationBuilds.mockImplementation(() => { throw new Error('Cross-context recovery forbidden'); });
    await expect(scheduler.runScheduledRefresh(null, { concurrency: 1, preAggregationsWarmup: true })).resolves.toEqual({ finished: true });
    expect(api.recoverPreAggregationBuilds).not.toHaveBeenCalled();
    expect((scheduler as any).refreshPreAggregations).toHaveBeenCalledTimes(1);
  });

  it('keeps an UNKNOWN iterator in place and does not mark refresh end reached', async () => {
    const { api, scheduler } = harness();
    (scheduler as any).refreshPreAggregations.mockRestore();
    const unknown = { code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError' };
    const preAggs = { getPreAggBackoff: jest.fn().mockResolvedValue(null), updatePreAggBackoff: jest.fn() };
    Object.assign(api, {
      executeQuery: jest.fn().mockRejectedValueOnce(unknown).mockResolvedValue(undefined),
      getQueryOrchestrator: () => ({ getPreAggregations: () => preAggs }),
    });
    const iterator = { current: async () => ({ preAggregations: [{ tableName: 's.rollup' }] }), partitionCounter: () => 0, advance: jest.fn().mockResolvedValue(false) };
    jest.spyOn(scheduler as any, 'roundRobinRefreshPreAggregationsQueryIterator').mockResolvedValue(iterator);
    const options = { concurrency: 1, workerIndices: [0], queryIteratorState: {} };
    const context = { securityContext: {} };
    await expect((scheduler as any).refreshPreAggregations(context, {}, options)).rejects.toBe(unknown);
    expect(iterator.advance).not.toHaveBeenCalled();
    expect(preAggs.updatePreAggBackoff).not.toHaveBeenCalled();
    expect(api.updateRefreshEndReached).not.toHaveBeenCalled();
    await (scheduler as any).refreshPreAggregations(context, {}, options);
    expect(iterator.advance).toHaveBeenCalledTimes(1);
  });

  it('preserves UNKNOWN across OrchestratorApi serialization', async () => {
    const api = Object.create(OrchestratorApi.prototype) as OrchestratorApi;
    Object.assign(api, {
      logger: jest.fn(), options: {}, continueWaitTimeout: 1,
      orchestrator: { fetchQuery: async () => { throw Object.assign(new Error('lost'), { code: 'MUTATION_UNKNOWN' }); } },
    });
    await expect(api.executeQuery({ scheduledRefresh: true } as any)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError' });
  });

  it('requires no external recovery capability and reports current scheduled UNKNOWN as unfinished', async () => {
    const { api, scheduler } = harness();
    delete (api as any).recoverPreAggregationBuilds;
    await expect(scheduler.runScheduledRefresh(null, { concurrency: 1, preAggregationsWarmup: true })).resolves.toEqual({ finished: true });
    const unknown = Object.assign(new Error('unresolved current build'), { code: 'MUTATION_UNKNOWN' });
    (scheduler as any).refreshPreAggregations.mockRejectedValue(unknown);
    await expect(scheduler.runScheduledRefresh(null, { concurrency: 1, preAggregationsWarmup: true })).resolves.toEqual({ finished: false });
    await expect(scheduler.runScheduledRefresh(null, { concurrency: 1, preAggregationsWarmup: true, throwErrors: true })).rejects.toBe(unknown);
  });
});
