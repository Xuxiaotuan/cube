import { PreAggregationLoader } from '../../src/orchestrator/PreAggregationLoader';
import { PreAggregations } from '../../src/orchestrator/PreAggregations';
import { QueryQueue } from '../../src/orchestrator/QueryQueue';

const entry = { table_name: 's.rollup', structure_version: 'aaaa1111', content_version: 'bbbb2222', last_updated_at: 1600000000000, naming_version: 2 };
const target = PreAggregations.targetTableName(entry);

function setup() {
  const driver = { resumePreAggregationBuild: jest.fn().mockResolvedValue(false), getProtectedPreAggregationTables: jest.fn().mockResolvedValue([]), dropTable: jest.fn(), getTablesQuery: jest.fn() };
  const preAggregations = {
    externalDriverFactory: async () => driver,
    structureVersionPersistTime: 0,
    updateLastTouch: jest.fn().mockResolvedValue(undefined),
    removeTableTouched: jest.fn().mockResolvedValue(undefined),
    removeTableUsed: jest.fn().mockResolvedValue(undefined),
    addTableUsed: jest.fn().mockResolvedValue(undefined),
    getRefreshEndReached: jest.fn().mockResolvedValue(true),
    dropPreAggregationsWithoutTouch: true,
    tablesUsed: jest.fn().mockResolvedValue([]),
    tablesTouched: jest.fn().mockResolvedValue([]),
  };
  const cache = { withLock: async (_key: string, _ttl: number, fn: () => Promise<any>) => fn() };
  const loadCache = { fetchTables: jest.fn().mockResolvedValue([]) };
  const loader = new PreAggregationLoader(async () => driver as any, jest.fn(), cache as any, preAggregations as any,
    { tableName: 's.rollup', preAggregationsSchema: 's', external: true, readOnly: true }, [], loadCache as any);
  return { loader, preAggregations, driver, loadCache };
}

describe('pre-aggregation queue/refresher failover', () => {
  it.each([true, false])('keeps timed-out durable work UNKNOWN and resumes the same identity (skipQueue=%s)', async skipQueue => {
    let release!: (value: string) => void;
    const dispatched = new Promise<string>(resolve => { release = resolve; });
    const logger = jest.fn();
    const cancel = jest.fn().mockResolvedValue(undefined);
    const handler = jest.fn().mockImplementationOnce(async (_query, setCancel) => {
      if (skipQueue) await setCancel(cancel);
      return dispatched;
    }).mockResolvedValue('ready');
    const queue = new QueryQueue(`durable-timeout-${skipQueue}`, {
      cacheAndQueueDriver: 'memory', logger, skipQueue, executionTimeout: 0.01,
      queryHandlers: { query: handler }, cancelHandlers: { query: cancel }, continueWaitTimeout: 1,
    });
    try {
      await expect(queue.executeInQueue('query', 'same-build', { preAggregationBuildId: target })).rejects.toMatchObject({
        code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError',
      });
      expect(logger).toHaveBeenCalledWith('Error while querying', expect.objectContaining({
        preAggregationBuildId: target, reconciliationRequired: true, errorCode: 'MUTATION_UNKNOWN',
      }));
      // The original operation may still complete after the queue timed out.
      release('late physical completion');
      await expect(queue.executeInQueue('query', 'same-build', { preAggregationBuildId: target })).resolves.toBe('ready');
      expect(handler).toHaveBeenCalledTimes(2);
      expect(handler.mock.calls.every(([query]) => query.preAggregationBuildId === target)).toBe(true);
    } finally {
      release('cleanup');
      await dispatched;
      await queue.shutdown();
    }
  });

  it.each([true, false])('does not label an arbitrary SQL timeout recoverable (skipQueue=%s)', async skipQueue => {
    let release!: () => void;
    const dispatched = new Promise<void>(resolve => { release = resolve; });
    const queue = new QueryQueue(`ordinary-timeout-${skipQueue}`, {
      cacheAndQueueDriver: 'memory', logger: jest.fn(), skipQueue, executionTimeout: 0.01,
      queryHandlers: { query: () => dispatched }, cancelHandlers: { query: jest.fn() }, continueWaitTimeout: 1,
    });
    try {
      await expect(queue.executeInQueue('query', 'sql', {})).rejects.toMatchObject({ code: undefined });
    } finally {
      release();
      await dispatched;
      await queue.shutdown();
    }
  });

  it('keeps touch/used keys on UNKNOWN but removes touch on a definitive failure', async () => {
    const { loader, preAggregations } = setup();
    const strategy = jest.spyOn(loader as any, 'refreshReadOnlyExternalStrategy');
    strategy.mockRejectedValue(Object.assign(new Error('unknown outcome'), { code: 'MUTATION_UNKNOWN' }));
    await expect(loader.refresh(entry, [], {})).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(preAggregations.removeTableTouched).not.toHaveBeenCalled();
    expect(preAggregations.removeTableUsed).not.toHaveBeenCalled();
    strategy.mockRejectedValue(new Error('definitive failure'));
    await expect(loader.refresh(entry, [], {})).rejects.toThrow('definitive failure');
    expect(preAggregations.removeTableTouched).toHaveBeenCalledWith(target);
  });

  it('resumes a durable CREATE before downloading source rows again', async () => {
    const { loader, driver, loadCache } = setup();
    driver.resumePreAggregationBuild.mockResolvedValue(true);
    const download = jest.spyOn(loader as any, 'refreshReadOnlyExternalStrategy');
    await loader.refresh(entry, [], {}, target);
    expect(driver.resumePreAggregationBuild).toHaveBeenCalledWith(target);
    expect(download).not.toHaveBeenCalled();
    expect(loadCache.fetchTables).toHaveBeenCalled();
  });

  it('keeps a draining ledger read unresolved and resumes the same build on the next refresh', async () => {
    const { loader, driver, preAggregations } = setup();
    driver.resumePreAggregationBuild.mockRejectedValueOnce(Object.assign(new Error('WrongConnection: Router is draining'), { name: 'ConnectionError' }));
    const download = jest.spyOn(loader as any, 'refreshReadOnlyExternalStrategy');
    await expect(loader.refresh(entry, [], {}, target)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError' });
    expect(preAggregations.removeTableTouched).not.toHaveBeenCalled();
    expect(preAggregations.removeTableUsed).not.toHaveBeenCalled();
    driver.resumePreAggregationBuild.mockResolvedValue(true);
    await loader.refresh(entry, [], {}, target);
    expect(driver.resumePreAggregationBuild.mock.calls).toEqual([[target], [target]]);
    expect(download).not.toHaveBeenCalled();
  });

  it('does not let a failed touch write replace the original unknown upload outcome', async () => {
    const { loader, driver, preAggregations } = setup();
    const unknown = Object.assign(new Error('upload response lost'), { code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError' });
    driver.resumePreAggregationBuild.mockImplementation(async () => {
      preAggregations.updateLastTouch.mockRejectedValue(Object.assign(new Error('Router is draining'), { name: 'ConnectionError' }));
      throw unknown;
    });
    await expect(loader.refresh(entry, [], {}, target)).rejects.toBe(unknown);
    expect(preAggregations.removeTableTouched).not.toHaveBeenCalled();
  });

  it('orphan cleanup preserves a pending target and its old ready fallback after touch expiry', async () => {
    const { loader, driver } = setup();
    const pending = 's.rollup_bbbb2222_aaaa1111_1';
    const oldReady = 's.rollup_cccc3333_aaaa1111_2';
    const orphan = 's.other_dddd4444_aaaa1111_3';
    driver.getTablesQuery.mockResolvedValue([pending, oldReady, orphan].map(table => ({ table_name: table.slice(2) })));
    driver.getProtectedPreAggregationTables.mockResolvedValue([pending, oldReady]);
    await (loader as any).dropOrphanedTables(driver, target, (p: any) => p, true, {});
    expect(driver.dropTable.mock.calls).toEqual([[orphan]]);
  });

  it.each([undefined, null, 1])('does not interpret an invalid resume result (%s) as permission to rebuild', async result => {
    const { loader, driver, preAggregations, loadCache } = setup();
    driver.resumePreAggregationBuild.mockResolvedValue(result);
    const download = jest.spyOn(loader as any, 'refreshReadOnlyExternalStrategy');
    await expect(loader.refresh(entry, [], {}, target)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(download).not.toHaveBeenCalled();
    expect(loadCache.fetchTables).not.toHaveBeenCalled();
    expect(preAggregations.removeTableTouched).not.toHaveBeenCalled();
    expect(preAggregations.removeTableUsed).not.toHaveBeenCalled();
  });

  it('executes the first build strategy when a selected durable build returns false', async () => {
    const { loader, driver, preAggregations } = setup();
    // The existing driver returns false for selected. This exercises that
    // first-build contract, not proof that every false result is safe to replay.
    driver.resumePreAggregationBuild.mockResolvedValue(false);
    const download = jest.spyOn(loader as any, 'refreshReadOnlyExternalStrategy').mockResolvedValue(undefined);
    await loader.refresh(entry, [], {}, target);
    expect(driver.resumePreAggregationBuild).toHaveBeenCalledWith(target);
    expect(download).toHaveBeenCalledTimes(1);
    expect(download).toHaveBeenCalledWith({}, entry, expect.any(Function), []);
    expect(preAggregations.removeTableTouched).not.toHaveBeenCalled();
  });

  it('does not enter the first-build fallback after an explicit UNKNOWN reconciliation error', async () => {
    const { loader, driver, preAggregations } = setup();
    const unknown = Object.assign(new Error('manifest outcome unresolved'), { code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError' });
    driver.resumePreAggregationBuild.mockRejectedValue(unknown);
    const download = jest.spyOn(loader as any, 'refreshReadOnlyExternalStrategy');
    await expect(loader.refresh(entry, [], {}, target)).rejects.toBe(unknown);
    expect(download).not.toHaveBeenCalled();
    expect(preAggregations.removeTableTouched).not.toHaveBeenCalled();
    expect(preAggregations.removeTableUsed).not.toHaveBeenCalled();
  });

  it('rejects a build identity that differs from the requested target before reconciliation', async () => {
    const { loader, driver, preAggregations } = setup();
    await expect(loader.refresh(entry, [], {}, 's.other')).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(driver.resumePreAggregationBuild).not.toHaveBeenCalled();
    expect(preAggregations.removeTableTouched).not.toHaveBeenCalled();
  });

  it('keeps a durable build unresolved when the driver has no recovery entry point', async () => {
    const { loader, driver, preAggregations } = setup();
    delete (driver as any).resumePreAggregationBuild;
    const download = jest.spyOn(loader as any, 'refreshReadOnlyExternalStrategy');
    await expect(loader.refresh(entry, [], {}, target)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(download).not.toHaveBeenCalled();
    expect(preAggregations.removeTableTouched).not.toHaveBeenCalled();
  });

  it('allows legacy source work without a durable build identity', async () => {
    const { loader, driver } = setup();
    const download = jest.spyOn(loader as any, 'refreshReadOnlyExternalStrategy').mockResolvedValue(undefined);
    await loader.refresh(entry, [], {});
    expect(driver.resumePreAggregationBuild).not.toHaveBeenCalled();
    expect(download).toHaveBeenCalledTimes(1);
  });

  it('preserves normal orphan cleanup when the protection snapshot is empty', async () => {
    const { loader, driver } = setup();
    driver.getTablesQuery.mockResolvedValue([{ table_name: 'other_dddd4444_aaaa1111_3' }]);
    await (loader as any).dropOrphanedTables(driver, target, (p: any) => p, true, {});
    expect(driver.getProtectedPreAggregationTables).toHaveBeenCalled();
    expect(driver.dropTable.mock.calls).toEqual([['s.other_dddd4444_aaaa1111_3']]);
  });

  it('preserves normal source temp table cleanup when the target is not protected', async () => {
    const { loader, driver } = setup();
    driver.getTablesQuery.mockResolvedValue([{ table_name: target.slice(2) }]);
    await (loader as any).cleanupWriteStrategy(driver, target, {}, (p: any) => p, true, true);
    expect(driver.dropTable.mock.calls).toEqual([[target]]);
    expect(driver.getTablesQuery).toHaveBeenCalled();
  });

  it('still retains a source temp table explicitly protected by a pending build', async () => {
    const { loader, driver } = setup();
    driver.getProtectedPreAggregationTables.mockResolvedValue([target]);
    await (loader as any).cleanupWriteStrategy(driver, target, {}, (p: any) => p, true, true);
    expect(driver.dropTable).not.toHaveBeenCalled();
    expect(driver.getTablesQuery).not.toHaveBeenCalled();
  });

  it('fails closed when protection enumeration fails', async () => {
    const { loader, driver } = setup();
    driver.getTablesQuery.mockResolvedValue([{ table_name: 'other_dddd4444_aaaa1111_3' }]);
    driver.getProtectedPreAggregationTables.mockRejectedValue(new Error('ledger unavailable'));
    await expect((loader as any).dropOrphanedTables(driver, target, (p: any) => p, true, {})).rejects.toThrow('ledger unavailable');
    expect(driver.dropTable).not.toHaveBeenCalled();
  });

  it('preserves legacy orphan cleanup for drivers without the durable protection contract', async () => {
    const { loader, driver } = setup();
    delete (driver as any).getProtectedPreAggregationTables;
    const orphan = 's.other_dddd4444_aaaa1111_3';
    driver.getTablesQuery.mockResolvedValue([{ table_name: orphan.slice(2) }]);
    await (loader as any).dropOrphanedTables(driver, target, (p: any) => p, true, {});
    expect(driver.dropTable.mock.calls).toEqual([[orphan]]);
  });

  it.each([true, false])('preserves UNKNOWN across queue serialization (skipQueue=%s)', async (skipQueue) => {
    const queue = new QueryQueue(`recovery-${skipQueue}`, {
      cacheAndQueueDriver: 'memory', logger: jest.fn(), skipQueue,
      queryHandlers: { query: async () => { throw Object.assign(new Error('response lost'), { code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError' }); } },
      cancelHandlers: {}, continueWaitTimeout: 1,
    });
    await expect(queue.executeInQueue('query', 'unknown', {})).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError' });
  });

  it('runs build-specific reconciliation instead of permanently caching UNKNOWN', async () => {
    const handler = jest.fn()
      .mockRejectedValueOnce(Object.assign(new Error('response lost'), { code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError' }))
      .mockResolvedValue('ready');
    const queue = new QueryQueue('reconcile-cached-unknown', {
      cacheAndQueueDriver: 'memory', logger: jest.fn(),
      queryHandlers: { query: handler }, cancelHandlers: {}, continueWaitTimeout: 1,
    });
    await expect(queue.executeInQueue('query', 'build', { preAggregationBuildId: target })).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    await expect(queue.executeInQueue('query', 'build', { preAggregationBuildId: target })).resolves.toBe('ready');
    expect(handler).toHaveBeenCalledTimes(2);
  });

  it.each([true, false])('re-enters a cached drain failure only for a durable build (recovery=%s)', async recovery => {
    const draining = Object.assign(new Error('WrongConnection: Router is draining'), { name: 'ConnectionError' });
    const handler = jest.fn().mockResolvedValue('ready');
    const queue = new QueryQueue(`reconcile-cached-drain-${recovery}`, {
      cacheAndQueueDriver: 'memory', logger: jest.fn(),
      queryHandlers: { query: handler }, cancelHandlers: {}, continueWaitTimeout: 1,
    });
    // Local queue results are consumed by the first caller. Seed a cached result
    // explicitly to exercise the cache-hit branch, not a fresh second enqueue.
    const queueDriver = (queue as any).queueDriver;
    const createConnection = queueDriver.createConnection.bind(queueDriver);
    let cached = true;
    jest.spyOn(queueDriver, 'createConnection').mockImplementation(async () => {
      const connection = await createConnection();
      const getResult = connection.getResult.bind(connection);
      connection.getResult = async (...args: any[]) => {
        if (cached) {
          cached = false;
          return { error: draining.message, errorName: draining.name };
        }
        return getResult(...args);
      };
      return connection;
    });
    const query = recovery ? { preAggregationBuildId: target } : {};
    if (recovery) {
      await expect(queue.executeInQueue('query', 'build', query)).resolves.toBe('ready');
    } else {
      await expect(queue.executeInQueue('query', 'build', query)).rejects.toMatchObject({ name: 'ConnectionError' });
    }
    expect(handler).toHaveBeenCalledTimes(recovery ? 1 : 0);
  });
});
