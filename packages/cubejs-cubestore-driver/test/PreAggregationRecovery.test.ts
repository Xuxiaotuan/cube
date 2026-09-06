import { createHash } from 'crypto';
import { mkdtemp, writeFile, rm, unlink, readFile } from 'fs/promises';
import { Readable } from 'stream';
import { gunzipSync } from 'zlib';
import { tmpdir } from 'os';
import { join } from 'path';
import fetch from 'node-fetch';
import { CubeStoreDriver } from '../src/CubeStoreDriver';
import { MutationUnknownError, WebSocketConnection } from '../src/WebSocketConnection';
import { PreAggregationBuildStore } from '../src/PreAggregationBuildStore';
import { ConnectionError } from '../src/errors';

jest.mock('node-fetch');
const fetchMock = fetch as jest.MockedFunction<typeof fetch>;
const table = 's.rollup_content_struct_1';
const versionEntry = { table_name: 's.rollup', content_version: 'content', structure_version: 'struct', last_updated_at: 1000, naming_version: 2 };

async function readyStatus(driver: CubeStoreDriver, tableId: number) {
  const record = await (driver as any).preAggregationBuilds.read(table);
  return { state: 'ready', tableId, locations: record.create.params };
}

function harness() {
  const cache = new Map<string, string>();
  const driver = new CubeStoreDriver({ host: 'localhost' });
  (driver as any).preAggregationReconcileTimeoutMs = 100;
  const creates: string[] = [];
  let create: () => Promise<any[]> = async () => [];
  jest.spyOn(driver as any, 'sleep').mockResolvedValue(undefined);
  jest.spyOn(driver, 'query').mockImplementation(async (sql, values) => {
    if (sql === 'CACHE GET ?') return cache.has(values[0]) ? [{ value: cache.get(values[0]) }] : [];
    if (sql === 'CACHE KEYS ?') {
      // Rust CacheItem uses the exact namespace before the final colon, not
      // an arbitrary string prefix or a recursive wildcard match.
      const prefix = values[0].replace(/:\*?$/, '');
      return [...cache.keys()].filter(key => key.slice(0, key.lastIndexOf(':')) === prefix).map(key => ({ key }));
    }
    if (sql.startsWith('CACHE SET')) {
      if (!sql.includes(' NX ') || !cache.has(values[0])) cache.set(values[0], values[1]);
      return [{ success: 'true' }];
    }
    creates.push(sql);
    return create();
  });
  const status = jest.spyOn(driver, 'getPreAggregationBuildStatus').mockResolvedValue({ state: 'absent' });
  return { driver, cache, creates, status, onCreate: (fn: () => Promise<any[]>) => { create = fn; } };
}

describe('durable pre-aggregation recovery', () => {
  afterEach(() => jest.restoreAllMocks());

  it('keeps the selected target across workers and protects old ready versions without TTL', async () => {
    const { driver, cache } = harness();
    const first = await driver.resolvePreAggregationBuild('logical', versionEntry, ['s.old_ready']);
    const second = await driver.resolvePreAggregationBuild('logical', { ...versionEntry, last_updated_at: 2000 }, []);
    expect(first?.buildId).toBe(table);
    expect(second?.buildId).toBe(table);
    expect(await driver.getProtectedPreAggregationTables()).toEqual([table, 's.old_ready']);
    expect([...cache.keys()]).toContain(`PRE_AGG_BUILD_V1:${table}`);
    expect((driver.query as jest.Mock).mock.calls.filter(([sql]) => sql.includes('CACHE SET')).every(([sql]) => !sql.includes('TTL'))).toBe(true);
  });

  it('queries the exact build namespace without treating arbitrary key prefixes as namespaces', async () => {
    const { driver, cache } = harness();
    await driver.resolvePreAggregationBuild('logical', versionEntry, ['s.old_ready']);
    cache.set('PRE_AGG_BUILD_V10:unrelated', 'not a build');
    cache.set('PRE_AGG_BUILD_V1:nested:unrelated', 'not a build');
    expect(await driver.getProtectedPreAggregationTables()).toEqual([table, 's.old_ready']);
    expect(driver.query).toHaveBeenCalledWith('CACHE KEYS ?', ['PRE_AGG_BUILD_V1:']);
  });

  it('reconciles a CREATE response lost after success, with no duplicate CREATE', async () => {
    const { driver, status, creates, onCreate } = harness();
    await driver.resolvePreAggregationBuild('logical', versionEntry, ['s.old_ready']);
    onCreate(async () => {
      status.mockResolvedValue(await readyStatus(driver, 42));
      throw new MutationUnknownError('response lost after commit');
    });
    await driver.createTableWithOptions(table, [{ name: 'x', type: 'int' }], { files: ['temp://immutable.csv.gz'] }, { preAggregationBuildId: table });
    expect(creates).toHaveLength(1);
    expect(await driver.resumePreAggregationBuild(table)).toBe(true);
    expect(creates).toHaveLength(1);
    expect(await driver.getProtectedPreAggregationTables()).toEqual([]);
  });

  it('retains building intent across queue retries and does not treat building as ready', async () => {
    const { driver, status, creates } = harness();
    await driver.resolvePreAggregationBuild('logical', versionEntry, ['s.old_ready']);
    status.mockResolvedValue({ state: 'building', tableId: 10 });
    await expect(driver.createTableWithOptions(table, [{ name: 'x', type: 'int' }], { files: ['temp://immutable.csv.gz'] }, { preAggregationBuildId: table }))
      .rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(creates).toHaveLength(0);
    expect(await driver.getProtectedPreAggregationTables()).toContain('s.old_ready');
    status.mockResolvedValue(await readyStatus(driver, 10));
    await expect(driver.resumePreAggregationBuild(table)).resolves.toBe(true);
  });

  it('fails closed if a previously observed target is replaced', async () => {
    const { driver, status } = harness();
    await driver.resolvePreAggregationBuild('logical', versionEntry, []);
    status.mockResolvedValue({ state: 'building', tableId: 10 });
    await expect(driver.createTableWithOptions(table, [{ name: 'x', type: 'int' }], { files: ['temp://immutable.csv.gz'] }, { preAggregationBuildId: table })).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    status.mockResolvedValue(await readyStatus(driver, 11));
    await expect(driver.resumePreAggregationBuild(table)).rejects.toThrow('Table identity changed');
  });

  it('routes recovery-enabled row arrays through files, never partially visible INSERT batches', async () => {
    const { driver } = harness();
    const importStream = jest.spyOn(driver as any, 'importStream').mockResolvedValue(undefined);
    await driver.uploadTableWithIndexes(table, [{ name: 'x', type: 'int' }], { rows: [{ x: 1 }, { x: 2 }] }, [], null, { preAggregationBuildId: table });
    const rows: any[] = [];
    for await (const row of (importStream.mock.calls[0][1] as any).rowStream) rows.push(row);
    expect(rows).toEqual([{ x: 1 }, { x: 2 }]);
    expect(driver.query).not.toHaveBeenCalled();
  });

  it('never drops an imported row table after unknown INSERT and keeps batch identity', async () => {
    const { driver, onCreate } = harness();
    jest.spyOn(driver, 'createTableWithOptions').mockResolvedValue([]);
    const drop = jest.spyOn(driver, 'dropTable');
    onCreate(async () => { throw new MutationUnknownError('INSERT response lost'); });
    await expect(driver.uploadTableWithIndexes(table, [{ name: 'x', type: 'int' }], { rows: [{ x: 1 }] }, [], null))
      .rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(drop).not.toHaveBeenCalled();
    expect((driver.query as jest.Mock).mock.calls[0][2].mutationId).toMatch(`${table}:insert:0:`);
  });

  it('does not enable generic write replay even when a mutation ID is provided', () => {
    const { driver } = harness();
    expect((driver as any).resolveRetryablePolicy(true, true, 'id')).toBe(false);
    const socket = new WebSocketConnection('ws://localhost/ws');
    expect((socket as any).isReplaySafeQuery('INSERT INTO x VALUES (1)')).toBe(false);
    expect((socket as any).isReplaySafeQuery('CACHE SET key value')).toBe(false);
    expect((socket as any).isReplaySafeQuery('CACHE GET key')).toBe(true);
  });

  it('does not infer NX persistence from an absent or conflicting exact readback', async () => {
    const query = jest.fn(async (sql: string) => {
      if (sql.startsWith('CACHE SET')) throw new MutationUnknownError('write response lost');
      return [];
    });
    const store = new PreAggregationBuildStore(query);
    await expect(store.save({ buildId: table, versionEntry, phase: 'create', protectedTables: [] })).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
  });

  it('polls through a realistic number of leader-transition observations in one call', async () => {
    const { driver, status, onCreate, creates } = harness();
    (driver as any).preAggregationReconcileTimeoutMs = 120000;
    await driver.resolvePreAggregationBuild('logical', versionEntry, []);
    let polls = 0;
    onCreate(async () => {
      status.mockImplementation(async () => ({ state: ++polls < 40 ? 'building' : 'ready', tableId: 42, locations: ['temp://immutable.csv.gz'] }));
      throw new MutationUnknownError('leader transition');
    });
    await driver.createTableWithOptions(table, [{ name: 'x', type: 'int' }], { files: ['temp://immutable.csv.gz'] }, { preAggregationBuildId: table });
    expect(polls).toBe(40);
    expect(creates).toHaveLength(1);
    expect((driver as any).sleep.mock.calls.every(([delay]) => delay <= 2000)).toBe(true);
  });

  it('atomically selects one of two unequal concurrent manifests', async () => {
    const { driver } = harness();
    const selected = await driver.resolvePreAggregationBuild('logical', versionEntry, []);
    const first = new PreAggregationBuildStore((sql, values) => driver.query(sql, values));
    const second = new PreAggregationBuildStore((sql, values) => driver.query(sql, values));
    const recordA = { ...selected!, phase: 'uploading' as const, create: { sql: 'CREATE TABLE t LOCATION ?', params: ['temp://a.csv.gz'] } };
    const recordB = { ...selected!, phase: 'uploading' as const, create: { sql: 'CREATE TABLE t LOCATION ?', params: ['temp://b.csv.gz'] } };
    const result = await Promise.allSettled([first.save(recordA), second.save(recordB)]);
    expect(result.filter(r => r.status === 'fulfilled')).toHaveLength(1);
    const failure = result.find(r => r.status === 'rejected') as PromiseRejectedResult;
    expect(failure.reason).toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect((await first.read(table))!.create!.params).toEqual(['temp://a.csv.gz']);
  });

  it.each([undefined, ['temp://different.csv.gz']])('refuses ready without authoritative matching file locations (%s)', async locations => {
    const { driver, status, onCreate } = harness();
    await driver.resolvePreAggregationBuild('logical', versionEntry, []);
    onCreate(async () => { status.mockResolvedValue({ state: 'ready', tableId: 42, locations }); return []; });
    await expect(driver.createTableWithOptions(table, [{ name: 'x', type: 'int' }], { files: ['temp://immutable.csv.gz'] }, { preAggregationBuildId: table }))
      .rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect((await (driver as any).preAggregationBuilds.read(table)).phase).not.toBe('ready');
  });

  it('never regresses ready when a stale owner publishes an earlier phase', async () => {
    const { driver } = harness();
    const selected = await driver.resolvePreAggregationBuild('logical', versionEntry, []);
    const store = new PreAggregationBuildStore((sql, values) => driver.query(sql, values));
    const create = { sql: 'CREATE TABLE t LOCATION ?', params: ['temp://a.csv.gz'] };
    await store.save({ ...selected!, phase: 'ready', create, tableId: 42 });
    await store.save({ ...selected!, phase: 'uploading', create });
    expect((await store.read(table))!.phase).toBe('ready');
    expect((await store.read(table))!.tableId).toBe('42');
  });

  it('allocates a new target when a terminal ready target has been retired', async () => {
    const { driver, status, onCreate } = harness();
    await driver.resolvePreAggregationBuild('logical', versionEntry, []);
    onCreate(async () => { status.mockResolvedValue(await readyStatus(driver, 42)); return []; });
    await driver.createTableWithOptions(table, [{ name: 'x', type: 'int' }], { files: ['temp://immutable.csv.gz'] }, { preAggregationBuildId: table });
    status.mockResolvedValue({ state: 'absent' });
    const next = await driver.resolvePreAggregationBuild('logical', versionEntry, []);
    expect(next!.buildId).not.toBe(table);
    expect(next!.phase).toBe('selected');
  });

  it('publishes only authoritative ready tables while retaining compatibility with an old router', async () => {
    const { driver, status } = harness();
    (driver.query as jest.Mock).mockImplementation(async sql => sql.startsWith('CACHE') ? [] : [{ table_name: 'old' }, { table_name: 'partial' }]);
    status.mockImplementation(async name => ({ state: name.endsWith('.old') ? 'ready' : 'building', tableId: 1 }));
    await expect(driver.getTablesQuery('s')).resolves.toEqual([{ table_name: 'old' }]);
    status.mockResolvedValue(null);
    await expect(driver.getTablesQuery('legacy')).resolves.toHaveLength(2);
  });

  it('commits ready before discovery publishes a table whose original CREATE has not returned', async () => {
    const { driver, status, onCreate, creates } = harness();
    await driver.resolvePreAggregationBuild('logical', versionEntry, ['s.old_ready']);
    let release!: () => void;
    let entered!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    const started = new Promise<void>(resolve => { entered = resolve; });
    onCreate(async () => { entered(); await blocked; return []; });
    let createCompleted = false;
    const creating = driver.createTableWithOptions(table, [{ name: 'x', type: 'int' }], { files: ['temp://immutable.csv.gz'] }, { preAggregationBuildId: table })
      .then(() => { createCompleted = true; });
    try {
      await started;
      status.mockResolvedValue(await readyStatus(driver, 42));
      const tables = [{ table_name: table.slice(2) }];
      expect(await (driver as any).readyPreAggregationTables('s', tables)).toEqual(tables);
      expect(createCompleted).toBe(false);
      expect(await (driver as any).preAggregationBuilds.read(table)).toMatchObject({ phase: 'ready', tableId: '42' });
      expect(await driver.getProtectedPreAggregationTables()).toEqual([]);
      expect(creates).toHaveLength(1);
    } finally {
      release();
      await creating;
    }
  });

  it.each([true, false])('requires exact durable evidence after a ready marker acknowledgement is lost (committed=%s)', async committed => {
    const { driver, status, cache } = harness();
    const selected = await driver.resolvePreAggregationBuild('logical', versionEntry, ['s.old_ready']);
    const store = (driver as any).preAggregationBuilds as PreAggregationBuildStore;
    await store.save({ ...selected!, phase: 'create', create: { sql: 'CREATE TABLE t LOCATION ?', params: ['temp://immutable.csv.gz'] } });
    status.mockResolvedValue(await readyStatus(driver, 42));
    const original = (driver.query as jest.Mock).getMockImplementation()!;
    (driver.query as jest.Mock).mockImplementation(async (sql, values) => {
      if (sql.startsWith('CACHE SET') && values[0] === `PRE_AGG_PHASE_V1:${table}:ready`) {
        if (committed) await original(sql, values);
        throw new MutationUnknownError('ready marker response lost');
      }
      return original(sql, values);
    });
    const tables = [{ table_name: table.slice(2) }];
    const publication = (driver as any).readyPreAggregationTables('s', tables);
    if (committed) {
      await expect(publication).resolves.toEqual(tables);
      expect((await store.read(table))!.phase).toBe('ready');
    } else {
      await expect(publication).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
      expect((await store.read(table))!.phase).toBe('create');
      expect(await driver.getProtectedPreAggregationTables()).toContain('s.old_ready');
    }
    expect(cache.has(`PRE_AGG_PHASE_V1:${table}:ready`)).toBe(committed);
  });

  it.each(['selected', 'failed', 'retired'] as const)('does not adopt a discovered table with a %s build', async phase => {
    const { driver, status, cache } = harness();
    const selected = await driver.resolvePreAggregationBuild('logical', versionEntry, []);
    if (phase !== 'selected') {
      await (driver as any).preAggregationBuilds.save({ ...selected!, phase,
        create: { sql: 'CREATE TABLE t LOCATION ?', params: ['temp://immutable.csv.gz'] } });
    }
    status.mockResolvedValue({ state: 'ready', tableId: 42, locations: ['temp://immutable.csv.gz'] });
    expect(await (driver as any).readyPreAggregationTables('s', [{ table_name: table.slice(2) }])).toEqual([]);
    expect(cache.has(`PRE_AGG_PHASE_V1:${table}:ready`)).toBe(false);
  });

  it.each(['locations', 'tableId'])('does not publish or mark ready when discovered %s conflicts', async conflict => {
    const { driver, status, cache } = harness();
    const selected = await driver.resolvePreAggregationBuild('logical', versionEntry, []);
    await (driver as any).preAggregationBuilds.save({ ...selected!, phase: 'create', tableId: 42,
      create: { sql: 'CREATE TABLE t LOCATION ?', params: ['temp://immutable.csv.gz'] } });
    status.mockResolvedValue({ state: 'ready', tableId: conflict === 'tableId' ? 43 : 42,
      locations: [conflict === 'locations' ? 'temp://different.csv.gz' : 'temp://immutable.csv.gz'] });
    expect(await (driver as any).readyPreAggregationTables('s', [{ table_name: table.slice(2) }])).toEqual([]);
    expect(cache.has(`PRE_AGG_PHASE_V1:${table}:ready`)).toBe(false);
  });
});

describe('real gzip manifests after refresh worker loss', () => {
  const paths = new Set<string>();
  afterEach(async () => {
    jest.restoreAllMocks();
    await Promise.all([...paths].map(path => unlink(path).catch(() => undefined)));
    paths.clear();
  });

  async function interrupted() {
    const h = harness();
    await h.driver.resolvePreAggregationBuild('logical', versionEntry, ['s.old_ready']);
    const post = jest.spyOn(h.driver as any, 'uploadTempFile').mockRejectedValue(new MutationUnknownError('worker lost'));
    await expect(h.driver.uploadTableWithIndexes(table, [{ name: 'x', type: 'int' }], { rows: [{ x: 1 }] }, [], null, { preAggregationBuildId: table })).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    const record = await (h.driver as any).preAggregationBuilds.read(table);
    record.uploads.forEach(upload => paths.add(upload.path));
    post.mockRestore();
    return { ...h, record };
  }

  it('keeps uploading ledger and active identity unchanged across drain and a new candidate probe', async () => {
    const { driver, record, cache, status } = await interrupted();
    const ledgerBefore = [...cache.entries()];
    jest.spyOn(driver as any, 'routerRecoveryJson').mockRejectedValue(new ConnectionError('WrongConnection: Router is draining'));
    await expect(driver.resumePreAggregationBuild(table)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect([...cache.entries()]).toEqual(ledgerBefore);
    expect((await (driver as any).preAggregationBuilds.read(table)).phase).toBe('uploading');
    const selected = await driver.resolvePreAggregationBuild('logical', { ...versionEntry, last_updated_at: 2000 }, []);
    expect(status).toHaveBeenCalledWith('s.rollup_content_struct_2');
    expect(selected!.buildId).toBe(record.buildId);
    expect(selected!.manifestHash).toBe(record.manifestHash);
    expect([...cache.entries()]).toEqual(ledgerBefore);
    expect(cache.has(`PRE_AGG_PHASE_V1:${table}:failed`)).toBe(false);
  });

  it('recovers an empty dataset using a gzip header file and LOCATION, never a bare CREATE', async () => {
    const { driver, status, onCreate, creates } = harness();
    await driver.resolvePreAggregationBuild('empty', versionEntry, []);
    let payload: Buffer | undefined;
    jest.spyOn(driver as any, 'uploadTempFile').mockImplementation(async (upload: any) => {
      payload = await readFile(upload.path);
      return upload.name;
    });
    onCreate(async () => {
      status.mockResolvedValue(await readyStatus(driver, 42));
      throw new MutationUnknownError('empty CREATE success response lost');
    });
    await driver.uploadTableWithIndexes(table, [{ name: 'x', type: 'int' }], { rows: [] }, [], null, { preAggregationBuildId: table });
    expect(gunzipSync(payload!).toString()).toBe('x\n');
    expect(creates).toHaveLength(1);
    expect(creates[0]).toContain(' LOCATION ?');
    expect((await (driver as any).preAggregationBuilds.read(table)).phase).toBe('ready');
  });

  it('recovers a complete remote manifest without any original local files', async () => {
    const { driver, record, onCreate, status } = await interrupted();
    for (const upload of record.uploads) {
      const bytes = await readFile(upload.path);
      expect(createHash('sha256').update(bytes).digest('hex')).toBe(upload.sha256);
      await unlink(upload.path);
    }
    jest.spyOn(driver as any, 'routerRecoveryJson').mockImplementation(async (url: any) => {
      const upload = record.uploads.find(u => url.includes(u.sha256));
      return { state: 'uploaded', sha256: upload.sha256, size: upload.size };
    });
    onCreate(async () => { status.mockResolvedValue(await readyStatus(driver, 42)); return []; });
    await expect(driver.resumePreAggregationBuild(table)).resolves.toBe(true);
    expect((await (driver as any).preAggregationBuilds.read(table)).phase).toBe('ready');
  });

  it('re-extracts only when remote and local inputs are missing, and accepts only identical compressed inputs', async () => {
    const { driver, record, onCreate, status } = await interrupted();
    for (const upload of record.uploads) await unlink(upload.path);
    jest.spyOn(driver as any, 'routerRecoveryJson').mockResolvedValue({ state: 'missing' });
    await expect(driver.resumePreAggregationBuild(table)).resolves.toBe(false);
    jest.spyOn(driver as any, 'uploadTempFile').mockImplementation(async (upload: any) => upload.name);
    onCreate(async () => { status.mockResolvedValue(await readyStatus(driver, 42)); return []; });
    await driver.uploadTableWithIndexes(table, [{ name: 'x', type: 'int' }], { rows: [{ x: 1 }] }, [], null, { preAggregationBuildId: table });
    const current = await (driver as any).preAggregationBuilds.read(table);
    expect(current.manifestHash).toBe(record.manifestHash);
  });

  it('does not mix regenerated changed inputs and moves a failed attempt to a new target', async () => {
    const { driver, record, creates } = await interrupted();
    try {
      await driver.uploadTableWithIndexes(table, [{ name: 'x', type: 'int' }], { rows: [{ x: 999 }] }, [], null, { preAggregationBuildId: table });
      throw new Error('expected manifest mismatch');
    } catch (error: any) {
      error.tempFiles?.forEach(path => paths.add(path));
      expect(error.code).toBe('PRE_AGG_REBUILD_REQUIRED');
    }
    expect(creates).toHaveLength(0);
    expect((await (driver as any).preAggregationBuilds.read(table)).manifestHash).toBe(record.manifestHash);
    const next = await driver.resolvePreAggregationBuild('logical', versionEntry, ['s.old_ready']);
    expect(next!.buildId).not.toBe(table);
    expect(next!.phase).toBe('selected');
  });

  it('settles all compression pipelines when the source fails mid-stream', async () => {
    const { driver } = harness();
    const source = Readable.from((async function* rows() {
      yield { x: 1 };
      throw new Error('source disconnected');
    }()));
    try {
      await driver.uploadTableWithIndexes(table, [{ name: 'x', type: 'int' }], { rowStream: source }, [], null);
      throw new Error('expected source failure');
    } catch (error: any) {
      error.tempFiles?.forEach(path => paths.add(path));
      expect(error.message).toBe('source disconnected');
    }
  });
});

describe('content-addressed upload recovery', () => {
  let dir: string;
  let upload: { path: string; name: string; sha256: string; size: number };
  beforeEach(async () => {
    fetchMock.mockReset();
    dir = await mkdtemp(join(tmpdir(), 'cube-upload-test-'));
    const bytes = Buffer.from('compressed payload');
    const sha256 = createHash('sha256').update(bytes).digest('hex');
    upload = { path: join(dir, 'payload'), name: `${sha256}.csv.gz`, sha256, size: bytes.length };
    await writeFile(upload.path, bytes);
  });
  afterEach(async () => { jest.restoreAllMocks(); await rm(dir, { recursive: true }); });

  it.each(['response lost after commit', 'mid-upload disconnect'])('recovers %s with the same immutable payload name', async (fault) => {
    const { driver } = harness();
    let uploaded = false;
    const bodies: any[] = [];
    jest.spyOn(driver as any, 'routerRecoveryJson').mockImplementation(async () => uploaded ? { state: 'uploaded', sha256: upload.sha256, size: upload.size } : { state: 'missing' });
    fetchMock.mockImplementation(async (_url, options) => {
      bodies.push(options!.body);
      // Consume the actual new stream, not a reused exhausted body.
      for await (const _chunk of options!.body as any) { /* drain */ }
      if (fault === 'response lost after commit' || bodies.length > 1) uploaded = true;
      if (bodies.length === 1) throw new Error(fault);
      return { ok: true, text: async () => '' } as any;
    });
    await expect((driver as any).uploadTempFile(upload, true)).resolves.toBe(upload.name);
    expect(bodies).toHaveLength(fault === 'response lost after commit' ? 1 : 2);
    if (bodies.length === 2) expect(bodies[0]).not.toBe(bodies[1]);
    for (const [url] of fetchMock.mock.calls) expect(String(url)).toContain(`sha256=${upload.sha256}`);
  });

  it('rejects uploaded metadata with a different checksum or length', async () => {
    const { driver } = harness();
    jest.spyOn(driver as any, 'routerRecoveryJson').mockResolvedValue({ state: 'uploaded', sha256: 'wrong', size: upload.size });
    await expect((driver as any).uploadTempFile(upload, true)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('negotiates an old router with a one-shot upload and never retries its unknown outcome', async () => {
    const { driver } = harness();
    jest.spyOn(driver as any, 'routerRecoveryJson').mockResolvedValue(null);
    fetchMock.mockImplementation(async (_url, options) => {
      for await (const _chunk of options!.body as any) { /* drain */ }
      throw new Error('lost');
    });
    await expect((driver as any).uploadTempFile(upload, false)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
