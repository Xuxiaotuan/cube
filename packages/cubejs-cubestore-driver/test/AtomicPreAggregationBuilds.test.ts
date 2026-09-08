import fetch from 'node-fetch';
import { AtomicPreAggregationBuilds } from '../src/AtomicPreAggregationBuilds';
import { AtomicPreAggregationLedger, LedgerCreate, LedgerRecord, LedgerRequest, LedgerResult } from '../src/AtomicPreAggregationLedger';
import { PreAggregationBuildStore } from '../src/PreAggregationBuildStore';
import { CubeStoreDriver } from '../src/CubeStoreDriver';
import { MutationUnknownError } from '../src/WebSocketConnection';

jest.mock('node-fetch');

const table = 's.rollup_content_struct_1';
const versionEntry = { table_name: 's.rollup', content_version: 'content', structure_version: 'struct', last_updated_at: 1000, naming_version: 2 };
const create: LedgerCreate = {
  schema: 's', table: 'rollup_content_struct_1', columns: [{ name: 'x', columnType: 'int' }],
  locations: ['temp://immutable.csv.gz'], indexes: [], importFormat: 'csv',
  contentVersion: 'content', structureVersion: 'struct',
};
const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value));

// Protocol-level fixtures only. These are not Rust, import-receipt, or HA proofs.
function harness() {
  const cache = new Map<string, string>();
  const records = new Map<string, LedgerRecord>();
  const receipts = new Map<string, { request: LedgerRequest; result: LedgerResult }>();
  const query = jest.fn(async (sql: string, values: any[]) => {
    if (sql === 'CACHE GET ?') return cache.has(values[0]) ? [{ value: cache.get(values[0]) }] : [];
    if (sql === 'CACHE SET ? ?') { cache.set(values[0], values[1]); return []; }
    if (sql === 'CACHE KEYS ?') return [...cache.keys()].filter(key => key.startsWith(values[0])).map(key => ({ key }));
    throw new Error(`Unexpected source-cache operation: ${sql}`);
  });
  const source = new PreAggregationBuildStore(query, true);
  let generation = 0;
  let importPending = 0;
  let publishUnknown = false;
  let physicalAbsent = false;
  let createEffects = 0;
  const requests: LedgerRequest[] = [];
  const ledger = new AtomicPreAggregationLedger(async (method, path, body) => {
    if (method === 'GET') {
      const params = new URL(path, 'http://fixture').searchParams;
      const record = records.get(params.get('key')!);
      if (!record || (params.has('generation') && record.generation !== params.get('generation'))) return null;
      return copy(record);
    }
    const request = body!;
    requests.push(copy(request));
    if (request.command.op === 'publish' && publishUnknown) throw new MutationUnknownError('fixture lost response');
    const old = receipts.get(request.requestId);
    if (old) {
      if (JSON.stringify(old.request) !== JSON.stringify(request)) throw new Error('request id conflict');
      return copy(old.result);
    }
    const command = request.command;
    let current = records.get(command.key);
    let rejection: string | null = null;
    if (command.op === 'claim') {
      if ((current?.generation || null) !== command.expectedGeneration) {
        rejection = 'LEDGER_GENERATION_CONFLICT';
      } else {
        current = { key: command.key, generation: String(++generation), owner: command.owner,
          leaseUntilMillis: '1800000000000', state: 'claimed', manifest: null, references: '0' };
      }
    } else if (!current || current.generation !== command.generation) {
      rejection = 'LEDGER_STALE_GENERATION';
    } else if (command.op === 'renew') {
      current.leaseUntilMillis = String(Number(current.leaseUntilMillis) + 1);
    } else if (command.op === 'create') {
      createEffects++;
      current.state = 'bound';
      current.manifest = { tableId: '9007199254740993', schema: command.create.schema,
        table: command.create.table, locations: command.create.locations,
        contentVersion: command.create.contentVersion, structureVersion: command.create.structureVersion };
    } else if (command.op === 'publish') {
      if (importPending-- > 0) rejection = 'LEDGER_IMPORT_RECEIPT_MISSING';
      else current.state = 'published';
    } else if (command.op === 'retire') {
      if (current.references !== '0') rejection = 'LEDGER_REFERENCED';
      else current.state = 'retired';
    } else if (command.op === 'drop') {
      current.state = 'dropped';
    } else {
      throw new Error(`Unexpected command ${command.op}`);
    }
    if (!rejection && current) records.set(command.key, current);
    const result: LedgerResult = { record: rejection ? null : copy(current!), rejection };
    receipts.set(request.requestId, { request: copy(request), result: copy(result) });
    return result;
  });
  const status = jest.fn(async () => {
    if (physicalAbsent) return { state: 'absent' };
    const record = records.get(table);
    return { state: record?.state === 'published' ? 'ready' : 'building',
      tableId: record?.manifest?.tableId, locations: record?.manifest?.locations };
  });
  const builds = new AtomicPreAggregationBuilds({ ledger, source, status,
    capabilities: jest.fn().mockResolvedValue(undefined), upload: jest.fn().mockResolvedValue('immutable.csv.gz'),
    sleep: jest.fn().mockResolvedValue(undefined), timeout: () => 1000 });
  const stage = async () => {
    const selected = await builds.resolve('logical', versionEntry, ['s.old'], false);
    await source.save({ ...selected, phase: 'uploaded', atomicCreate: create,
      create: { sql: 'CREATE TABLE s.rollup_content_struct_1 LOCATION ?', params: create.locations } });
  };
  return { builds, source, cache, query, requests, records, stage, status, ledger,
    createEffects: () => createEffects,
    pending: (count: number) => { importPending = count; },
    unknownPublish: (value: boolean) => { publishUnknown = value; },
    absent: () => { physicalAbsent = true; } };
}

describe('strict atomic pre-aggregation business integration', () => {
  it.each(['key', 'generation'] as const)('rejects a shape-valid authority response with a mismatched %s', async field => {
    const h = harness();
    await h.stage();
    const current = copy(h.records.get(table)!);
    const requestsBefore = h.requests.length;
    jest.spyOn(h.ledger, 'read').mockResolvedValue({
      ...current, [field]: field === 'key' ? 's.other_content_struct_1' : '9007199254740993',
    });
    await expect(h.builds.resume(table)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(h.requests).toHaveLength(requestsBefore);
    expect(h.createEffects()).toBe(0);
  });

  it('selects through the ledger and never overwrites a pending target or uses CACHE NX', async () => {
    const h = harness();
    const first = await h.builds.resolve('logical', versionEntry, ['s.old'], false);
    const second = await h.builds.resolve('logical', { ...versionEntry, last_updated_at: 2000 }, [], true);
    expect(first.buildId).toBe(table);
    expect(second.atomicLedger).toEqual(first.atomicLedger);
    expect(second.buildId).toBe(table);
    expect(h.requests.filter(r => r.command.op === 'claim')).toHaveLength(2);
    expect(h.query.mock.calls.every(([sql]) => !sql.includes('NX'))).toBe(true);
    await expect(h.builds.resume(table)).resolves.toBe(false);
  });

  it('performs typed create/bind then publish before physical ready observation', async () => {
    const h = harness();
    await h.stage();
    await h.builds.create(table);
    expect(h.requests.find(r => r.command.op === 'create')?.command).toMatchObject({ op: 'create', key: table, create });
    expect(h.records.get(table)?.state).toBe('published');
    expect(await h.source.read(table)).toMatchObject({ phase: 'ready', tableId: '9007199254740993',
      atomicLedger: { key: table, generation: '2' } });
    expect(h.createEffects()).toBe(1);
    expect(h.query.mock.calls.every(([sql]) => sql.startsWith('CACHE'))).toBe(true);
  });

  it('keeps the same publish request id across UNKNOWN and a resumed build', async () => {
    const h = harness();
    await h.stage();
    h.unknownPublish(true);
    await expect(h.builds.create(table)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect((await h.source.read(table))?.phase).not.toBe('ready');
    h.unknownPublish(false);
    await expect(h.builds.resume(table)).resolves.toBe(true);
    const ids = h.requests.filter(r => r.command.op === 'publish').map(r => r.requestId);
    expect(ids).toHaveLength(3);
    expect(new Set(ids).size).toBe(1);
    expect(h.createEffects()).toBe(1);
  });

  it('advances publish request ids only after explicit durable NOT_READY rejection', async () => {
    const h = harness();
    await h.stage();
    h.pending(1);
    await h.builds.create(table);
    const ids = h.requests.filter(r => r.command.op === 'publish').map(r => r.requestId);
    expect(ids).toHaveLength(2);
    expect(new Set(ids).size).toBe(2);
    expect((await h.source.read(table))?.publishAttempt).toBe(1);
  });

  it('does not regenerate source intent when its authority survives cache loss', async () => {
    const h = harness();
    await h.stage();
    h.cache.delete(`PRE_AGG_BUILD_V1:${table}`);
    await expect(h.builds.resume(table)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    await expect(h.builds.resolve('logical', { ...versionEntry, last_updated_at: 2000 }, [], true))
      .rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(h.createEffects()).toBe(0);
    expect(h.cache.has(`PRE_AGG_BUILD_V1:${table}`)).toBe(false);
  });

  it('does not rebuild a published target when physical observation says absent', async () => {
    const h = harness();
    await h.stage();
    await h.builds.create(table);
    h.absent();
    await expect(h.builds.resume(table)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(h.createEffects()).toBe(1);
  });

  it('retains referenced tables and retries retirement after a definite rejection', async () => {
    const h = harness();
    await h.stage();
    await h.builds.create(table);
    h.records.get(table)!.references = '1';
    await expect(h.builds.retireAndDrop(table)).resolves.toBe(false);
    expect(h.requests.some(r => r.command.op === 'drop')).toBe(false);
    h.records.get(table)!.references = '0';
    await expect(h.builds.retireAndDrop(table)).resolves.toBe(true);
    expect(h.records.get(table)?.state).toBe('dropped');
    expect(h.query.mock.calls.some(([sql]) => sql.startsWith('DROP'))).toBe(false);
  });

  it('never retires a bound but unpublished build', async () => {
    const h = harness();
    await h.stage();
    h.unknownPublish(true);
    await expect(h.builds.create(table)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    await expect(h.builds.retireAndDrop(table)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(h.requests.some(r => r.command.op === 'retire' || r.command.op === 'drop')).toBe(false);
    expect(await h.builds.protectedTables()).toEqual([table, 's.old']);
  });
});

describe('strict driver capability and authentication boundaries', () => {
  const previous = process.env.CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT;
  afterEach(() => {
    if (previous === undefined) delete process.env.CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT;
    else process.env.CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT = previous;
    jest.restoreAllMocks();
    (fetch as unknown as jest.Mock).mockReset();
  });

  it('rejects malformed strict configuration instead of treating it as false', () => {
    process.env.CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT = 'TRUE';
    expect(() => new CubeStoreDriver()).toThrow('must be true or false');
  });

  it.each([undefined, false, 'true'])('rejects non-true queryReferences (%s) without ledger POST or cache fallback', async queryReferences => {
    process.env.CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT = 'true';
    const driver = new CubeStoreDriver({ host: 'localhost', user: 'u', password: 'p' });
    jest.spyOn(driver as any, 'detectLeaderIndex').mockResolvedValue(0);
    const query = jest.spyOn(driver, 'query');
    (fetch as unknown as jest.Mock).mockResolvedValue({ ok: true, json: async () => ({ writeReady: true, recoveryCapabilities: {
      atomicCreateAndBind: true, queryReferences, atomicDrop: true, uploadReceipts: true,
      preAggregationStatus: true, fileImportRecovery: true, jobAttemptFencing: true,
    } }) });
    await expect(driver.resolvePreAggregationBuild('logical', versionEntry, [], false)).rejects.toMatchObject({ code: 'MUTATION_UNKNOWN' });
    expect(query).not.toHaveBeenCalled();
    expect((fetch as unknown as jest.Mock).mock.calls.every(([url]) => String(url).endsWith('/router/status'))).toBe(true);
  });

  it('passes configured Basic auth to WebSocket connections without opening a connection', () => {
    const driver = new CubeStoreDriver({ host: 'localhost', user: 'u', password: 'p' });
    expect((driver as any).connections[0].headers.Authorization).toBe('Basic dTpw');
  });
});
