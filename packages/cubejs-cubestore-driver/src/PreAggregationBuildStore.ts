import { createHash } from 'crypto';
import { LedgerCreate, LedgerIdentity } from './AtomicPreAggregationLedger';
import { MutationUnknownError } from './WebSocketConnection';

export type BuildUpload = { name: string; sha256: string; size: number; path: string };

export type PreAggregationBuild = {
  buildId: string;
  versionEntry: any;
  phase: 'selected' | 'uploading' | 'uploaded' | 'create' | 'ready' | 'failed' | 'retired';
  protectedTables: string[];
  uploads?: BuildUpload[];
  manifestHash?: string;
  create?: { sql: string; params: string[] };
  tableId?: string | number;
  error?: string;
  createdAt?: number;
  // Source intent and an authority locator, never a cache-based claim receipt.
  atomicLedger?: LedgerIdentity;
  atomicCreate?: LedgerCreate;
  publishAttempt?: number;
  retireAttempt?: number;
};

/** Business intent lives in the existing shared Cube Store CacheStore, without a TTL.
 * A cache loss is not proof that a previously dispatched mutation had no effect.
 */
export class PreAggregationBuildStore {
  private readonly prefix = 'PRE_AGG_BUILD_V1:';

  public constructor(private readonly query: (sql: string, values: any[]) => Promise<any[]>, private readonly strict = false) {}

  private async get(key: string) {
    const rows = await this.query('CACHE GET ?', [key]);
    return rows.length ? JSON.parse(rows[0].value) : null;
  }

  private async set(key: string, value: any, nx = false) {
    const serialized = JSON.stringify(value);
    try {
      await this.query(nx ? 'CACHE SET NX ? ?' : 'CACHE SET ? ?', [key, serialized]);
    } catch (error) {
      // Lost acknowledgement of a state write: read back, never blindly replay.
      const actual = await this.get(key);
      if (!(nx && actual) && JSON.stringify(actual) !== serialized) throw error;
    }
  }

  public async read(table: string): Promise<PreAggregationBuild | null> {
    const identity: PreAggregationBuild | null = await this.get(`${this.prefix}${table}`);
    if (!identity) return null;
    if (this.strict) return identity;
    // Immutable phase markers make progress monotonic even when a stale queue
    // owner writes after another worker completed the build.
    let record = identity;
    for (const phase of ['retired', 'ready', 'failed', 'create', 'uploaded', 'uploading']) {
      const snapshot = await this.get(`PRE_AGG_PHASE_V1:${table}:${phase}`);
      if (snapshot) {
        record = snapshot;
        break;
      }
    }
    const tableId = await this.get(`PRE_AGG_TABLE_ID_V1:${table}`);
    return tableId === null ? record : { ...record, tableId };
  }

  public async save(record: PreAggregationBuild): Promise<void> {
    if (this.strict) {
      if (!record.atomicLedger || record.atomicLedger.key !== record.buildId) {
        throw new MutationUnknownError(`Missing atomic ledger locator: ${record.buildId}`);
      }
      const current = await this.read(record.buildId);
      for (const field of ['atomicLedger', 'create', 'atomicCreate', 'manifestHash'] as const) {
        if (current?.[field] !== undefined && record[field] !== undefined &&
            JSON.stringify(current[field]) !== JSON.stringify(record[field])) {
          throw new MutationUnknownError(`Conflicting source intent ${field}: ${record.buildId}`);
        }
      }
      if (current?.tableId != null && record.tableId != null && String(current.tableId) !== String(record.tableId)) {
        throw new MutationUnknownError(`Conflicting source table identity: ${record.buildId}`);
      }
      const next = { ...current, ...record,
        protectedTables: [...new Set([...(current?.protectedTables || []), ...record.protectedTables])],
        publishAttempt: Math.max(current?.publishAttempt || 0, record.publishAttempt || 0),
        retireAttempt: Math.max(current?.retireAttempt || 0, record.retireAttempt || 0),
      };
      if (current && ['ready', 'retired'].includes(current.phase) && !['ready', 'retired'].includes(next.phase)) {
        next.phase = current.phase;
      }
      // This ordinary cache write is only a recovery aid. All mutations and
      // publication are fenced by the authoritative ledger, not this read/write.
      await this.set(`${this.prefix}${record.buildId}`, next);
      return;
    }
    if (record.create) {
      // Paths belong to one worker. Only immutable transmitted bytes and the
      // complete CREATE fingerprint participate in shared manifest identity.
      const manifest = {
        create: record.create,
        uploads: record.uploads?.map(({ name, sha256, size }) => ({ name, sha256, size })),
        manifestHash: record.manifestHash,
      };
      const key = `PRE_AGG_MANIFEST_V1:${record.buildId}`;
      await this.set(key, manifest, true);
      if (JSON.stringify(await this.get(key)) !== JSON.stringify(manifest)) {
        throw Object.assign(new Error(`Immutable pre-aggregation manifest conflict: ${record.buildId}`), { code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError' });
      }
    }
    if (record.tableId != null) {
      const key = `PRE_AGG_TABLE_ID_V1:${record.buildId}`;
      await this.set(key, String(record.tableId), true);
      if (await this.get(key) !== String(record.tableId)) {
        throw Object.assign(new Error(`Table identity changed for ${record.buildId}`), { code: 'MUTATION_UNKNOWN', name: 'MutationUnknownError' });
      }
    }
    const current = await this.read(record.buildId);
    if (current && ['ready', 'failed', 'retired'].includes(current.phase) &&
        !['ready', 'failed', 'retired'].includes(record.phase)) return;
    await this.set(`PRE_AGG_PHASE_V1:${record.buildId}:${record.phase}`, record, true);
  }

  public async resolve(key: string, candidate: PreAggregationBuild, force = false): Promise<PreAggregationBuild> {
    if (this.strict) throw new MutationUnknownError('Strict build selection requires the atomic ledger');
    candidate = { ...candidate, createdAt: candidate.createdAt ?? Date.now() };
    // NX selects one durable target before enqueueing. Terminal attempts form a
    // chain so retiring an old attempt cannot remove another worker's new claim.
    // Walk existing terminal attempts, rather than permanently locking a
    // logical refresh key after a fixed number of failures.
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const activeKey = `PRE_AGG_ACTIVE_V1:${createHash('sha256').update(key).digest('hex')}`;
      await this.set(activeKey, candidate, true);
      const selected: PreAggregationBuild = await this.get(activeKey);
      if (!selected) throw new Error('Pre-aggregation build identity disappeared');
      await this.set(`${this.prefix}${selected.buildId}`, selected, true);
      const current = await this.read(selected.buildId);
      if (!current) throw new Error('Pre-aggregation build state disappeared');
      if (current.phase !== 'failed' && current.phase !== 'retired' && (current.phase !== 'ready' || !force)) return current;
      if (current.buildId === candidate.buildId) {
        const versionEntry = { ...candidate.versionEntry, last_updated_at: current.versionEntry.last_updated_at + 1000 };
        const timestamp = versionEntry.naming_version === 2 ? Math.floor(versionEntry.last_updated_at / 1000).toString(32) : versionEntry.last_updated_at;
        candidate = { ...candidate, createdAt: Date.now(), versionEntry, buildId: `${versionEntry.table_name}_${versionEntry.content_version}_${versionEntry.structure_version}_${timestamp}` };
      }
      key = `${key}:${current.buildId}`;
    }
  }

  /** Walk every identity with bounded in-flight reads; never truncate history.
   * CACHE KEYS is still an unpaged enumeration, not a transactional snapshot.
   */
  public async visit(visitor: (record: PreAggregationBuild) => Promise<void>, concurrency = 8): Promise<void> {
    if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 8) throw new Error('Invalid build scan concurrency');
    const keys = await this.query('CACHE KEYS ?', [this.prefix]);
    let cursor = 0;
    let failure: unknown;
    let failed = false;
    await Promise.all(Array.from({ length: Math.min(concurrency, keys.length) }, async () => {
      while (!failed && cursor < keys.length) {
        const { key } = keys[cursor++];
        try {
          const record = await this.read(key.slice(this.prefix.length));
          if (!record) throw new Error(`Pre-aggregation build identity disappeared: ${key}`);
          await visitor(record);
        } catch (error) {
          failed = true;
          failure = error;
        }
      }
    }));
    if (failed) throw failure;
  }

  public async protectedTables(): Promise<string[]> {
    const result = new Set<string>();
    await this.visit(async record => {
      if (!['ready', 'failed', 'retired'].includes(record.phase)) {
        result.add(record.buildId);
        record.protectedTables.forEach(table => result.add(table));
      }
    });
    return [...result];
  }
}
