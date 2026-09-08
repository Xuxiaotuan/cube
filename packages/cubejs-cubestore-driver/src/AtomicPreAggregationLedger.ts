import { MutationUnknownError } from './WebSocketConnection';

// Integers remain strings on the wire: RocksDB identifiers can exceed 2^53.
export type LedgerManifest = {
  tableId: string;
  schema: string;
  table: string;
  locations: string[];
  contentVersion: string;
  structureVersion: string;
};
export type LedgerIdentity = { key: string; generation: string; owner: string };
export type LedgerCreate = {
  schema: string;
  table: string;
  columns: { name: string; columnType: string }[];
  locations: string[];
  indexes: { name: string; columns: string[]; type: 'regular' | 'aggregate' }[];
  importFormat: 'csv' | 'csvNoHeader';
  contentVersion: string;
  structureVersion: string;
  buildRangeEnd?: string;
  delimiter?: string;
  disableQuoting?: boolean;
};
export type LedgerCommand =
  | { op: 'claim'; key: string; owner: string; leaseMillis: string; expectedGeneration: string | null }
  | (LedgerIdentity & { op: 'create'; create: LedgerCreate })
  | (LedgerIdentity & { op: 'renew'; leaseMillis: string })
  | (LedgerIdentity & { op: 'bind'; manifest: LedgerManifest })
  | (LedgerIdentity & { op: 'publish' | 'retire' | 'drop' })
  | { op: 'acquire' | 'release'; key: string; generation: string; referenceId: string };
export type LedgerRequest = { requestId: string; command: LedgerCommand };
export type LedgerRecord = {
  key: string;
  generation: string;
  owner: string;
  leaseUntilMillis: string;
  state: 'claimed' | 'bound' | 'published' | 'retired' | 'dropped';
  manifest: LedgerManifest | null;
  references: string;
};
export type LedgerResult = { record: LedgerRecord | null; rejection: string | null };
export type LedgerTransport = (method: 'GET' | 'POST', path: string, body?: LedgerRequest) => Promise<unknown>;

const endpoint = '/router/pre-aggregation-ledger';
const decimal = (value: unknown): value is string => typeof value === 'string' && /^(0|[1-9][0-9]*)$/.test(value);
const object = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object';
const validManifest = (value: unknown): value is LedgerManifest => object(value) && decimal(value.tableId) &&
  ['schema', 'table', 'contentVersion', 'structureVersion'].every((key) => typeof value[key] === 'string') &&
  Array.isArray(value.locations) && value.locations.every((location) => typeof location === 'string');
const validRecord = (value: unknown): value is LedgerRecord => object(value) &&
  typeof value.key === 'string' && typeof value.owner === 'string' && decimal(value.generation) &&
  decimal(value.leaseUntilMillis) && decimal(value.references) &&
  ['claimed', 'bound', 'published', 'retired', 'dropped'].includes(value.state as string) &&
  (value.manifest === null || validManifest(value.manifest));

/** Explicit protocol client, not a replacement for query-lifetime reference integration. */
export class AtomicPreAggregationLedger {
  public constructor(private readonly transport: LedgerTransport) {}

  public async mutate(requestId: string, command: LedgerCommand): Promise<LedgerResult> {
    if (!requestId) {
      throw new Error('An immutable requestId is required for ledger mutation');
    }
    // Never invent a fresh requestId or retry an ambiguous write automatically.
    const result = await this.transport('POST', endpoint, { requestId, command });
    if (!object(result) || !(result.record === null || validRecord(result.record)) ||
        !(result.rejection === null || typeof result.rejection === 'string') ||
        (result.record === null && result.rejection === null)) {
      throw new MutationUnknownError('Invalid atomic ledger mutation receipt; reconcile using the same requestId');
    }
    return result as LedgerResult;
  }

  public async read(key: string, generation?: string): Promise<LedgerRecord | null> {
    const query = `?key=${encodeURIComponent(key)}${generation === undefined ? '' : `&generation=${encodeURIComponent(generation)}`}`;
    const record = await this.transport('GET', `${endpoint}${query}`);
    if (record !== null && !validRecord(record)) {
      throw new MutationUnknownError('Invalid atomic ledger lookup response');
    }
    // Missing is only a ledger lookup result, never proof that an earlier SQL write had no effect.
    return record as LedgerRecord | null;
  }
}
