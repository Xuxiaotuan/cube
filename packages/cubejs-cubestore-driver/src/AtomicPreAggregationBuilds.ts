import { createHash } from 'crypto';
import { AtomicPreAggregationLedger, LedgerCommand, LedgerIdentity, LedgerRecord, LedgerResult } from './AtomicPreAggregationLedger';
import { PreAggregationBuild, PreAggregationBuildStore, BuildUpload } from './PreAggregationBuildStore';
import { MutationUnknownError } from './WebSocketConnection';

type BuildStatus = { state: string; tableId?: string | number; locations?: string[]; error?: string } | null;
type Options = {
  ledger: AtomicPreAggregationLedger;
  source: PreAggregationBuildStore;
  capabilities: () => Promise<void>;
  status: (table: string) => Promise<BuildStatus>;
  upload: (upload: BuildUpload) => Promise<string>;
  sleep: (ms: number) => Promise<void>;
  timeout: () => number;
};

const leaseMillis = '3600000';
const identity = ({ key, generation, owner }: LedgerRecord): LedgerIdentity => ({ key, generation, owner });
const requestId = (...parts: unknown[]) => createHash('sha256').update(JSON.stringify(parts)).digest('hex');
const unknown = (message: string) => new MutationUnknownError(message);

/** The cache supplies source bytes; only the ledger can authorize their use. */
export class AtomicPreAggregationBuilds {
  public constructor(private readonly options: Options) {}

  private async mutate(id: string, command: LedgerCommand): Promise<LedgerResult> {
    try {
      return await this.options.ledger.mutate(id, command);
    } catch (_error) {
      // Only the server's durable request receipt authorizes this retry. Neither
      // a missing table nor a failed connection authorizes replaying ordinary SQL.
      try {
        return await this.options.ledger.mutate(id, command);
      } catch (error) {
        throw new MutationUnknownError(`Atomic ${command.op} outcome unknown; requestId=${id}`, error as Error);
      }
    }
  }

  private accepted(result: LedgerResult, key: string, owner: string, generation?: string): LedgerRecord {
    if (result.rejection || !result.record || result.record.key !== key || result.record.owner !== owner ||
        (generation !== undefined && result.record.generation !== generation)) {
      throw unknown(`Atomic ledger rejected or mismatched ${key}: ${result.rejection || 'invalid identity'}`);
    }
    return result.record;
  }

  private async authority(source: PreAggregationBuild): Promise<LedgerRecord> {
    const pointer = source.atomicLedger;
    if (!pointer || pointer.key !== source.buildId) throw unknown(`Missing authority locator: ${source.buildId}`);
    const record = await this.options.ledger.read(pointer.key, pointer.generation);
    if (!record || record.key !== pointer.key || record.generation !== pointer.generation || record.owner !== pointer.owner) {
      throw unknown(`Authority identity missing or changed: ${source.buildId}`);
    }
    return record;
  }

  private async source(table: string): Promise<PreAggregationBuild> {
    const source = await this.options.source.read(table);
    if (!source) throw unknown(`Missing source intent for authoritative build: ${table}`);
    return source;
  }

  private versionFor(table: string, versionEntry: any): any {
    const prefix = `${versionEntry.table_name}_${versionEntry.content_version}_${versionEntry.structure_version}_`;
    if (!table.startsWith(prefix)) throw unknown(`Selection identity/version mismatch: ${table}`);
    const suffix = table.slice(prefix.length);
    const seconds = parseInt(suffix, versionEntry.naming_version === 2 ? 32 : 10);
    const timestamp = versionEntry.naming_version === 2 ? seconds * 1000 : seconds;
    if (!Number.isSafeInteger(timestamp) || timestamp < 0 ||
        seconds.toString(versionEntry.naming_version === 2 ? 32 : 10) !== suffix) {
      throw unknown(`Invalid selected build timestamp: ${table}`);
    }
    return { ...versionEntry, last_updated_at: timestamp };
  }

  private target(versionEntry: any): string {
    const timestamp = versionEntry.naming_version === 2
      ? Math.floor(versionEntry.last_updated_at / 1000).toString(32) : versionEntry.last_updated_at;
    return `${versionEntry.table_name}_${versionEntry.content_version}_${versionEntry.structure_version}_${timestamp}`;
  }

  public async resolve(key: string, versionEntry: any, protectedTables: string[], force: boolean): Promise<PreAggregationBuild> {
    await this.options.capabilities();
    const selectionKey = `PRE_AGG_SELECTION_V1:${requestId(key)}`;
    let selection = await this.options.ledger.read(selectionKey);
    let candidate = { ...versionEntry };
    let advance = selection === null;
    if (selection) {
      const previous = await this.options.ledger.read(selection.owner);
      if (previous?.state === 'dropped' || (previous?.state === 'published' && force)) {
        const selectedVersion = this.versionFor(selection.owner, versionEntry);
        candidate.last_updated_at = Math.max(candidate.last_updated_at, selectedVersion.last_updated_at + 1000);
        // Selection and physical publication are separate transactions. Advance
        // only after the physical outcome is authoritative, never on cache loss.
        if (selection.state !== 'retired') {
          selection = this.accepted(await this.mutate(requestId('retire-selection', identity(selection)), {
            op: 'retire', ...identity(selection),
          }), selectionKey, selection.owner, selection.generation);
        }
        advance = true;
      } else if (selection.state !== 'claimed') {
        throw unknown(`Selection is terminal without a completed physical build: ${selectionKey}`);
      }
    }
    if (advance) {
      const command: LedgerCommand = { op: 'claim', key: selectionKey, owner: this.target(candidate),
        leaseMillis, expectedGeneration: selection?.generation || null };
      const result = await this.mutate(requestId('selection', command), command);
      if (result.rejection === 'LEDGER_GENERATION_CONFLICT' || result.rejection === 'LEDGER_CLAIM_BUSY') {
        selection = await this.options.ledger.read(selectionKey);
        if (!selection || selection.state !== 'claimed') throw unknown(`Selection race unresolved: ${selectionKey}`);
      } else {
        selection = this.accepted(result, selectionKey, command.owner);
      }
    }
    if (!selection) throw unknown(`Missing atomic selection: ${selectionKey}`);
    const buildId = selection.owner;
    const selectedVersion = this.versionFor(buildId, versionEntry);
    let physical = await this.options.ledger.read(buildId);
    const existingSource = await this.options.source.read(buildId);
    if (!physical) {
      if (existingSource) throw unknown(`Source intent exists but its authority disappeared: ${buildId}`);
      const command: LedgerCommand = { op: 'claim', key: buildId, owner: `build:${buildId}`, leaseMillis, expectedGeneration: null };
      physical = this.accepted(await this.mutate(requestId('physical', command), command), buildId, command.owner);
      await this.options.source.save({ buildId, versionEntry: selectedVersion, protectedTables,
        phase: 'selected', createdAt: Date.now(), atomicLedger: identity(physical) });
    }
    const source = await this.source(buildId);
    await this.authority(source);
    // A missing cache intent after a claim is UNKNOWN, not permission to invent
    // fresh source data for an in-flight create request.
    return source;
  }

  private checkManifest(source: PreAggregationBuild, record: LedgerRecord): void {
    const manifest = record.manifest;
    const create = source.atomicCreate;
    if (!manifest || !create || `${manifest.schema}.${manifest.table}` !== source.buildId ||
        manifest.schema !== create.schema || manifest.table !== create.table ||
        manifest.contentVersion !== source.versionEntry.content_version ||
        manifest.structureVersion !== source.versionEntry.structure_version ||
        JSON.stringify(manifest.locations) !== JSON.stringify(create.locations) ||
        JSON.stringify(manifest.locations) !== JSON.stringify(source.create?.params) ||
        (source.tableId != null && String(source.tableId) !== manifest.tableId)) {
      throw unknown(`Authoritative manifest differs from source intent: ${source.buildId}`);
    }
  }

  private async confirmPublished(source: PreAggregationBuild, record: LedgerRecord): Promise<void> {
    this.checkManifest(source, record);
    const status = await this.options.status(source.buildId);
    if (!status || status.state !== 'ready' ||
        (typeof status.tableId === 'number' && !Number.isSafeInteger(status.tableId)) ||
        String(status.tableId) !== record.manifest!.tableId ||
        JSON.stringify(status.locations) !== JSON.stringify(record.manifest!.locations)) {
      throw unknown(`Published physical table/locations could not be confirmed: ${source.buildId}`);
    }
    await this.options.source.save({ ...source, phase: 'ready', tableId: record.manifest!.tableId });
  }

  private async renew(source: PreAggregationBuild): Promise<LedgerRecord> {
    const current = await this.authority(source);
    if (current.state === 'published') return current;
    if (!['claimed', 'bound'].includes(current.state)) throw unknown(`Build cannot be renewed: ${source.buildId}`);
    return this.accepted(await this.mutate(requestId('renew', identity(current), current.leaseUntilMillis), {
      op: 'renew', ...identity(current), leaseMillis,
    }), current.key, current.owner, current.generation);
  }

  public async withLease<T>(table: string, action: () => Promise<T>): Promise<T> {
    const source = await this.source(table);
    await this.renew(source);
    let failure: unknown;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let pending: Promise<void> = Promise.resolve();
    const schedule = () => {
      timer = setTimeout(() => {
        pending = this.renew(source).then(() => { if (!stopped) schedule(); }, error => { failure = error; });
      }, 60000);
    };
    schedule();
    try {
      const result = await action();
      stopped = true;
      if (timer) clearTimeout(timer);
      await pending;
      if (failure) throw failure;
      return result;
    } finally {
      stopped = true;
      if (timer) clearTimeout(timer);
      await pending;
    }
  }

  public async resume(table: string): Promise<boolean> {
    await this.options.capabilities();
    const source = await this.source(table);
    const current = await this.authority(source);
    if (current.state === 'claimed' && source.phase === 'selected' && !source.create && !source.atomicCreate) return false;
    await this.create(table);
    return true;
  }

  public async create(table: string): Promise<any[]> {
    let source = await this.source(table);
    let current = await this.authority(source);
    if (current.state === 'claimed') {
      if (!source.atomicCreate || !source.create || source.phase === 'ready') throw unknown(`Missing typed CREATE intent: ${table}`);
      await this.renew(source);
      for (const upload of source.uploads || []) await this.options.upload(upload);
      source = { ...source, phase: 'create' };
      await this.options.source.save(source);
      const result = await this.mutate(requestId('create', identity(current)), {
        op: 'create', ...identity(current), create: source.atomicCreate!,
      });
      this.accepted(result, current.key, current.owner, current.generation);
      // Receipts are historical. Always reconcile the current authority after a
      // duplicate response; never regress a Published or Retired record to Bound.
      current = await this.authority(source);
    }
    if (current.state === 'published') {
      await this.confirmPublished(source, current);
      return [];
    }
    if (current.state !== 'bound') throw unknown(`Build is not publishable: ${table} (${current.state})`);
    this.checkManifest(source, current);
    await this.publish(source, true);
    return [];
  }

  private async publish(source: PreAggregationBuild, wait: boolean): Promise<boolean> {
    const deadline = Date.now() + Math.max(1, this.options.timeout());
    const maxPolls = wait ? Math.ceil(Math.max(1, this.options.timeout()) / 250) + 1 : 1;
    for (let poll = 0; poll < maxPolls; poll++) {
      source = await this.source(source.buildId);
      const current = await this.authority(source);
      if (current.state === 'published') {
        await this.confirmPublished(source, current);
        return true;
      }
      if (current.state !== 'bound') throw unknown(`Publish authority changed: ${source.buildId}`);
      this.checkManifest(source, current);
      await this.renew(source);
      const attempt = source.publishAttempt || 0;
      const result = await this.mutate(requestId('publish', identity(current), attempt), { op: 'publish', ...identity(current) });
      if (result.rejection === 'LEDGER_IMPORT_RECEIPT_MISSING' || result.rejection === 'LEDGER_TABLE_NOT_READY') {
        // Only a durable NOT_READY rejection advances the request identity.
        // On UNKNOWN this write is never reached; resume uses the SAME attempt.
        await this.options.source.save({ ...source, publishAttempt: attempt + 1 });
        if (!wait) return false;
        if (Date.now() >= deadline) break;
        await this.options.sleep(Math.min(250, Math.max(0, deadline - Date.now())));
      } else {
        this.accepted(result, current.key, current.owner, current.generation);
        const published = await this.authority(source);
        if (published.state !== 'published') throw unknown(`Publish response is not current: ${source.buildId}`);
        await this.confirmPublished(source, published);
        return true;
      }
    }
    throw unknown(`Pre-aggregation still requires authoritative publication: ${source.buildId}`);
  }

  public async readyTables(schema: string, tables: any[]): Promise<any[]> {
    await this.options.capabilities();
    const ready: any[] = [];
    for (const table of tables) {
      const name = `${schema}.${table.table_name || table.TABLE_NAME}`;
      const current = await this.options.ledger.read(name);
      if (!current) {
        if (await this.options.source.read(name)) throw unknown(`Known build lost its ledger: ${name}`);
        continue; // Legacy/unmanaged tables are not strict publications.
      }
      if (!['bound', 'published'].includes(current.state)) continue;
      const source = await this.source(name);
      const exact = await this.authority(source);
      if (exact.generation !== current.generation) throw unknown(`Discovery generation mismatch: ${name}`);
      if (await this.publish(source, false)) ready.push(table);
    }
    return ready;
  }

  public async protectedTables(): Promise<string[]> {
    await this.options.capabilities();
    const protectedTables = new Set<string>();
    await this.options.source.visit(async source => {
      const current = await this.authority(source);
      if (current.state === 'claimed' || current.state === 'bound') {
        protectedTables.add(source.buildId);
        source.protectedTables.forEach(table => protectedTables.add(table));
      }
    });
    return [...protectedTables];
  }

  public async retireAndDrop(table: string): Promise<boolean> {
    await this.options.capabilities();
    const source = await this.source(table);
    let current = await this.authority(source);
    if (current.state === 'dropped') return true;
    this.checkManifest(source, current);
    if (current.state === 'published') {
      const attempt = source.retireAttempt || 0;
      const result = await this.mutate(requestId('retire', identity(current), attempt), { op: 'retire', ...identity(current) });
      if (result.rejection === 'LEDGER_REFERENCED') {
        await this.options.source.save({ ...source, retireAttempt: attempt + 1 });
        return false;
      }
      current = this.accepted(result, current.key, current.owner, current.generation);
    }
    if (current.state !== 'retired') throw unknown(`Refusing to drop an unresolved build: ${table}`);
    await this.options.source.save({ ...source, phase: 'retired' });
    const dropped = this.accepted(await this.mutate(requestId('drop', identity(current)), {
      op: 'drop', ...identity(current),
    }), current.key, current.owner, current.generation);
    if (dropped.state !== 'dropped' || (await this.authority(source)).state !== 'dropped') {
      throw unknown(`Atomic drop outcome unresolved: ${table}`);
    }
    return true;
  }
}
