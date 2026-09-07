import { createHash } from 'crypto';
import { pipeline, Readable, Writable } from 'stream';
import { createGzip } from 'zlib';
import { createReadStream, createWriteStream } from 'fs';
import { access } from 'fs/promises';
import { unlink } from 'fs-extra';
import tempy from 'tempy';
import csvWriter from 'csv-write-stream';
import { createClient } from 'redis';
import {
  BaseDriver,
  CreateTableIndex,
  DownloadTableCSVData,
  DownloadTableMemoryData,
  DriverInterface,
  ExternalCreateTableOptions,
  ExternalDriverCompatibilities,
  IndexesSQL,
  QueryOptions,
  StreamingSourceTableData,
  StreamTableData,
  TableColumnQueryResult,
  TableStructure,
} from '@cubejs-backend/base-driver';
import { AsyncDebounce, getEnv, isVersionGte } from '@cubejs-backend/shared';
import { escape, format as formatSql } from 'sqlstring';
import fetch from 'node-fetch';

import { ConnectionConfig } from './types';
import { ConnectionError } from './errors';
import { WebSocketConnection, MutationUnknownError } from './WebSocketConnection';
import {
  ExistingResult,
  IdempotencyOwnershipLostError,
  MutationLease,
  RedisIdempotencyStore,
  SerializedError,
} from './IdempotencyStore';
import { QueryResultFormat } from '../codegen';
import { BuildUpload, PreAggregationBuild, PreAggregationBuildStore } from './PreAggregationBuildStore';

const CubeStoreCapabilityMinVersion = {
  queueExclusive: '1.6.22',
  queueExternalId: '1.6.26',
  sendableParameters: '1.6.38',
  arrowFormat: '1.6.66',
} satisfies Record<string, string>;
type CubeStoreCapability = keyof typeof CubeStoreCapabilityMinVersion;

const GenericTypeToCubeStore: Record<string, string> = {
  string: 'varchar(255)',
  text: 'varchar(255)',
  uuid: 'varchar(64)',
  // Cube Store uses an old version of sql parser which doesn't support timestamp with custom precision, but
  // athena driver (I believe old version) allowed to use it
  'timestamp(3)': 'timestamp',
  // TODO comes from JDBC. We might consider decimal96 here
  bigdecimal: 'decimal'
};

type Column = {
  type: string;
  name: string;
};

type CreateTableOptions = {
  streamOffset?: string;
  inputFormat?: string
  buildRangeEnd?: string
  uniqueKey?: string
  indexes?: string
  files?: string[]
  aggregations?: string
  selectStatement?: string
  sourceTable?: any
  sealAt?: string
  delimiter?: string
  disableQuoting?: boolean
};

type CubeStoreQueryOptions = QueryOptions & {
  sendParameters?: boolean,
  responseFormat?: QueryResultFormat,
  retryable?: boolean,
  mutationId?: string,
  preAggregationBuildId?: string,
};

type RouterStatusPayload = {
  is_leader?: unknown;
  timestamp_unix_secs?: unknown;
  node_name?: unknown;
  activeLeader?: unknown;
  leaderState?: unknown;
  leaderEpoch?: unknown;
};

type RouterRoleStatePayload = {
  activeLeader?: unknown;
  leaderEpoch?: unknown;
};

export class CubeStoreDriver extends BaseDriver implements DriverInterface {
  private readonly preAggregationBuilds = new PreAggregationBuildStore((sql, values) => this.query(sql, values));

  protected readonly preAggregationReconcileTimeoutMs = Number(process.env.CUBE_STORE_PRE_AGGREGATION_RECONCILE_TIMEOUT_MS) || 120000;

  protected readonly config: any;

  protected readonly connections: WebSocketConnection[];

  protected readonly routerStatusUrls: string[];

  protected readonly routerBaseUrls: string[];

  protected activeConnectionIndex = 0;

  protected leaderProbeInProgress: Promise<number | null> | null = null;

  protected leaderProbeExpiresAt = 0;

  protected leaderIndexCache: number | null = null;

  protected leaderEpochCache: number | null = null;

  protected leaderIndexCommitted: number | null = null;

  protected readonly leaderProbeUseStatus: boolean = getEnv(
    'cubeStoreRouterLeaderProbeUseStatus',
  ) !== 'false';

  protected readonly leaderProbeStatusMaxAgeSecs: number = (() => {
    const raw = getEnv('cubeStoreRouterLeaderStatusMaxAgeSecs');
    if (!raw) {
      return 90;
    }
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : 90;
  })();

  protected readonly leaderProbeTimeoutMs: number = Number(
    getEnv('cubeStoreRouterLeaderProbeTimeoutMs') || 1500,
  );

  protected readonly leaderProbeStatusTimeoutMs: number = Number(
    getEnv('cubeStoreRouterLeaderStatusProbeTimeoutMs') || 900,
  );

  protected readonly leaderProbeTtlMs: number = Number(
    getEnv('cubeStoreRouterLeaderProbeTtlMs') || 2500,
  );

  protected readonly leaderEpochFence: boolean = getEnv(
    'cubeStoreRouterLeaderEpochFence',
  ) !== 'false';

  protected readonly leaderConnectionResetOnChange: boolean = getEnv(
    'cubeStoreRouterLeaderConnectionResetOnChange',
  ) !== 'false';

  protected readonly strictWriteRetryWithoutMutationId: boolean = getEnv(
    'cubeStoreStrictWriteRetryWithoutMutationId',
  ) !== 'false';

  protected readonly idempotencyRedisDsn?: string;

  protected readonly idempotencyRedisKeyPrefix: string;

  protected readonly idempotencyCompletedTtlSeconds: number;

  protected readonly idempotencyFailedTtlSeconds: number;

  protected readonly idempotencyPendingTtlSeconds: number;

  protected readonly idempotencyPollIntervalMs: number;

  protected readonly idempotencyPollMaxAttempts: number;

  protected idempotencyRedisClient: any = null;

  protected idempotencyRedisConnecting: Promise<void> | null = null;

  protected idempotencyStore: RedisIdempotencyStore | null = null;

  protected closeAllConnections(): void {
    this.connections.forEach((connection) => connection.close());
  }

  protected async maybeResetOnLeaderShift(nextLeaderIndex: number | null): Promise<void> {
    if (!this.leaderConnectionResetOnChange) {
      this.leaderIndexCommitted = nextLeaderIndex;
      return;
    }

    if (this.leaderIndexCommitted === nextLeaderIndex) {
      return;
    }

    if (nextLeaderIndex === null) {
      this.leaderIndexCommitted = null;
      return;
    }

    this.closeAllConnections();
    this.leaderIndexCommitted = nextLeaderIndex;
  }

  protected parseLeaderEpoch(raw: unknown): number | null {
    if (typeof raw === 'number' && Number.isFinite(raw)) {
      return Math.trunc(raw);
    }

    if (typeof raw === 'string') {
      const value = Number(raw.trim());
      if (!Number.isFinite(value)) {
        return null;
      }
      return Math.trunc(value);
    }

    return null;
  }

  protected parseEnvInt(raw: string | undefined, fallback: number, minValue = 0): number {
    const value = Number(raw);
    if (!Number.isFinite(value)) {
      return fallback;
    }
    const rounded = Math.trunc(value);
    if (rounded < minValue) {
      return fallback;
    }
    return rounded;
  }

  protected async getIdempotencyRedisClient(): Promise<any | null> {
    if (!this.idempotencyRedisDsn) {
      return null;
    }

    if (this.idempotencyRedisClient?.isOpen) {
      return this.idempotencyRedisClient;
    }

    if (!this.idempotencyRedisConnecting) {
      const connectingPromise = (async () => {
        const client = createClient({ url: this.idempotencyRedisDsn });
        this.idempotencyRedisClient = client;
        await client.connect();
      })();
      this.idempotencyRedisConnecting = connectingPromise;
    }

    try {
      await this.idempotencyRedisConnecting;
      return this.idempotencyRedisClient;
    } catch {
      await this.idempotencyRedisClient?.quit();
      this.idempotencyRedisClient = null;
      return null;
    } finally {
      this.idempotencyRedisConnecting = null;
    }
  }

  protected serializeError(error: any): SerializedError {
    if (error instanceof Error) {
      return {
        name: error.name || 'Error',
        message: error.message || String(error),
        code: (error as any).code ? `${(error as any).code}` : undefined,
        stack: error.stack?.slice(0, 4096),
      };
    }

    return {
      name: 'Error',
      message: `${error}`,
    };
  }

  protected buildIdempotencyError(record: SerializedError): Error {
    const error = new Error(record.message || 'idempotent mutation previously failed');
    error.name = record.name || 'Error';
    (error as any).code = record.code;
    if (record.stack) {
      error.stack = record.stack;
    }
    return error;
  }

  protected sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  protected stableStringify(value: any): string {
    return JSON.stringify(value, (key, nestedValue) => {
      if (key === 'mutationId' || key === 'mutation_id') {
        return undefined;
      }

      if (nestedValue instanceof Date) {
        return nestedValue.toISOString();
      }
      if (typeof nestedValue === 'bigint') {
        return `${nestedValue.toString()}n`;
      }
      if (Buffer.isBuffer(nestedValue)) {
        return `__buffer__:${nestedValue.toString('base64')}`;
      }
      if (nestedValue && typeof nestedValue === 'object' && !Array.isArray(nestedValue)) {
        return Object.keys(nestedValue).sort().reduce((acc, k) => {
          acc[k] = nestedValue[k];
          return acc;
        }, {} as any);
      }
      return nestedValue;
    });
  }

  protected createIdempotencyFingerprintWithCanonicalQuery(sql: string, values: any[]): string {
    return createHash('sha256').update(this.stableStringify([sql, values])).digest('hex');
  }

  protected async getIdempotencyStore(): Promise<RedisIdempotencyStore> {
    const redis = await this.getIdempotencyRedisClient();
    if (!redis) {
      throw new Error('CubeStore mutation idempotency Redis is required but unavailable');
    }
    if (!this.idempotencyStore || this.idempotencyStore.client !== redis) {
      this.idempotencyStore = new RedisIdempotencyStore(redis, {
        keyPrefix: this.idempotencyRedisKeyPrefix,
        pendingTtlSeconds: this.idempotencyPendingTtlSeconds,
        completedTtlSeconds: this.idempotencyCompletedTtlSeconds,
        failedTtlSeconds: this.idempotencyFailedTtlSeconds,
      });
    }
    return this.idempotencyStore;
  }

  public constructor(config?: Partial<ConnectionConfig>) {
    super();

    this.config = {
      batchingRowSplitCount: getEnv('batchingRowSplitCount'),
      ...config,
      // We use ip here instead of localhost, because Node.js 18 resolve localhost to IPV6 by default
      // https://github.com/node-fetch/node-fetch/issues/1624
      host: config?.host || getEnv('cubeStoreHost') || '127.0.0.1',
      port: config?.port || getEnv('cubeStorePort') || '3030',
      user: config?.user || getEnv('cubeStoreUser'),
      password: config?.password || getEnv('cubeStorePass'),
    };

    this.idempotencyRedisDsn = process.env.CUBE_STORE_IDEMPOTENCY_REDIS_DSN
      || process.env.CUBEJS_CUBESTORE_IDEMPOTENCY_REDIS_DSN
      || process.env.CUBEJS_CUBESTORE_IDEMPOTENCY_REDIS_URL;
    this.idempotencyRedisKeyPrefix = process.env.CUBE_STORE_IDEMPOTENCY_REDIS_KEY_PREFIX || 'cubejs:cubestore:mutation';
    this.idempotencyCompletedTtlSeconds = this.parseEnvInt(process.env.CUBE_STORE_IDEMPOTENCY_COMPLETED_TTL_SECONDS, 86400);
    this.idempotencyFailedTtlSeconds = this.parseEnvInt(process.env.CUBE_STORE_IDEMPOTENCY_FAILED_TTL_SECONDS, 86400);
    this.idempotencyPendingTtlSeconds = this.parseEnvInt(process.env.CUBE_STORE_IDEMPOTENCY_PENDING_TTL_SECONDS, 30);
    this.idempotencyPollIntervalMs = this.parseEnvInt(process.env.CUBE_STORE_IDEMPOTENCY_POLL_INTERVAL_MS, 250, 10);
    this.idempotencyPollMaxAttempts = this.parseEnvInt(process.env.CUBE_STORE_IDEMPOTENCY_POLL_MAX_ATTEMPTS, 120, 1);

    const rawHost = this.config.url
      ? this.config.url
      : `${this.config.host}`;
    const rawPort = this.config.port || '3030';
    const routerHosts = this.config.url
      ? [rawHost]
      : String(rawHost).split(',').map((host: string) => host.trim()).filter(Boolean);

    const baseUrls = this.normalizeRouterBaseUrls(routerHosts, rawPort);
    if (!baseUrls.length) {
      throw new Error('cubejs-cubestore-driver: no valid router endpoint configured');
    }

    this.routerBaseUrls = baseUrls;
    this.routerStatusUrls = baseUrls.map(baseUrl => this.routerStatusUrl(baseUrl));
    this.connections = baseUrls.map(baseUrl => new WebSocketConnection(`${baseUrl}/ws`));
  }

  public async hasCapability(capability: CubeStoreCapability): Promise<boolean> {
    const minVersion = CubeStoreCapabilityMinVersion[capability];

    return this.withFailover(async (connection) => isVersionGte(await connection.getCubeStoreVersion(), minVersion));
  }

  public async testConnection() {
    await this.query('SELECT 1', []);
  }

  public async query<R = any>(query: string, values: any[], options?: CubeStoreQueryOptions): Promise<R[]> {
    const {
      inlineTables,
      sendParameters,
      responseFormat,
      retryable,
      ...queryTracingObj
    } = options ?? {};

    if (!sendParameters) {
      query = formatSql(query, values || []);
    }

    const tracingObj = { ...queryTracingObj, instance: getEnv('instanceId') } as Record<string, any>;
    const isReplaySafeByDefault = this.shouldRetryOnFailure(query);
    const normalizedMutationId = this.normalizeMutationId(tracingObj.mutationId || tracingObj.mutation_id);

    if (normalizedMutationId) {
      tracingObj.mutationId = normalizedMutationId;
      tracingObj.mutation_id = normalizedMutationId;
    }

    const isMutating = !isReplaySafeByDefault;
    const requestedRetryable = typeof retryable === 'boolean' ? retryable : isReplaySafeByDefault;
    const replaySafe = this.resolveRetryablePolicy(requestedRetryable, isMutating, normalizedMutationId);
    const sendValues = sendParameters ? values : [];

    const executeQuery = () => this.withFailover(async (connection) => connection.query(query, sendValues, {
      inlineTables: inlineTables ?? [],
      queryTracingObj: tracingObj,
      responseFormat: responseFormat ?? (
        await this.hasCapability('arrowFormat') ? QueryResultFormat.Arrow : QueryResultFormat.Legacy
      ),
      replaySafe,
    }), { retryable: replaySafe });

    if (isMutating && normalizedMutationId && this.idempotencyRedisDsn) {
      return this.withMutationIdempotency(normalizedMutationId, query, sendValues, () => executeQuery());
    }

    return executeQuery();
  }

  protected resolveRetryablePolicy(requestedRetryable: boolean, isMutating: boolean, mutationId?: string): boolean {
    if (isMutating) {
      return false;
    }

    return requestedRetryable;
  }

  protected async withMutationIdempotency<R>(
    mutationId: string,
    query: string,
    values: any[],
    action: () => Promise<R[]>,
  ): Promise<R[]> {
    const store = await this.getIdempotencyStore();
    const fingerprint = this.createIdempotencyFingerprintWithCanonicalQuery(query, values);
    const acquired = await store.acquire(mutationId, fingerprint);
    if ('ownerToken' in acquired) {
      return this.executeOwnedMutation(store, acquired, action);
    }
    return this.waitForMutationCompletion(store, fingerprint, mutationId, acquired);
  }

  protected resultRef(result: unknown): string {
    const serialized = JSON.stringify(result);
    if (typeof serialized === 'string' && serialized.length <= 8192) {
      return `inline:${serialized}`;
    }
    return `unrecoverable:${createHash('sha256').update(serialized || String(result)).digest('hex')}`;
  }

  protected resultFromRef<R>(resultRef?: string): R[] {
    if (resultRef?.startsWith('inline:')) {
      try {
        return JSON.parse(resultRef.slice('inline:'.length)) as R[];
      } catch {
        throw new Error('mutation idempotency result reference is invalid');
      }
    }
    const error = new Error('mutation completed but its response is not recoverable from the idempotency store');
    (error as any).code = 'MUTATION_RESULT_UNRECOVERABLE';
    throw error;
  }

  protected async executeOwnedMutation<R>(
    store: RedisIdempotencyStore,
    initialLease: MutationLease,
    action: () => Promise<R[]>,
  ): Promise<R[]> {
    let lease = initialLease;
    let renewalFailure: Error | null = null;
    let renewalPromise: Promise<void> = Promise.resolve();
    let stopped = false;
    let renewalTimer: ReturnType<typeof setTimeout> | null = null;
    const renewalDelayMs = Math.max(1, Math.floor(this.idempotencyPendingTtlSeconds * 1000 / 3) - 10);

    const scheduleRenewal = () => {
      renewalTimer = setTimeout(() => {
        renewalPromise = (async () => {
          try {
            lease = await store.renew(lease);
            if (!stopped) {
              scheduleRenewal();
            }
          } catch (error: any) {
            renewalFailure = error instanceof Error ? error : new Error(String(error));
          }
        })();
      }, renewalDelayMs);
    };
    scheduleRenewal();

    try {
      const result = await action();
      stopped = true;
      if (renewalTimer) clearTimeout(renewalTimer);
      await renewalPromise;
      if (renewalFailure) {
        throw renewalFailure;
      }
      await store.complete(lease, this.resultRef(result));
      return result;
    } catch (error: any) {
      stopped = true;
      if (renewalTimer) clearTimeout(renewalTimer);
      await renewalPromise;

      if (renewalFailure || error instanceof IdempotencyOwnershipLostError) {
        throw error;
      }

      if (error instanceof MutationUnknownError || error?.code === 'MUTATION_UNKNOWN') {
        await store.fail(lease, {
          ...this.serializeError(error),
          code: 'MUTATION_UNKNOWN',
        });
      } else {
        await store.fail(lease, this.serializeError(error));
      }
      throw error;
    }
  }

  protected async waitForMutationCompletion<R>(
    store: RedisIdempotencyStore,
    fingerprint: string,
    mutationId: string,
    initial: ExistingResult,
  ): Promise<R[]> {
    let record: ExistingResult | null = initial;
    for (let attempt = 0; attempt < this.idempotencyPollMaxAttempts; attempt++) {
      if (!record) {
        throw new MutationUnknownError(`mutationId ${mutationId} lost its idempotency state; reconcile against authoritative metadata before retrying`);
      }
      if (record.fingerprint !== fingerprint) {
        throw new Error(`mutationId ${mutationId} conflict: fingerprint changed during in-flight replay`);
      }
      if (record.status === 'COMPLETED') {
        return this.resultFromRef<R>(record.resultRef);
      }
      if (record.status === 'FAILED') {
        throw this.buildIdempotencyError(record.error || { name: 'Error', message: `mutation ${mutationId} previously failed` });
      }
      if (record.status === 'UNKNOWN') {
        throw new MutationUnknownError(`mutationId ${mutationId} has an unknown outcome; reconcile against authoritative metadata before retrying`);
      }
      await this.sleep(this.idempotencyPollIntervalMs);
      record = await store.read(mutationId);
    }
    throw new Error(`mutationId ${mutationId} is waiting for completion for too long`);
  }

  protected normalizeMutationId(rawMutationId: unknown): string | undefined {
    if (typeof rawMutationId !== 'string') {
      return undefined;
    }

    const normalized = rawMutationId.trim();
    return normalized.length ? normalized : undefined;
  }

  public async release() {
    await this.idempotencyRedisClient?.quit();
    this.idempotencyRedisClient = null;
    this.idempotencyStore = null;
    await Promise.all(this.connections.map(async connection => connection.close()));
  }

  protected normalizeRouterBaseUrl(rawHost: string, port: string | number): string {
    const normalized = rawHost.trim();
    if (!normalized) {
      return normalized;
    }

    if (normalized.startsWith('ws://') || normalized.startsWith('wss://')) {
      const parsed = new URL(normalized);
      return `${parsed.origin}`;
    }

    if (normalized.startsWith('http://') || normalized.startsWith('https://')) {
      const parsed = new URL(normalized.replace(/^https?:\/\//, normalized.startsWith('https://') ? 'wss://' : 'ws://'));
      return `${parsed.origin}`;
    }

    return `ws://${normalized.includes(':') ? normalized : `${normalized}:${port}`}`;
  }

  protected normalizeRouterBaseUrls(routerHosts: string[], port: string | number): string[] {
    const normalized = routerHosts
      .map(host => this.normalizeRouterBaseUrl(host, port))
      .filter(Boolean)
      .map(url => url.replace(/\/$/, ''));

    return [...new Set(normalized)];
  }

  protected routerStatusUrl(baseUrl: string): string {
    if (baseUrl.startsWith('wss://')) {
      return `${baseUrl.replace(/^wss:\/\//, 'https://')}/router/status`;
    }

    if (baseUrl.startsWith('ws://')) {
      return `${baseUrl.replace(/^ws:\/\//, 'http://')}/router/status`;
    }

    return `${baseUrl}/router/status`;
  }

  protected uploadBaseUrl(baseUrl: string): string {
    if (baseUrl.startsWith('wss://')) {
      return baseUrl.replace(/^wss:\/\//, 'https://');
    }

    if (baseUrl.startsWith('ws://')) {
      return baseUrl.replace(/^ws:\/\//, 'http://');
    }

    return baseUrl;
  }

  protected activeRouterBaseUrl(): string {
    return this.routerBaseUrls[this.activeConnectionIndex] || this.routerBaseUrls[0];
  }

  protected async fetchWithTimeout(url: string, timeoutMs: number): Promise<any> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      return await fetch(url, { signal: controller.signal });
    } finally {
      clearTimeout(timer);
    }
  }

  protected shouldRetryOnFailure(query: string): boolean {
    const normalized = this.normalizeQueryCommand(query);
    if (!normalized) {
      return false;
    }

    const head = normalized.split(/\s+/)[0];
    if (/^CACHE\s+(GET|KEYS)\s/.test(normalized)) return true;
    const retryableHeads = ['SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN', 'PRAGMA'];
    return retryableHeads.includes(head);
  }

  protected normalizeQueryCommand(query: string): string {
    let normalized = `${query}`.replace(/^\uFEFF/, '').trimStart();

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const blockMatch = normalized.match(/^\/\*[\s\S]*?\*\//);
      const lineMatch = normalized.match(/^--.*?(?:\r\n|\r|\n|$)/);
      const shellMatch = normalized.match(/^#.*?(?:\r\n|\r|\n|$)/);

      if (blockMatch) {
        normalized = normalized.slice(blockMatch[0].length).trimStart();
        // eslint-disable-next-line no-continue
        continue;
      }

      if (lineMatch) {
        normalized = normalized.slice(lineMatch[0].length).trimStart();
        // eslint-disable-next-line no-continue
        continue;
      }

      if (shellMatch) {
        normalized = normalized.slice(shellMatch[0].length).trimStart();
        // eslint-disable-next-line no-continue
        continue;
      }

      break;
    }

    return normalized.toUpperCase();
  }

  protected async withFailover<T>(
    action: (connection: WebSocketConnection) => Promise<T>,
    options: { retryable: boolean } = { retryable: true },
  ): Promise<T> {
    if (!this.connections.length) {
      throw new Error('cubejs-cubestore-driver: no Cubestore router connections configured');
    }

    const total = this.connections.length;
    let lastError: any;
    const leaderIndex = await this.detectLeaderIndex();
    await this.maybeResetOnLeaderShift(leaderIndex);
    if (leaderIndex !== null) {
      this.activeConnectionIndex = leaderIndex;
    }

    // A Kubernetes leader Service is intentionally represented by one host.
    // A stale WebSocket can still be connected to the deleted leader after the
    // Service EndpointSlice changes, so replay-safe queries get one fresh
    // connection attempt through the Service. Writes keep the old no-replay
    // behavior and never enter this recovery path.
    const maxAttempts = total === 1 ? 2 : total;
    for (let offset = 0; offset < maxAttempts; offset += 1) {
      const index = (this.activeConnectionIndex + offset) % total;
      const connection = this.connections[index];
      try {
        const result = await action(connection);
        this.activeConnectionIndex = index;
        return result;
      } catch (e: any) {
        lastError = e;
        if ((!options.retryable && e?.code !== 'MUTATION_NOT_DISPATCHED') || !(e instanceof ConnectionError) || offset + 1 >= maxAttempts) {
          throw e;
        }

        if (total === 1) {
          connection.close();
          await new Promise(resolve => setTimeout(resolve, Math.min(500, this.leaderProbeTimeoutMs)));
          this.activeConnectionIndex = 0;
          // eslint-disable-next-line no-continue
          continue;
        }

        const refreshedLeaderIndex = await this.detectLeaderIndex(true);
        if (refreshedLeaderIndex !== null) {
          await this.maybeResetOnLeaderShift(refreshedLeaderIndex);
          this.activeConnectionIndex = refreshedLeaderIndex;
        } else {
          await this.maybeResetOnLeaderShift((index + 1) % total);
          this.activeConnectionIndex = (index + 1) % total;
        }
        // eslint-disable-next-line no-continue
        continue;
      }
    }

    if (lastError) {
      throw lastError;
    }

    throw new Error('cubejs-cubestore-driver: failed to execute query on any router');
  }

  protected async detectLeaderIndex(force = false): Promise<number | null> {
    if (this.connections.length <= 1) {
      return 0;
    }

    const now = Date.now();
    if (
      !force &&
      this.leaderIndexCache !== null &&
      now < this.leaderProbeExpiresAt
    ) {
      return this.leaderIndexCache;
    }

    if (!force && this.leaderProbeInProgress) {
      return this.leaderProbeInProgress;
    }

    this.leaderProbeInProgress = this.detectLeaderIndexImpl();
    const index = await this.leaderProbeInProgress;
    this.leaderIndexCache = index;
    this.leaderProbeExpiresAt = now + (index === null ? Math.min(500, this.leaderProbeTtlMs) : this.leaderProbeTtlMs);
    this.leaderProbeInProgress = null;
    return index;
  }

  protected async detectLeaderIndexImpl(): Promise<number | null> {
    if (this.leaderProbeUseStatus) {
      const statusIndex = await this.detectLeaderIndexImplByStatus();
      if (statusIndex !== null) {
        return statusIndex;
      }
    }

    return this.detectLeaderIndexImplByQuery();
  }

  protected async detectLeaderIndexImplByStatus(): Promise<number | null> {
    const checks = this.routerStatusUrls.map(async (statusUrl, index) => {
      try {
        const response = await this.fetchWithTimeout(statusUrl, this.leaderProbeStatusTimeoutMs);

        if (!response.ok) {
          return null;
        }

        const payload = await response.json() as RouterStatusPayload;
        if (payload && payload.is_leader === true) {
          const roleState = payload.leaderState as RouterRoleStatePayload | undefined;
          let activeLeader: string | undefined;
          if (typeof payload.activeLeader === 'string') {
            activeLeader = payload.activeLeader;
          } else if (typeof roleState?.activeLeader === 'string') {
            activeLeader = roleState.activeLeader;
          }
          const payloadLeaderEpoch = payload.leaderEpoch ?? roleState?.leaderEpoch;
          const leaderEpoch = this.parseLeaderEpoch(payloadLeaderEpoch);
          const nodeName = typeof payload.node_name === 'string' ? payload.node_name : undefined;

          if (activeLeader && nodeName && activeLeader !== nodeName) {
            return null;
          }

          if (this.leaderEpochFence && leaderEpoch !== null) {
            if (this.leaderEpochCache !== null && leaderEpoch < this.leaderEpochCache) {
              return null;
            }
          }

          const rawTimestamp = (payload as RouterStatusPayload).timestamp_unix_secs;
          const updatedAt = typeof rawTimestamp === 'number' ? rawTimestamp : undefined;
          const now = Math.floor(Date.now() / 1000);

          if (
            this.leaderProbeStatusMaxAgeSecs <= 0
            || updatedAt === undefined
            || (now - updatedAt <= this.leaderProbeStatusMaxAgeSecs && now >= updatedAt)
          ) {
            if (this.leaderEpochFence && leaderEpoch !== null) {
              this.leaderEpochCache = Math.max(this.leaderEpochCache ?? leaderEpoch, leaderEpoch);
            }
            return index;
          }
        }
      } catch {
        return null;
      }
      return null;
    });

    const settled = await Promise.all(checks);
    const leaders = settled.filter((candidate) => candidate !== null);

    if (leaders.length !== 1) {
      return null;
    }

    return leaders[0];
  }

  protected async detectLeaderIndexImplByQuery(): Promise<number | null> {
    const checks = this.connections.map(async (connection, index) => {
      let timer: ReturnType<typeof setTimeout> | null = null;
      try {
        await Promise.race([
          connection.query('SELECT 1', [], {
            inlineTables: [],
            queryTracingObj: {},
            responseFormat: QueryResultFormat.Legacy,
            replaySafe: true,
          }),
          new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error('leader probe timeout')), this.leaderProbeTimeoutMs);
          }),
        ]);
        return index;
      } catch {
        return null;
      } finally {
        if (timer) {
          clearTimeout(timer);
        }
      }
    });

    const settled = await Promise.all(checks);
    return settled.find((candidate) => candidate !== null) ?? null;
  }

  public informationSchemaQuery() {
    return `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns as columns
      WHERE columns.table_schema NOT IN ('information_schema', 'system')`;
  }

  public createTableSqlWithOptions(tableName, columns, options: CreateTableOptions) {
    let sql = this.createTableSql(tableName, columns);
    const params: string[] = [];
    const withEntries: string[] = [];

    if (options.inputFormat) {
      withEntries.push(`input_format = '${options.inputFormat}'`);
    }
    if (options.delimiter) {
      withEntries.push(`delimiter = '${options.delimiter}'`);
    }
    if (options.disableQuoting) {
      withEntries.push('disable_quoting = true');
    }
    if (options.buildRangeEnd) {
      withEntries.push(`build_range_end = '${options.buildRangeEnd}'`);
    }
    if (options.sealAt) {
      withEntries.push(`seal_at = '${options.sealAt}'`);
    }
    if (options.selectStatement) {
      withEntries.push(`select_statement = ${escape(options.selectStatement)}`);
    }
    if (options.sourceTable) {
      withEntries.push(`source_table = ${escape(`CREATE TABLE ${options.sourceTable.tableName} (${options.sourceTable.types.map(t => `${t.name} ${this.fromGenericType(t.type)}`).join(', ')})`)}`);
    }
    if (options.streamOffset) {
      withEntries.push(`stream_offset = '${options.streamOffset}'`);
    }
    if (withEntries.length > 0) {
      sql = `${sql} WITH (${withEntries.join(', ')})`;
    }
    if (options.uniqueKey) {
      sql = `${sql} UNIQUE KEY (${options.uniqueKey})`;
    }
    if (options.aggregations) {
      sql = `${sql} ${options.aggregations}`;
    }
    if (options.indexes) {
      sql = `${sql} ${options.indexes}`;
    }
    if (options.files) {
      sql = `${sql} LOCATION ${options.files.map(() => '?').join(', ')}`;
      params.push(...options.files);
    }
    return sql;
  }

  public createTableWithOptions(tableName: string, columns: Column[], options: CreateTableOptions, queryTracingObj: any) {
    const sql = this.createTableSqlWithOptions(tableName, columns, options);
    const params: string[] = [];

    if (options.files) {
      params.push(...options.files);
    }

    const execute = async () => {
      if (queryTracingObj?.preAggregationBuildId && options.files?.every(file => !file.startsWith('stream://'))) {
        const record = await this.preAggregationBuilds.read(tableName);
        if (!record) throw new MutationUnknownError(`Missing durable build identity for ${tableName}`);
        const create = { sql, params };
        if (record.create && JSON.stringify(record.create) !== JSON.stringify(create)) {
          throw new MutationUnknownError(`Build manifest conflict for ${tableName}`);
        }
        await this.preAggregationBuilds.save({ ...record, phase: 'create', create });
        return this.executePreAggregationCreate({ ...record, phase: 'create', create }, queryTracingObj);
      }
      return this.query(sql, params, queryTracingObj);
    };
    return execute().catch(e => {
      e.message = `Error during create table: ${sql}: ${e.message}`;
      throw e;
    });
  }

  @AsyncDebounce()
  public async getTablesQuery(schemaName) {
    const tables = await this.query(
      `SELECT table_name, build_range_end FROM information_schema.tables WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
    return this.readyPreAggregationTables(schemaName, tables);
  }

  @AsyncDebounce()
  public async getPrefixTablesQuery(schemaName, tablePrefixes) {
    const prefixWhere = tablePrefixes.map(_ => 'table_name LIKE CONCAT(?, \'%\')').join(' OR ');
    const tables = await this.query(
      `SELECT table_name, build_range_end FROM information_schema.tables WHERE table_schema = ${this.param(0)} AND (${prefixWhere})`,
      [schemaName].concat(tablePrefixes)
    );
    return this.readyPreAggregationTables(schemaName, tables);
  }

  private async readyPreAggregationTables(schema: string, tables: any[]): Promise<any[]> {
    const ready: any[] = [];
    for (const table of tables) {
      const name = `${schema}.${table.table_name}`;
      const status = await this.getPreAggregationBuildStatus(name);
      if (status === null || status.state === 'ready') {
        const build = await this.preAggregationBuilds.read(name);
        if (build && (!build.create || !status || ['failed', 'retired'].includes(build.phase))) continue;
        if (build?.create?.params.length && JSON.stringify(status?.locations) !== JSON.stringify(build.create.params)) continue;
        if (build?.tableId != null && String(status?.tableId) !== String(build.tableId)) continue;
        if (build && build.phase !== 'ready') {
          // A retried /load can discover an imported table before its original
          // CREATE call returns. Publish only after the same immutable build's
          // authoritative terminal state is durable, not just its physical table.
          await this.preAggregationBuilds.save({ ...build, phase: 'ready', tableId: status!.tableId });
          const completed = await this.preAggregationBuilds.read(name);
          if (completed?.phase !== 'ready' || String(completed.tableId) !== String(status!.tableId)) {
            throw new MutationUnknownError(`Ready build publication was not confirmed: ${name}`);
          }
        }
        ready.push(table);
      }
    }
    return ready;
  }

  public async tableColumnTypes(table: string): Promise<TableStructure> {
    const [schema, name] = table.split('.');

    const columns = await this.query<TableColumnQueryResult>(
      `SELECT column_name as ${this.quoteIdentifier('column_name')},
             table_name as ${this.quoteIdentifier('table_name')},
             table_schema as ${this.quoteIdentifier('table_schema')},
             data_type as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns
      WHERE table_name = ${this.param(0)} AND table_schema = ${this.param(1)}`,
      [name, schema]
    );

    return columns.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type) }));
  }

  public quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  public fromGenericType(columnType: string): string {
    return GenericTypeToCubeStore[columnType] || super.fromGenericType(columnType);
  }

  public toColumnValue(value: any, genericType: any) {
    if (genericType === 'timestamp' && typeof value === 'string') {
      return value?.replace('Z', '');
    }
    if (genericType === 'boolean' && typeof value === 'string') {
      if (value.toLowerCase() === 'true') {
        return true;
      }
      if (value.toLowerCase() === 'false') {
        return false;
      }
    }
    return super.toColumnValue(value, genericType);
  }

  public async uploadTableWithIndexes(table: string, columns: Column[], tableData: any, indexesSql: IndexesSQL, uniqueKeyColumns: string[] | null, queryTracingObj?: any, externalOptions?: ExternalCreateTableOptions) {
    const createTableIndexes = externalOptions?.createTableIndexes;
    const aggregationsColumns = externalOptions?.aggregationsColumns;

    const indexes = createTableIndexes?.length ? createTableIndexes.map(this.createIndexString).join(' ') : '';

    let hasAggregatingIndexes = false;
    if (createTableIndexes?.length) {
      hasAggregatingIndexes = createTableIndexes.some((index) => index.type === 'aggregate');
    }

    const aggregations = hasAggregatingIndexes && aggregationsColumns?.length ? ` AGGREGATIONS (${aggregationsColumns.join(', ')})` : '';

    if (tableData.rows && queryTracingObj?.preAggregationBuildId) {
      // File-import CREATE publishes only after the complete import. An empty
      // CREATE followed by INSERT batches publishes a partially loaded table.
      await this.importStream(columns, { ...tableData, rowStream: Readable.from(tableData.rows) }, table, indexes, aggregations, queryTracingObj);
    } else if (tableData.rowStream) {
      await this.importStream(columns, tableData, table, indexes, aggregations, queryTracingObj);
    } else if (tableData.csvFile) {
      await this.importCsvFile(tableData, table, columns, indexes, aggregations, queryTracingObj);
    } else if (tableData.streamingSource) {
      await this.importStreamingSource(columns, tableData, table, indexes, uniqueKeyColumns, queryTracingObj, externalOptions?.sealAt);
    } else if (tableData.rows) {
      await this.importRows(table, columns, indexes, aggregations, tableData, queryTracingObj);
    } else {
      throw new Error(`Unsupported table data passed to ${this.constructor}`);
    }
  }

  private createIndexString(index: CreateTableIndex) {
    const prefix = {
      regular: '',
      aggregate: 'AGGREGATE '
    }[index.type] || '';
    return `${prefix}INDEX ${index.indexName} (${index.columns.join(',')})`;
  }

  private async importRows(table: string, columns: Column[], indexesSql: any, aggregations: any, tableData: DownloadTableMemoryData, queryTracingObj?: any) {
    if (!columns || columns.length === 0) {
      throw new Error('Unable to import (as rows) in Cube Store: empty columns. Most probably, introspection has failed.');
    }

    await this.createTableWithOptions(table, columns, { indexes: indexesSql, aggregations, buildRangeEnd: queryTracingObj?.buildRangeEnd }, queryTracingObj);
    try {
      const batchSize = 2000; // TODO make dynamic?
      for (let j = 0; j < Math.ceil(tableData.rows.length / batchSize); j++) {
        const currentBatchSize = Math.min(tableData.rows.length - j * batchSize, batchSize);
        const indexArray = Array.from({ length: currentBatchSize }, (v, i) => i);
        const valueParamPlaceholders =
          indexArray.map(i => `(${columns.map((c, paramIndex) => this.param(paramIndex + i * columns.length)).join(', ')})`).join(', ');
        const params = indexArray.map(i => columns
          .map(c => this.toColumnValue(tableData.rows[i + j * batchSize][c.name], c.type)))
          .reduce((a, b) => a.concat(b), []);

        await this.query(
          `INSERT INTO ${table}
        (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
        VALUES ${valueParamPlaceholders}`,
          params,
          { ...queryTracingObj, mutationId: `${table}:insert:${j}:${this.createIdempotencyFingerprintWithCanonicalQuery('', params)}` }
        );
      }
    } catch (e: any) {
      if (!(e instanceof ConnectionError) && e?.code !== 'MUTATION_UNKNOWN') await this.dropTable(table);
      throw e;
    }
  }

  private async importCsvFile(tableData: DownloadTableCSVData, table: string, columns: Column[], indexes: any, aggregations: any, queryTracingObj?: any) {
    if (!columns || columns.length === 0) {
      throw new Error('Unable to import (as csv) in Cube Store: empty columns. Most probably, introspection has failed.');
    }

    const files = Array.isArray(tableData.csvFile) ? tableData.csvFile : [tableData.csvFile];
    const options: CreateTableOptions = {
      buildRangeEnd: queryTracingObj?.buildRangeEnd,
      indexes,
      aggregations
    };
    if (files.length > 0) {
      options.inputFormat = tableData.csvNoHeader ? 'csv_no_header' : 'csv';
      if (tableData.csvDelimiter) {
        options.delimiter = tableData.csvDelimiter;
      }
      if (tableData.csvDisableQuoting) {
        options.disableQuoting = tableData.csvDisableQuoting;
      }
      options.files = files;
    }

    return this.createTableWithOptions(table, columns, options, queryTracingObj);
  }

  private async importStream(columns: Column[], tableData: StreamTableData, table: string, indexes: string, aggregations: string, queryTracingObj?: any) {
    if (!columns || columns.length === 0) {
      throw new Error('Unable to import (as stream) in Cube Store: empty columns. Most probably, introspection has failed.');
    }

    const tempFiles: string[] = [];
    const pipelinePromises: Promise<any>[] = [];
    const fileWriters: any[] = [];
    let completed = false;
    try {
      let currentFileStream: { stream: NodeJS.WritableStream, tempFile: string } | null = null;

      const options: CreateTableOptions = {
        buildRangeEnd: queryTracingObj?.buildRangeEnd,
        indexes,
        aggregations
      };

      const getFileStream = () => {
        if (!currentFileStream) {
          const writer = csvWriter({ headers: columns.map(c => c.name) });
          fileWriters.push(writer);
          const tempFile = tempy.file();
          tempFiles.push(tempFile);
          const gzipStream = createGzip();
          pipelinePromises.push(new Promise((resolve, reject) => {
            pipeline(writer, gzipStream, createWriteStream(tempFile), (err) => {
              if (err) {
                reject(err);
                return;
              }
              resolve(null);
            });
            currentFileStream = { stream: writer, tempFile };
          }));
          // Keep rejection observable below without an unhandled rejection while
          // the source pipeline is still producing other batches.
          pipelinePromises[pipelinePromises.length - 1].catch(() => undefined);
        }
        if (!currentFileStream) {
          throw new Error('Stream init error');
        }
        return currentFileStream;
      };

      let rowCount = 0;
      let totalRowCount = 0;

      const endStream = (chunk, encoding, callback) => {
        const { stream } = getFileStream();
        currentFileStream = null;
        rowCount = 0;
        if (chunk) {
          stream.end(chunk, encoding, callback);
        } else {
          if (totalRowCount === 0) {
            // csv-write-stream emits its header on the first data row only.
            // An empty rollup still needs a valid CSV file and LOCATION import.
            const header = columns.map(({ name }) => /[",\r\n]/.test(name) ? `"${name.replace(/"/g, '""')}"` : name).join(',');
            (stream as any).push(`${header}\n`);
          }
          stream.end(callback);
        }
      };

      const { batchingRowSplitCount } = this.config;

      const outputStream = new Writable({
        write(chunk, encoding, callback) {
          rowCount++;
          totalRowCount++;
          if (rowCount >= batchingRowSplitCount) {
            endStream(chunk, encoding, callback);
          } else {
            getFileStream().stream.write(chunk, encoding, callback);
          }
        },
        final(callback: (error?: (Error | null)) => void) {
          if (!currentFileStream && totalRowCount > 0) {
            callback();
            return;
          }
          endStream(null, null, callback);
        },
        objectMode: true
      });

      await new Promise(
        (resolve, reject) => pipeline(
          tableData.rowStream, outputStream, (err) => (err ? reject(err) : resolve(null))
        )
      );

      await Promise.all(pipelinePromises);
      const uploads: BuildUpload[] = [];
      for (const path of tempFiles) {
        const hash = createHash('sha256');
        let size = 0;
        for await (const chunk of createReadStream(path)) {
          hash.update(chunk);
          size += chunk.length;
        }
        const sha256 = hash.digest('hex');
        uploads.push({ name: `${sha256}.csv.gz`, sha256, size, path });
      }
      if (queryTracingObj?.preAggregationBuildId) {
        const record = await this.preAggregationBuilds.read(table);
        if (!record) throw new MutationUnknownError(`Missing durable build identity for ${table}`);
        const manifestHash = createHash('sha256').update(JSON.stringify(uploads.map(({ name, sha256, size }) => ({ name, sha256, size })))).digest('hex');
        if (record.manifestHash && record.manifestHash !== manifestHash) {
          const status = await this.getPreAggregationBuildStatus(table);
          if (status?.state !== 'absent' || record.tableId != null) throw new MutationUnknownError(`Regenerated input changed but target outcome is unresolved: ${table}`);
          const error = Object.assign(new Error(`Regenerated input differs from immutable build manifest; a new build attempt is required: ${table}`), { code: 'PRE_AGG_REBUILD_REQUIRED' });
          await this.preAggregationBuilds.save({ ...record, phase: 'failed', error: error.message });
          throw error;
        }
        // Persist the complete, ordered manifest before dispatching any upload.
        options.files = uploads.map(upload => `temp://${upload.name}`);
        await this.preAggregationBuilds.save({ ...record, phase: 'uploading', uploads, manifestHash,
          create: { sql: this.createTableSqlWithOptions(table, columns, options), params: options.files } });
      }
      const files: string[] = [];
      for (const upload of uploads) files.push(await this.uploadTempFile(upload, !!queryTracingObj?.preAggregationBuildId));
      if (queryTracingObj?.preAggregationBuildId) {
        const record = await this.preAggregationBuilds.read(table);
        if (!record) throw new MutationUnknownError(`Missing durable build identity for ${table}`);
        await this.preAggregationBuilds.save({ ...record, phase: 'uploaded' });
      }
      if (files.length > 0) {
        options.files = files.map(fileName => `temp://${fileName}`);
      }

      const result = await this.createTableWithOptions(table, columns, options, queryTracingObj);
      completed = true;
      return result;
    } catch (error: any) {
      fileWriters.forEach(writer => writer.destroy());
      error.tempFiles = tempFiles;
      throw error;
    } finally {
      // No unlink while a compressor still owns the file, and no loss of the
      // only resumable source when upload/CREATE has an unknown outcome.
      await Promise.allSettled(pipelinePromises);
      if (completed) await Promise.all(tempFiles.map(tempFile => unlink(tempFile)));
    }
  }

  protected async routerRecoveryJson(path: string): Promise<any | null> {
    let lastError: any;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const leader = await this.detectLeaderIndex(attempt > 0);
        if (leader !== null) this.activeConnectionIndex = leader;
        const res = await fetch(`${this.uploadBaseUrl(this.activeRouterBaseUrl())}${path}`, { timeout: this.leaderProbeTimeoutMs, headers: this.recoveryHeaders() });
        if (res.status === 404 || res.status === 405) return null; // old router, no capability
        if (!res.ok) throw new Error(`Recovery metadata unavailable: HTTP ${res.status}`);
        return await res.json();
      } catch (error) {
        lastError = error;
        if (attempt < 2) await this.sleep(100 * (attempt + 1));
      }
    }
    throw lastError;
  }

  public async getPreAggregationBuildStatus(table: string): Promise<{ state: string; tableId?: string | number; error?: string; locations?: string[] } | null> {
    const status = await this.routerRecoveryJson(`/router/build-status?table=${encodeURIComponent(table)}`);
    if (status === null) return null;
    if (!['absent', 'building', 'ready', 'failed', 'unknown'].includes(status.state) ||
        (['building', 'ready', 'failed'].includes(status.state) && status.tableId == null)) {
      throw new Error('Invalid authoritative pre-aggregation build status');
    }
    return status;
  }

  public async resolvePreAggregationBuild(key: string, versionEntry: any, protectedTables: string[], force = false) {
    const timestamp = versionEntry.naming_version === 2 ? Math.floor(versionEntry.last_updated_at / 1000).toString(32) : versionEntry.last_updated_at;
    const buildId = `${versionEntry.table_name}_${versionEntry.content_version}_${versionEntry.structure_version}_${timestamp}`;
    const initialStatus = await this.getPreAggregationBuildStatus(buildId);
    if (initialStatus === null) return null;
    if (initialStatus.state !== 'absent' && !(await this.preAggregationBuilds.read(buildId))) {
      throw new MutationUnknownError(`Refusing to adopt existing target without a durable build identity: ${buildId}`);
    }
    const candidate: PreAggregationBuild = { buildId, versionEntry, protectedTables, phase: 'selected' };
    const selected = await this.preAggregationBuilds.resolve(key, candidate, force);
    if (selected.phase === 'ready' && (await this.getPreAggregationBuildStatus(selected.buildId))?.state === 'absent') {
      // A terminal, previously ready table may have been legitimately retired.
      // Allocate a NEW attempt, never resurrect its old table identity.
      await this.preAggregationBuilds.save({ ...selected, phase: 'retired', error: 'Previously completed target was retired' });
      return this.preAggregationBuilds.resolve(key, candidate, force);
    }
    return selected;
  }

  public async getProtectedPreAggregationTables(): Promise<string[]> {
    return this.preAggregationBuilds.protectedTables();
  }

  public async resumePreAggregationBuild(table: string): Promise<boolean> {
    const record = await this.preAggregationBuilds.read(table);
    // Only a durable, selected build can enter the initial source strategy.
    // Missing recovery evidence is not permission to start a new mutation.
    if (!record) throw new MutationUnknownError(`Missing durable build identity for ${table}`);
    if (record.phase === 'selected') return false;
    if (record.phase === 'failed' || record.phase === 'retired') throw new Error(record.error || `Pre-aggregation build failed: ${table}`);
    if (!record.create) throw new MutationUnknownError(`Missing CREATE manifest for ${table}`);
    if (record.phase === 'uploading') {
      try {
        // Each helper checks authoritative remote status before opening a local
        // path. Another refresh worker needs no local files if all are remote.
        for (const upload of record.uploads || []) await this.uploadTempFile(upload, true);
      } catch (error: any) {
        if (error.code === 'PRE_AGG_UPLOAD_SOURCE_MISSING') {
          throw new MutationUnknownError(`Upload source unavailable for durable build ${table}; preserve its manifest for recovery`);
        }
        throw error;
      }
      await this.preAggregationBuilds.save({ ...record, phase: 'uploaded' });
    }
    if (record.phase === 'uploaded') {
      // Do not infer durability from a former acknowledgement alone.
      for (const upload of record.uploads || []) {
        try {
          await this.uploadTempFile(upload, true);
        } catch (error: any) {
          if (error.code === 'PRE_AGG_UPLOAD_SOURCE_MISSING') {
            throw new MutationUnknownError(`Upload source unavailable for durable build ${table}; preserve its manifest for recovery`);
          }
          throw error;
        }
      }
    }
    await this.preAggregationBuilds.save({ ...record, phase: 'create' });
    await this.executePreAggregationCreate({ ...record, phase: 'create' }, { preAggregationBuildId: table });
    return true;
  }

  private async executePreAggregationCreate(record: PreAggregationBuild, tracing: any): Promise<any[]> {
    const current = await this.preAggregationBuilds.read(record.buildId);
    if (!current) throw new MutationUnknownError(`Build identity disappeared: ${record.buildId}`);
    if (current.phase === 'failed' || current.phase === 'retired') throw new Error(current.error || `Build is terminal: ${record.buildId}`);
    record = current;
    if (!record.create) throw new MutationUnknownError(`Missing CREATE manifest for ${record.buildId}`);
    let lastError: any;
    const deadline = Date.now() + Math.max(1, this.preAggregationReconcileTimeoutMs);
    const maxPolls = Math.ceil(Math.max(1, this.preAggregationReconcileTimeoutMs) / 1000) + 1;
    for (let attempt = 0; attempt < maxPolls && Date.now() <= deadline; attempt++) {
      let status;
      try {
        status = await this.getPreAggregationBuildStatus(record.buildId);
      } catch (error: any) {
        lastError = error;
        await this.sleep(Math.min(2000, Math.max(0, deadline - Date.now())));
        continue;
      }
      if (!status || status.state === 'unknown') throw new MutationUnknownError(`Build outcome unknown for ${record.buildId}`);
      if (record.tableId != null && status.tableId != null && String(record.tableId) !== String(status.tableId)) {
        throw new MutationUnknownError(`Table identity changed for ${record.buildId}`);
      }
      if (status.state === 'ready') {
        if (record.create!.params.length && JSON.stringify(status.locations) !== JSON.stringify(record.create!.params)) {
          throw new MutationUnknownError(`Authoritative CREATE locations do not match immutable manifest: ${record.buildId}`);
        }
        await this.preAggregationBuilds.save({ ...record, phase: 'ready', tableId: status.tableId });
        return [];
      }
      if (status.state === 'failed') {
        await this.preAggregationBuilds.save({ ...record, phase: 'failed', tableId: status.tableId, error: status.error });
        throw new Error(status.error || `Pre-aggregation import failed: ${record.buildId}`);
      }
      if (status.tableId != null) {
        record = { ...record, tableId: status.tableId };
        await this.preAggregationBuilds.save(record);
      }
      if (status.state === 'absent') {
        if (record.tableId != null) throw new MutationUnknownError(`Previously observed table disappeared: ${record.buildId}`);
        // Only the immutable, build-specific CREATE can be retried after an
        // authoritative no-effect observation. Never replay an arbitrary INSERT.
        try {
          await this.query(record.create!.sql, record.create!.params, {
            ...tracing, mutationId: `${record.buildId}:create`, retryable: false
          });
        } catch (error) {
          lastError = error;
        }
      }
      await this.sleep(Math.min(2000, 250 * (attempt + 1), Math.max(0, deadline - Date.now())));
    }
    throw new MutationUnknownError(`Pre-aggregation ${record.buildId} still requires reconciliation${lastError ? `: ${lastError.message}` : ''}`);
  }

  protected async uploadTempFile(upload: BuildUpload, requireRecovery: boolean): Promise<string> {
    let lastError: any;
    for (let attempt = 0; attempt < 3; attempt++) {
      const query = `name=${encodeURIComponent(upload.name)}&sha256=${upload.sha256}`;
      let status;
      try {
        status = await this.routerRecoveryJson(`/upload-temp-file-status?${query}`);
      } catch (error) {
        lastError = error;
        continue;
      }
      if (status?.state === 'uploaded') {
        if (status.sha256 !== upload.sha256 || status.size !== upload.size) throw new MutationUnknownError('Uploaded payload checksum/size mismatch');
        return upload.name;
      }
      if (status !== null && status?.state !== 'missing') throw new MutationUnknownError('Invalid upload status');
      if (status === null && (requireRecovery || attempt > 0)) throw new MutationUnknownError('Upload recovery is not supported by this router');
      try {
        await access(upload.path);
      } catch (error: any) {
        if (error.code === 'ENOENT') throw Object.assign(new Error(`Regenerate source for missing upload ${upload.name}`), { code: 'PRE_AGG_UPLOAD_SOURCE_MISSING' });
        throw error;
      }
      const body = createReadStream(upload.path);
      try {
        const res = await fetch(`${this.uploadBaseUrl(this.activeRouterBaseUrl())}/upload-temp-file?${status === null ? `name=${encodeURIComponent(upload.name)}` : query}`, {
          method: 'POST', body, timeout: 60000, headers: this.recoveryHeaders(),
        });
        if (!res.ok) throw new Error(`Upload failed: HTTP ${res.status}`);
        // Consume the response within the timeout before closing the source.
        await res.text();
        if (status === null) return upload.name;
      } catch (error) {
        lastError = error;
        if (status === null) throw new MutationUnknownError(`Legacy upload outcome unknown: ${upload.name}`);
      } finally {
        body.destroy();
      }
    }
    // The final POST may have committed even though its response was lost.
    const final = await this.routerRecoveryJson(`/upload-temp-file-status?name=${encodeURIComponent(upload.name)}&sha256=${upload.sha256}`).catch(() => null);
    if (final?.state === 'uploaded' && final.sha256 === upload.sha256 && final.size === upload.size) return upload.name;
    throw new MutationUnknownError(`Upload outcome unresolved for ${upload.name}${lastError ? `: ${lastError.message}` : ''}`);
  }

  private recoveryHeaders(): Record<string, string> {
    return this.config.user ? { Authorization: `Basic ${Buffer.from(`${this.config.user}:${this.config.password || ''}`).toString('base64')}` } : {};
  }

  private async importStreamingSource(columns: Column[], tableData: StreamingSourceTableData, table: string, indexes: string, uniqueKeyColumns: string[] | null, queryTracingObj?: any, sealAt?: string) {
    if (!uniqueKeyColumns) {
      throw new Error('Older version of orchestrator is being used with newer version of Cube Store driver. Please upgrade cube.js.');
    }
    await this.query(
      `CREATE SOURCE OR UPDATE ${this.quoteIdentifier(tableData.streamingSource.name)} as ? VALUES (${Object.keys(tableData.streamingSource.credentials).map(k => `${k} = ?`)})`,
      [tableData.streamingSource.type]
        .concat(
          Object.keys(tableData.streamingSource.credentials).map(k => tableData.streamingSource.credentials[k])
        ),
      queryTracingObj
    );

    let locations = [`stream://${tableData.streamingSource.name}/${tableData.streamingTable}`];

    if (tableData.partitions) {
      locations = [];
      for (let i = 0; i < tableData.partitions; i++) {
        locations.push(`stream://${tableData.streamingSource.name}/${tableData.streamingTable}/${i}`);
      }
    }

    const options: CreateTableOptions = {
      buildRangeEnd: queryTracingObj?.buildRangeEnd,
      uniqueKey: uniqueKeyColumns.join(','),
      indexes,
      files: locations,
      selectStatement: tableData.selectStatement,
      sourceTable: tableData.sourceTable,
      streamOffset: tableData.streamOffset,
      sealAt
    };
    return this.createTableWithOptions(table, columns, options, queryTracingObj);
  }

  public capabilities(): ExternalDriverCompatibilities {
    return {
      csvImport: true,
      streamImport: true,
    };
  }
}
