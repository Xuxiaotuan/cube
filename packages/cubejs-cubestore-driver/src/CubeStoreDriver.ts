import { createHash } from 'crypto';
import { pipeline, Writable } from 'stream';
import { createGzip } from 'zlib';
import { createReadStream, createWriteStream } from 'fs';
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
    if (!this.strictWriteRetryWithoutMutationId) {
      return requestedRetryable;
    }

    if (isMutating && !mutationId) {
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
    if (serialized.length <= 8192) {
      return `inline:${serialized}`;
    }
    return `omitted:${createHash('sha256').update(serialized).digest('hex')}`;
  }

  protected resultFromRef<R>(resultRef?: string): R[] {
    if (resultRef?.startsWith('inline:')) {
      try {
        return JSON.parse(resultRef.slice('inline:'.length)) as R[];
      } catch {
        throw new Error('mutation idempotency result reference is invalid');
      }
    }
    return [] as R[];
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
      } catch (e) {
        lastError = e;
        if (!options.retryable || !(e instanceof ConnectionError) || offset + 1 >= maxAttempts) {
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

    return this.query(sql, params, queryTracingObj).catch(e => {
      e.message = `Error during create table: ${sql}: ${e.message}`;
      throw e;
    });
  }

  @AsyncDebounce()
  public async getTablesQuery(schemaName) {
    return this.query(
      `SELECT table_name, build_range_end FROM information_schema.tables WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
  }

  @AsyncDebounce()
  public async getPrefixTablesQuery(schemaName, tablePrefixes) {
    const prefixWhere = tablePrefixes.map(_ => 'table_name LIKE CONCAT(?, \'%\')').join(' OR ');
    return this.query(
      `SELECT table_name, build_range_end FROM information_schema.tables WHERE table_schema = ${this.param(0)} AND (${prefixWhere})`,
      [schemaName].concat(tablePrefixes)
    );
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

    if (tableData.rowStream) {
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
          queryTracingObj
        );
      }
    } catch (e) {
      await this.dropTable(table);
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
    try {
      const pipelinePromises: Promise<any>[] = [];
      const filePromises: Promise<string>[] = [];
      let currentFileStream: { stream: NodeJS.WritableStream, tempFile: string } | null = null;

      const options: CreateTableOptions = {
        buildRangeEnd: queryTracingObj?.buildRangeEnd,
        indexes,
        aggregations
      };

      const baseUrl = this.uploadBaseUrl(this.activeRouterBaseUrl());
      let fileCounter = 0;

      this.createTableSql(table, columns);
      // eslint-disable-next-line no-unused-vars
      const createTableSqlWithoutLocation = this.createTableSqlWithOptions(table, columns, options);

      const getFileStream = () => {
        if (!currentFileStream) {
          const writer = csvWriter({ headers: columns.map(c => c.name) });
          const tempFile = tempy.file();
          tempFiles.push(tempFile);
          const gzipStream = createGzip();
          pipelinePromises.push(new Promise((resolve, reject) => {
            pipeline(writer, gzipStream, createWriteStream(tempFile), (err) => {
              if (err) {
                reject(err);
              }

              const fileName = `${table}-${fileCounter++}.csv.gz`;
              filePromises.push(fetch(`${baseUrl.replace(/^ws/, 'http')}/upload-temp-file?name=${fileName}`, {
                method: 'POST',
                body: createReadStream(tempFile),
              }).then(async res => {
                if (res.status !== 200) {
                  const error = await res.json();
                  throw new Error(`Error during upload of ${fileName} create table: ${createTableSqlWithoutLocation}: ${error.error}`);
                }
                return fileName;
              }));

              resolve(null);
            });
            currentFileStream = { stream: writer, tempFile };
          }));
        }
        if (!currentFileStream) {
          throw new Error('Stream init error');
        }
        return currentFileStream;
      };

      let rowCount = 0;

      const endStream = (chunk, encoding, callback) => {
        const { stream } = getFileStream();
        currentFileStream = null;
        rowCount = 0;
        if (chunk) {
          stream.end(chunk, encoding, callback);
        } else {
          stream.end(callback);
        }
      };

      const { batchingRowSplitCount } = this.config;

      const outputStream = new Writable({
        write(chunk, encoding, callback) {
          rowCount++;
          if (rowCount >= batchingRowSplitCount) {
            endStream(chunk, encoding, callback);
          } else {
            getFileStream().stream.write(chunk, encoding, callback);
          }
        },
        final(callback: (error?: (Error | null)) => void) {
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

      const files = await Promise.all(filePromises);
      if (files.length > 0) {
        options.files = files.map(fileName => `temp://${fileName}`);
      }

      return this.createTableWithOptions(table, columns, options, queryTracingObj);
    } finally {
      await Promise.all(tempFiles.map(tempFile => unlink(tempFile)));
    }
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
