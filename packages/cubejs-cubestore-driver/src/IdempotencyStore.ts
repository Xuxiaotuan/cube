import { randomUUID } from 'crypto';

export type SerializedError = {
  name: string;
  message: string;
  code?: string;
  stack?: string;
};

export type MutationLease = {
  key: string;
  ownerToken: string;
  fingerprint: string;
  expiresAt: number;
};

export type ExistingResult = {
  key: string;
  fingerprint: string;
  status: 'PENDING' | 'COMPLETED' | 'FAILED' | 'UNKNOWN';
  expiresAt: number;
  resultRef?: string;
  error?: SerializedError;
};

export interface IdempotencyStore {
  acquire(mutationId: string, fingerprint: string): Promise<MutationLease | ExistingResult>;
  renew(lease: MutationLease): Promise<MutationLease>;
  complete(lease: MutationLease, resultRef: string): Promise<void>;
  fail(lease: MutationLease, error: SerializedError): Promise<void>;
}

type RedisClient = {
  get(key: string): Promise<string | null>;
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<string | number | null>;
};

type IdempotencyStoreOptions = {
  keyPrefix: string;
  pendingTtlSeconds: number;
  completedTtlSeconds: number;
  failedTtlSeconds: number;
};

type StoredRecord = {
  status: ExistingResult['status'];
  fingerprint: string;
  ownerToken?: string;
  startedAt?: number;
  finishedAt?: number;
  expiresAt: number;
  resultRef?: string;
  error?: SerializedError;
};

export class IdempotencyOwnershipLostError extends Error {
  public constructor(message = 'mutation idempotency lease ownership was lost') {
    super(message);
    this.name = 'IdempotencyOwnershipLostError';
  }
}

export class RedisIdempotencyStore implements IdempotencyStore {
  public constructor(
    public readonly client: RedisClient,
    private readonly options: IdempotencyStoreOptions,
  ) {}

  private key(mutationId: string): string {
    return `${this.options.keyPrefix}:${mutationId}`;
  }

  private pendingTtlMs(): number {
    return this.options.pendingTtlSeconds * 1000;
  }

  private async evalJson(script: string, keys: string[], args: string[]): Promise<any> {
    const raw = await this.client.eval(script, { keys, arguments: args });
    if (typeof raw !== 'string') {
      return raw;
    }
    return JSON.parse(raw);
  }

  private existing(key: string, record: StoredRecord): ExistingResult {
    return {
      key,
      fingerprint: record.fingerprint,
      status: record.status,
      expiresAt: record.expiresAt,
      resultRef: record.resultRef,
      error: record.error,
    };
  }

  public async acquire(mutationId: string, fingerprint: string): Promise<MutationLease | ExistingResult> {
    const key = this.key(mutationId);
    const ownerToken = randomUUID();
    const now = Date.now();
    const response = await this.evalJson(`
      -- IDEMPOTENCY_ACQUIRE
      local current = redis.call('GET', KEYS[1])
      if current then
        local ok, record = pcall(cjson.decode, current)
        if not ok then return cjson.encode({ kind = 'INVALID' }) end
        if record.fingerprint ~= ARGV[1] then
          return cjson.encode({ kind = 'CONFLICT', fingerprint = record.fingerprint })
        end
        return cjson.encode({ kind = 'EXISTING', record = record })
      end
      local ttl = tonumber(ARGV[4])
      local record = {
        status = 'PENDING', fingerprint = ARGV[1], ownerToken = ARGV[2],
        startedAt = tonumber(ARGV[3]), expiresAt = tonumber(ARGV[3]) + ttl
      }
      redis.call('SET', KEYS[1], cjson.encode(record), 'PX', ttl)
      return cjson.encode({ kind = 'LEASE', record = record })
    `, [key], [fingerprint, ownerToken, `${now}`, `${this.pendingTtlMs()}`]);

    if (response?.kind === 'LEASE') {
      return {
        key,
        ownerToken,
        fingerprint,
        expiresAt: response.record.expiresAt,
      };
    }
    if (response?.kind === 'CONFLICT') {
      throw new Error(`mutationId conflict: same mutationId=${mutationId} but different fingerprint`);
    }
    if (response?.kind === 'EXISTING') {
      return this.existing(key, response.record as StoredRecord);
    }
    throw new Error(`mutationId ${mutationId} idempotency state is invalid`);
  }

  public async renew(lease: MutationLease): Promise<MutationLease> {
    const now = Date.now();
    const response = await this.evalJson(`
      -- IDEMPOTENCY_RENEW
      local current = redis.call('GET', KEYS[1])
      if not current then return 0 end
      local ok, record = pcall(cjson.decode, current)
      if not ok or record.status ~= 'PENDING' or record.ownerToken ~= ARGV[1] then return 0 end
      local ttl = tonumber(ARGV[3])
      record.expiresAt = tonumber(ARGV[2]) + ttl
      redis.call('SET', KEYS[1], cjson.encode(record), 'PX', ttl, 'XX')
      return cjson.encode(record)
    `, [lease.key], [lease.ownerToken, `${now}`, `${this.pendingTtlMs()}`]);

    if (!response || typeof response !== 'object') {
      throw new IdempotencyOwnershipLostError();
    }
    return { ...lease, expiresAt: response.expiresAt };
  }

  public async complete(lease: MutationLease, resultRef: string): Promise<void> {
    const now = Date.now();
    const completedTtlMs = this.options.completedTtlSeconds * 1000;
    const completed = await this.client.eval(`
      -- IDEMPOTENCY_COMPLETE
      local current = redis.call('GET', KEYS[1])
      if not current then return 0 end
      local ok, record = pcall(cjson.decode, current)
      if not ok or record.status ~= 'PENDING' or record.ownerToken ~= ARGV[1] then return 0 end
      local ttl = tonumber(ARGV[4])
      local result = {
        status = 'COMPLETED', fingerprint = record.fingerprint, startedAt = record.startedAt,
        finishedAt = tonumber(ARGV[2]), expiresAt = tonumber(ARGV[2]) + ttl, resultRef = ARGV[3]
      }
      redis.call('SET', KEYS[1], cjson.encode(result), 'PX', ttl, 'XX')
      return 1
    `, { keys: [lease.key], arguments: [lease.ownerToken, `${now}`, resultRef, `${completedTtlMs}`] });
    if (completed !== 1 && completed !== '1') {
      throw new IdempotencyOwnershipLostError();
    }
  }

  public async fail(lease: MutationLease, error: SerializedError): Promise<void> {
    const now = Date.now();
    const unknown = error.code === 'MUTATION_UNKNOWN';
    const ttlMs = (unknown ? this.options.pendingTtlSeconds : this.options.failedTtlSeconds) * 1000;
    const failed = await this.client.eval(`
      -- IDEMPOTENCY_FAIL
      local current = redis.call('GET', KEYS[1])
      if not current then return 0 end
      local ok, record = pcall(cjson.decode, current)
      if not ok or record.status ~= 'PENDING' or record.ownerToken ~= ARGV[1] then return 0 end
      local ttl = tonumber(ARGV[5])
      local result = {
        status = ARGV[2], fingerprint = record.fingerprint, startedAt = record.startedAt,
        finishedAt = tonumber(ARGV[3]), expiresAt = tonumber(ARGV[3]) + ttl,
        error = cjson.decode(ARGV[4])
      }
      redis.call('SET', KEYS[1], cjson.encode(result), 'PX', ttl, 'XX')
      return 1
    `, {
      keys: [lease.key],
      arguments: [lease.ownerToken, unknown ? 'UNKNOWN' : 'FAILED', `${now}`, JSON.stringify(error), `${ttlMs}`],
    });
    if (failed !== 1 && failed !== '1') {
      throw new IdempotencyOwnershipLostError();
    }
  }

  public async read(mutationId: string): Promise<ExistingResult | null> {
    const key = this.key(mutationId);
    const raw = await this.client.get(key);
    if (!raw) {
      return null;
    }
    try {
      const record = JSON.parse(raw) as StoredRecord;
      if (!record || !record.status || !record.fingerprint) {
        throw new Error('invalid record');
      }
      return this.existing(key, record);
    } catch {
      throw new Error(`mutationId ${mutationId} idempotency state is invalid`);
    }
  }
}
