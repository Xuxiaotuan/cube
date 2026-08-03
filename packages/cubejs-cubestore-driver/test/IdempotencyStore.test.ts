import {
  IdempotencyOwnershipLostError,
  RedisIdempotencyStore,
} from '../src/IdempotencyStore';

type Entry = { value: string };

class InMemoryRedis {
  private readonly entries = new Map<string, Entry>();

  public async get(key: string): Promise<string | null> {
    return this.entries.get(key)?.value || null;
  }

  public clear(): void {
    this.entries.clear();
  }

  public async eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<string | number> {
    const key = options.keys[0];
    const args = options.arguments;
    const current = await this.get(key);
    const record = current ? JSON.parse(current) : null;

    if (script.includes('IDEMPOTENCY_ACQUIRE')) {
      if (record) {
        if (record.fingerprint !== args[0]) {
          return JSON.stringify({ kind: 'CONFLICT', fingerprint: record.fingerprint });
        }
        return JSON.stringify({ kind: 'EXISTING', record });
      }
      const created = {
        status: 'PENDING', fingerprint: args[0], ownerToken: args[1],
        startedAt: Number(args[2]), expiresAt: Number(args[2]) + Number(args[3]),
      };
      this.entries.set(key, { value: JSON.stringify(created) });
      return JSON.stringify({ kind: 'LEASE', record: created });
    }

    if (!record || record.status !== 'PENDING' || record.ownerToken !== args[0]) {
      return 0;
    }
    if (script.includes('IDEMPOTENCY_RENEW')) {
      record.expiresAt = Number(args[1]) + Number(args[2]);
      this.entries.set(key, { value: JSON.stringify(record) });
      return JSON.stringify(record);
    }
    if (script.includes('IDEMPOTENCY_COMPLETE')) {
      this.entries.set(key, { value: JSON.stringify({
        status: 'COMPLETED', fingerprint: record.fingerprint, startedAt: record.startedAt,
        finishedAt: Number(args[1]), expiresAt: Number(args[1]) + Number(args[3]), resultRef: args[2],
      }) });
      return 1;
    }
    if (script.includes('IDEMPOTENCY_FAIL')) {
      this.entries.set(key, { value: JSON.stringify({
        status: args[1], fingerprint: record.fingerprint, startedAt: record.startedAt,
        finishedAt: Number(args[2]), expiresAt: args[1] === 'UNKNOWN' ? 0 : Number(args[2]) + Number(args[4]), error: JSON.parse(args[3]),
      }) });
      return 1;
    }
    throw new Error('unknown script');
  }
}

const createStore = (redis = new InMemoryRedis()) => ({
  redis,
  store: new RedisIdempotencyStore(redis, {
    keyPrefix: 'test:mutation', pendingTtlSeconds: 1, completedTtlSeconds: 60, failedTtlSeconds: 60,
  }),
});

describe('RedisIdempotencyStore', () => {
  it('renews a mutation that runs longer than its original pending TTL', async () => {
    const { store } = createStore();
    const now = jest.spyOn(Date, 'now').mockReturnValue(2_000).mockReturnValueOnce(1_000);
    try {
      const lease = await store.acquire('long-running', 'fingerprint');
      expect('ownerToken' in lease).toBe(true);
      if (!('ownerToken' in lease)) throw new Error('expected lease');
      const renewed = await store.renew(lease);
      expect(renewed.expiresAt).toBeGreaterThan(lease.expiresAt);
      await store.complete(renewed, 'empty');
    } finally {
      now.mockRestore();
    }
  });

  it('fails closed after a Redis restart loses a pending lease', async () => {
    const { store, redis } = createStore();
    const lease = await store.acquire('restart', 'fingerprint');
    if (!('ownerToken' in lease)) throw new Error('expected lease');
    redis.clear();
    await expect(store.renew(lease)).rejects.toBeInstanceOf(IdempotencyOwnershipLostError);
  });

  it('allows only one of two Cube API replicas to own a mutation', async () => {
    const { redis } = createStore();
    const first = new RedisIdempotencyStore(redis, { keyPrefix: 'test:mutation', pendingTtlSeconds: 1, completedTtlSeconds: 60, failedTtlSeconds: 60 });
    const second = new RedisIdempotencyStore(redis, { keyPrefix: 'test:mutation', pendingTtlSeconds: 1, completedTtlSeconds: 60, failedTtlSeconds: 60 });
    const winner = await first.acquire('replicas', 'fingerprint');
    const follower = await second.acquire('replicas', 'fingerprint');
    expect('ownerToken' in winner).toBe(true);
    expect('ownerToken' in follower).toBe(false);
  });

  it('rejects stale completion after ownership changes', async () => {
    const { store, redis } = createStore();
    const first = await store.acquire('stale', 'fingerprint');
    if (!('ownerToken' in first)) throw new Error('expected lease');
    redis.clear();
    const second = await store.acquire('stale', 'fingerprint');
    if (!('ownerToken' in second)) throw new Error('expected lease');
    await expect(store.complete(first, 'empty')).rejects.toBeInstanceOf(IdempotencyOwnershipLostError);
    await store.complete(second, 'empty');
  });

  it('rejects conflicting fingerprints for the same mutation id', async () => {
    const { store } = createStore();
    await store.acquire('conflict', 'first');
    await expect(store.acquire('conflict', 'second')).rejects.toThrow('mutationId conflict');
  });

  it('persists connection loss after commit as UNKNOWN instead of allowing a replay', async () => {
    const { store } = createStore();
    const lease = await store.acquire('connection-loss', 'fingerprint');
    if (!('ownerToken' in lease)) throw new Error('expected lease');
    await store.fail(lease, { name: 'MutationUnknownError', message: 'connection closed after send', code: 'MUTATION_UNKNOWN' });
    const observed = await store.acquire('connection-loss', 'fingerprint');
    expect('ownerToken' in observed).toBe(false);
    if ('ownerToken' in observed) throw new Error('expected existing state');
    expect(observed.status).toBe('UNKNOWN');
    expect(observed.expiresAt).toBe(0);
  });
});
