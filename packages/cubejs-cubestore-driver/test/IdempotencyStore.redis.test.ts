import { createClient } from 'redis';
import {
  IdempotencyOwnershipLostError,
  RedisIdempotencyStore,
} from '../src/IdempotencyStore';

const redisUrl = process.env.REDIS_URL;
const redisIt = redisUrl ? it : it.skip;

describe('RedisIdempotencyStore Lua integration', () => {
  redisIt('uses Redis owner CAS and preserves UNKNOWN as a tombstone', async () => {
    const client = createClient({ url: redisUrl });
    const prefix = `cubestore-idempotency-test:${Date.now()}:${Math.random()}`;
    const createStore = () => new RedisIdempotencyStore(client, {
      keyPrefix: prefix,
      pendingTtlSeconds: 1,
      completedTtlSeconds: 60,
      failedTtlSeconds: 60,
    });

    await client.connect();
    try {
      const first = createStore();
      const second = createStore();
      const lease = await first.acquire('mutation', 'fingerprint');
      if (!('ownerToken' in lease)) throw new Error('expected lease');

      const follower = await second.acquire('mutation', 'fingerprint');
      expect('ownerToken' in follower).toBe(false);

      await first.fail(lease, {
        name: 'MutationUnknownError',
        message: 'connection closed after send',
        code: 'MUTATION_UNKNOWN',
      });
      await expect(first.renew(lease)).rejects.toBeInstanceOf(IdempotencyOwnershipLostError);

      const state = await second.acquire('mutation', 'fingerprint');
      expect('ownerToken' in state).toBe(false);
      if ('ownerToken' in state) throw new Error('UNKNOWN tombstone must deny acquisition');
      expect(state.status).toBe('UNKNOWN');
      expect(await client.ttl(lease.key)).toBe(-1);
    } finally {
      await client.del(`${prefix}:mutation`);
      await client.quit();
    }
  });
});
