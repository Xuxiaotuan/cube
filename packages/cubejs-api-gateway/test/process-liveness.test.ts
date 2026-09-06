import express from 'express';
import request from 'supertest';
import { ApiGateway } from '../src';

function setup() {
  const storage = {
    testConnections: jest.fn().mockResolvedValue(undefined),
    testOrchestratorConnections: jest.fn().mockResolvedValue(undefined),
  };
  const compilerApi = jest.fn().mockRejectedValue(new Error('Compiler must not be used by process probes'));
  const adapterApi = jest.fn().mockRejectedValue(new Error('Router unavailable'));
  const checkAuth = jest.fn().mockRejectedValue(new Error('No API credentials'));
  const gateway = new ApiGateway('secret', compilerApi, adapterApi, jest.fn(), {
    standalone: true,
    basePath: '/cubejs-api',
    dataSourceStorage: storage,
    refreshScheduler: {},
    enforceSecurityChecks: true,
    checkAuth,
  });
  const app = express();
  gateway.initApp(app);
  return { app, gateway, storage, compilerApi, adapterApi, checkAuth };
}

describe('process-only liveness', () => {
  it('returns the existing health JSON without credentials or dependency access', async () => {
    const { app, gateway, storage, compilerApi, adapterApi, checkAuth } = setup();
    try {
      await request(app).get('/livez/process').timeout(1000).expect(200, { health: 'HEALTH' });
      expect(storage.testConnections).not.toHaveBeenCalled();
      expect(storage.testOrchestratorConnections).not.toHaveBeenCalled();
      expect(compilerApi).not.toHaveBeenCalled();
      expect(adapterApi).not.toHaveBeenCalled();
      expect(checkAuth).not.toHaveBeenCalled();
    } finally {
      gateway.release();
    }
  });

  it.each(['testConnections', 'testOrchestratorConnections'] as const)(
    'remains healthy when %s fails, without changing legacy probe semantics', async dependency => {
      const { app, gateway, storage, adapterApi } = setup();
      storage[dependency].mockRejectedValue(new Error('Router is draining'));
      try {
        await request(app).get('/livez').timeout(1000).expect(500, { health: 'DOWN' });
        const before = [storage.testConnections.mock.calls.length, storage.testOrchestratorConnections.mock.calls.length];
        await request(app).get('/livez/process').timeout(1000).expect(200, { health: 'HEALTH' });
        expect([storage.testConnections.mock.calls.length, storage.testOrchestratorConnections.mock.calls.length]).toEqual(before);
        expect(adapterApi).not.toHaveBeenCalled();
        await request(app).get('/readyz').timeout(1000).expect(500, { health: 'DOWN' });
        expect(adapterApi).toHaveBeenCalledTimes(1);
      } finally {
        gateway.release();
      }
    }
  );

  it.each(['testConnections', 'testOrchestratorConnections'] as const)(
    'responds while legacy /livez is blocked inside %s', async dependency => {
      const { app, gateway, storage, compilerApi, adapterApi } = setup();
      let release!: () => void;
      let entered!: () => void;
      const blocked = new Promise<void>(resolve => { release = resolve; });
      const started = new Promise<void>(resolve => { entered = resolve; });
      storage[dependency].mockImplementation(() => { entered(); return blocked; });
      let legacyCompleted = false;
      // Calling then starts the real HTTP request before probing the new route.
      const legacy = request(app).get('/livez').expect(200, { health: 'HEALTH' }).then(() => { legacyCompleted = true; });
      try {
        await started;
        const before = [storage.testConnections.mock.calls.length, storage.testOrchestratorConnections.mock.calls.length];
        await request(app).get('/livez/process').timeout(1000).expect(200, { health: 'HEALTH' });
        expect(legacyCompleted).toBe(false);
        expect([storage.testConnections.mock.calls.length, storage.testOrchestratorConnections.mock.calls.length]).toEqual(before);
        expect(compilerApi).not.toHaveBeenCalled();
        expect(adapterApi).not.toHaveBeenCalled();
      } finally {
        release();
        await legacy;
        gateway.release();
      }
    }
  );
});
