import express from 'express';
import { createServer, get } from 'http';
import type { AddressInfo } from 'net';
import { ApiGateway } from '@cubejs-backend/api-gateway';
import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import { OrchestratorApi } from '../src/core/OrchestratorApi';
import { fileImportBuildScheduling, recoveryCapabilitiesMiddleware } from '../src/core/recoveryCapabilities';
import { CubejsServerCore } from '../src/core/server';

function setup(overrides: any = {}) {
  const driver = new CubeStoreDriver({ host: 'localhost' });
  const api = new OrchestratorApi(async () => driver, jest.fn(), {
    contextToDbType: async () => 'cubestore',
    contextToExternalDbType: () => 'cubestore',
    cacheAndQueueDriver: 'cubestore',
    externalDriverFactory: async () => driver,
    queryCacheOptions: { queueOptions: async () => ({ concurrency: 1 }) },
    preAggregationsOptions: { queueOptions: async () => ({ concurrency: 1 }) },
    ...overrides,
  });
  jest.spyOn(api, 'testConnection').mockResolvedValue(undefined);
  jest.spyOn(api, 'testOrchestratorConnections').mockResolvedValue(undefined);
  return { api, driver };
}

describe('effective file-import build scheduling capability', () => {
  it('requires actual shared CubeStore cache/queues and the resumable driver', async () => {
    const { api } = setup();
    await expect(fileImportBuildScheduling(api)).resolves.toBe(true);
  });

  it.each([
    { cacheAndQueueDriver: 'memory' },
    { queryCacheOptions: { cacheAndQueueDriver: 'memory' } },
    { preAggregationsOptions: { cacheAndQueueDriver: 'memory', queueOptions: async () => ({ concurrency: 1 }) } },
    { preAggregationsOptions: { externalRefresh: true, queueOptions: async () => ({ concurrency: 1 }) } },
    { preAggregationsOptions: { queueOptions: async () => ({ concurrency: 1, skipQueue: true }) } },
    { preAggregationsOptions: { queueOptions: async () => ({ concurrency: 1, cacheAndQueueDriver: 'memory' }) } },
  ])('rejects effective configuration overrides: %j', async overrides => {
    const { api } = setup(overrides);
    await expect(fileImportBuildScheduling(api)).resolves.toBe(false);
  });

  it('rejects an older driver without resume support', async () => {
    const { api, driver } = setup();
    (driver as any).resumePreAggregationBuild = undefined;
    await expect(fileImportBuildScheduling(api)).resolves.toBe(false);
  });

  it('rejects an unverified separate CubeStore cache service', async () => {
    const other = new CubeStoreDriver({ host: 'other' });
    const { api } = setup({ queryCacheOptions: { cubeStoreDriverFactory: async () => other } });
    await expect(fileImportBuildScheduling(api)).resolves.toBe(false);
  });

  it('fails closed when the configured external factory cannot resolve', async () => {
    const { api } = setup({ externalDriverFactory: async () => { throw new Error('unavailable'); } });
    await expect(fileImportBuildScheduling(api)).resolves.toBe(false);
  });
});

describe('real API readiness JSON', () => {
  async function request(api: OrchestratorApi, path: string, capabilityThrows = false, options: {
    standalone?: boolean;
    getApi?: (context: any) => Promise<OrchestratorApi>;
  } = {}) {
    const app = express();
    const standalone = options.standalone ?? true;
    const gateway = new ApiGateway('test-secret', jest.fn(), async () => api, jest.fn(), {
      standalone,
      basePath: '/cubejs-api',
      refreshScheduler: jest.fn(),
      dataSourceStorage: { testConnections: async () => undefined, testOrchestratorConnections: async () => undefined } as any,
    });
    if (capabilityThrows) {
      app.get('/readyz', recoveryCapabilitiesMiddleware(async () => { throw new Error('capability unavailable'); }));
      gateway.initApp(app);
    } else {
      // Exercise the actual server-core route registration, not a demo endpoint.
      await CubejsServerCore.prototype.initApp.call({
        apiGateway: () => gateway, standalone,
        getOrchestratorApi: options.getApi || (async () => api), options: { devServer: false },
      }, app);
    }
    const server = createServer(app);
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    try {
      return await new Promise<{ status: number; body: any }>((resolve, reject) => {
        get({ host: '127.0.0.1', port: (server.address() as AddressInfo).port, path, agent: false }, response => {
          let body = '';
          response.on('data', chunk => { body += chunk; });
          response.on('end', () => resolve({ status: response.statusCode!, body: JSON.parse(body) }));
        }).on('error', reject);
      });
    } finally {
      gateway.release();
      await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
    }
  }

  it('adds capability evidence to HEALTH without replacing health semantics', async () => {
    const { api } = setup();
    await expect(request(api, '/readyz')).resolves.toEqual({ status: 200, body: { health: 'HEALTH', recoveryCapabilities: { fileImportBuildScheduling: true } } });
    expect(api.testConnection).toHaveBeenCalled();
    expect(api.testOrchestratorConnections).toHaveBeenCalled();
  });

  it('keeps DOWN and HTTP 500 even when configuration supports recovery', async () => {
    const { api } = setup();
    jest.mocked(api.testConnection).mockRejectedValue(new Error('source down'));
    await expect(request(api, '/readyz')).resolves.toEqual({ status: 500, body: { health: 'DOWN', recoveryCapabilities: { fileImportBuildScheduling: true } } });
  });

  it('keeps HEALTH when capability inspection is unavailable', async () => {
    const { api } = setup();
    await expect(request(api, '/readyz', true)).resolves.toEqual({ status: 200, body: { health: 'HEALTH', recoveryCapabilities: { fileImportBuildScheduling: false } } });
  });

  it('does not alter /livez', async () => {
    const { api } = setup();
    await expect(request(api, '/livez')).resolves.toEqual({ status: 200, body: { health: 'HEALTH' } });
  });

  it('checks a non-standalone default context instead of rejecting contextToAppId configurations', async () => {
    const { api } = setup();
    const getApi = jest.fn(async () => api);
    await expect(request(api, '/readyz', false, { standalone: false, getApi })).resolves.toEqual({
      status: 200, body: { health: 'HEALTH', recoveryCapabilities: { fileImportBuildScheduling: true } },
    });
    expect(getApi).toHaveBeenCalledTimes(1);
    expect(getApi).toHaveBeenCalledWith({});
    // Preserve the original non-standalone health handler's behavior.
    expect(api.testConnection).not.toHaveBeenCalled();
    expect(api.testOrchestratorConnections).not.toHaveBeenCalled();
  });

  it.each([
    { cacheAndQueueDriver: 'memory' },
    { preAggregationsOptions: { externalRefresh: true, queueOptions: async () => ({ concurrency: 1 }) } },
    { preAggregationsOptions: { queueOptions: async () => ({ concurrency: 1, skipQueue: true }) } },
  ])('keeps a non-standalone unsupported default context false: %j', async overrides => {
    const { api } = setup(overrides);
    const getApi = jest.fn(async () => api);
    await expect(request(api, '/readyz', false, { standalone: false, getApi })).resolves.toEqual({
      status: 200, body: { health: 'HEALTH', recoveryCapabilities: { fileImportBuildScheduling: false } },
    });
    expect(getApi).toHaveBeenCalledWith({});
  });

  it('fails closed when a non-standalone factory cannot construct the default context', async () => {
    const { api } = setup();
    const getApi = jest.fn(async () => { throw new Error('Tenant context is required'); });
    await expect(request(api, '/readyz', false, { standalone: false, getApi })).resolves.toEqual({
      status: 200, body: { health: 'HEALTH', recoveryCapabilities: { fileImportBuildScheduling: false } },
    });
    expect(getApi).toHaveBeenCalledTimes(1);
    expect(getApi).toHaveBeenCalledWith({});
  });
});
