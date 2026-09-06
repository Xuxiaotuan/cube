import { CubeStoreCacheDriver, CubeStoreDriver, CubeStoreQueueDriver } from '@cubejs-backend/cubestore-driver';
import type { RequestHandler } from 'express';
import type { OrchestratorApi } from './OrchestratorApi';

/** Configuration/capability evidence, not proof of a completed recovery or of
 * the remote MetaStore's durability. Use the effective orchestrator instances,
 * not NODE_ENV or an operator-provided declaration of support. The /readyz
 * caller inspects only the default context, not every tenant/scheduled context.
 */
export async function fileImportBuildScheduling(api: OrchestratorApi): Promise<boolean> {
  try {
    const orchestrator = api.getQueryOrchestrator();
    const cache = orchestrator.getQueryCache();
    const preAggregations = orchestrator.getPreAggregations();
    const preAggregationOptions = preAggregations.options as typeof preAggregations.options & { externalRefresh?: boolean };
    if (!(cache.getCacheDriver() instanceof CubeStoreCacheDriver) ||
        cache.options.cacheAndQueueDriver !== 'cubestore' ||
        preAggregations.options.cacheAndQueueDriver !== 'cubestore' ||
        preAggregationOptions.externalRefresh ||
        !preAggregations.externalDriverFactory) return false;

    const driver = await preAggregations.externalDriverFactory();
    if (!(driver instanceof CubeStoreDriver) ||
        !['resolvePreAggregationBuild', 'resumePreAggregationBuild', 'getPreAggregationBuildStatus', 'getProtectedPreAggregationTables']
          .every(method => typeof driver[method] === 'function')) return false;

    // Overrides can select a different cache/queue service even when the top
    // level setting says cubestore. Unknown equivalence is deliberately false.
    if (await cache.options.cubeStoreDriverFactory?.() !== driver ||
        await preAggregations.options.cubeStoreDriverFactory?.() !== driver) return false;

    const source = 'default'; // Default-context capability, not tenant-wide evidence.
    const queueOptions = await preAggregations.options.queueOptions(source) as { skipQueue?: boolean };
    const queryQueueOptions = await cache.options.queueOptions?.(source);
    if (queueOptions?.skipQueue || (queryQueueOptions as any)?.skipQueue) return false;
    const buildQueue = await preAggregations.getQueue(source);
    const queryQueue = await cache.getQueue(source);
    return buildQueue.getQueueDriver() instanceof CubeStoreQueueDriver &&
      queryQueue.getQueueDriver() instanceof CubeStoreQueueDriver;
  } catch {
    return false;
  }
}

/** Add only JSON metadata; the original readiness handler retains ownership of
 * status codes and health checks. Capability failures never turn health DOWN.
 */
export function recoveryCapabilitiesMiddleware(getApi: () => Promise<OrchestratorApi | null>): RequestHandler {
  return async (_req, res, next) => {
    let supported = false;
    try {
      const api = await getApi();
      if (api) supported = await fileImportBuildScheduling(api);
    } catch {
      // Unknown configuration is not positive capability evidence.
    }
    const json = res.json;
    res.json = function withRecoveryCapabilities(body) {
      res.json = json;
      return json.call(this, {
        ...body,
        recoveryCapabilities: { ...body?.recoveryCapabilities, fileImportBuildScheduling: supported },
      });
    };
    next();
  };
}
