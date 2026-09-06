use crate::cluster::transport::MetaStoreTransport;
use crate::cluster::ClusterMetaStoreClient;
use crate::config::injection::Injector;
use crate::config::{is_router, Config};
use crate::metastore::{MetaStore, MetaStoreRpcClient};
use crate::sql::SqlService;
use crate::CubeError;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use warp::http::StatusCode;
use warp::Filter;

pub fn serve_status_probes(c: &Config) {
    let mut addresses = Vec::new();
    if let Some(addr) = c.config_obj().status_bind_address() {
        addresses.push(addr.clone());
    }
    // Workers get probes ONLY on their HTTP port, never SQL/upload/drain routes.
    if !is_router(c.config_obj().as_ref()) {
        if let Some(addr) = c.config_obj().http_bind_address() {
            if !addresses.contains(addr) {
                addresses.push(addr.clone());
            }
        }
    }
    for addr in addresses {
        let probes = RouterProbes {
            services: c.injector(),
        };
        let live = warp::path!("livez").and(warp::get()).map(|| StatusCode::OK);
        let health_probes = probes.clone();
        let health = warp::path!("healthz").and(warp::get()).and_then(move || {
            let probes = health_probes.clone();
            async move { status_probe_reply("health", probes.is_healthy().await) }
        });
        let ready = warp::path!("readyz").and(warp::get()).and_then(move || {
            let probes = probes.clone();
            async move { status_probe_reply("readiness", probes.is_ready().await) }
        });
        let addr: SocketAddr = addr.parse().expect("cannot parse status probe address");
        match warp::serve(live.or(health).or(ready)).try_bind_ephemeral(addr) {
            Ok((addr, future)) => {
                log::info!("Serving status probes at {}", addr);
                tokio::spawn(future);
            }
            Err(error) => log::error!("Failed to serve status probes at {}: {}", addr, error),
        }
    }
}

pub async fn check_meta_store(meta_store: &dyn MetaStore) -> Result<(), CubeError> {
    check_meta_store_read(async {
        meta_store.get_schemas().await?;
        Ok(())
    })
    .await
}

async fn check_meta_store_read(
    read: impl std::future::Future<Output = Result<(), CubeError>>,
) -> Result<(), CubeError> {
    tokio::time::timeout(Duration::from_secs(2), read)
        .await
        .map_err(|_| CubeError::internal("MetaStore health read timed out".into()))??;
    Ok(())
}

pub fn status_probe_reply(
    probe: &str,
    result: Result<(), CubeError>,
) -> Result<StatusCode, Infallible> {
    match result {
        Ok(()) => Ok(StatusCode::OK),
        Err(error) => {
            log::warn!("{} probe failed: {}", probe, error.display_with_backtrace());
            Ok(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

#[derive(Clone)]
struct RouterProbes {
    services: Arc<Injector>,
}

impl RouterProbes {
    async fn is_healthy(&self) -> Result<(), CubeError> {
        // Bound dependency resolution as well as I/O. The remote transport
        // depends only on ConfigObj, not the worker/SQL/MetaStore service graph.
        tokio::time::timeout(Duration::from_secs(2), async {
            if self
                .services
                .has_service_typed::<dyn MetaStoreTransport>()
                .await
            {
                let transport = self
                    .services
                    .get_service_typed::<dyn MetaStoreTransport>()
                    .await;
                let meta_store = MetaStoreRpcClient::new(ClusterMetaStoreClient::new(transport));
                return check_meta_store(&meta_store).await;
            }
            // try_get only reads initialized instances; it does not run factories.
            let meta_store = self
                .services
                .try_get_service_typed::<dyn MetaStore>()
                .await
                .ok_or_else(|| CubeError::internal("MetaStore is not ready".into()))?;
            check_meta_store(meta_store.as_ref()).await
        })
        .await
        .map_err(|_| CubeError::internal("MetaStore startup health timed out".into()))?
    }

    async fn is_ready(&self) -> Result<(), CubeError> {
        self.is_healthy().await?;
        if let Some(service) = self
            .services
            .try_get_service_typed::<dyn SqlService>()
            .await
        {
            if service
                .mutation_gate()
                .map(|gate| gate.status().draining)
                .unwrap_or(false)
            {
                return Err(CubeError::internal("Router is draining".into()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::message::NetworkMessage;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FailingMetaStoreTransport(Arc<AtomicUsize>);
    crate::di_service!(FailingMetaStoreTransport, [MetaStoreTransport]);

    #[async_trait::async_trait]
    impl MetaStoreTransport for FailingMetaStoreTransport {
        async fn meta_store_call(
            &self,
            _message: NetworkMessage,
        ) -> Result<NetworkMessage, CubeError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(CubeError::internal("MetaStore RPC is unavailable".into()))
        }
    }

    #[tokio::test]
    async fn worker_probe_uses_real_rpc_client_without_initializing_sql_or_metastore() {
        let services = Injector::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        services
            .register_typed::<dyn MetaStoreTransport, _, _, _>(async move |_| {
                Arc::new(FailingMetaStoreTransport(count))
            })
            .await;
        let probes = RouterProbes {
            services: services.clone(),
        };
        assert!(probes.is_healthy().await.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "probe must perform an actual RPC read"
        );
        assert!(services
            .try_get_service_typed::<dyn SqlService>()
            .await
            .is_none());
        assert!(services
            .try_get_service_typed::<dyn MetaStore>()
            .await
            .is_none());
    }

    #[tokio::test]
    async fn startup_without_metastore_fails_closed() {
        let probes = RouterProbes {
            services: Injector::new(),
        };
        assert!(probes.is_healthy().await.is_err());
        assert!(probes.is_ready().await.is_err());
    }

    #[tokio::test]
    async fn failed_read_is_not_a_successful_metastore_probe() {
        assert!(check_meta_store_read(async {
            Err(CubeError::internal("RPC connection closed".into()))
        })
        .await
        .is_err());
        assert!(check_meta_store_read(async { Ok(()) }).await.is_ok());
    }

    #[tokio::test]
    async fn stalled_metastore_probe_is_bounded() {
        let result = check_meta_store_read(std::future::pending::<Result<(), CubeError>>()).await;
        assert!(result.unwrap_err().to_string().contains("timed out"));
    }

    #[test]
    fn readiness_errors_are_service_unavailable() {
        assert_eq!(
            status_probe_reply("health", Ok(())).unwrap(),
            StatusCode::OK
        );
        assert_eq!(
            status_probe_reply("health", Err(CubeError::internal("offline".into()))).unwrap(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
