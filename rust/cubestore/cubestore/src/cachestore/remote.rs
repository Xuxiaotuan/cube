use super::{CacheStoreRpcClientTransport, CacheStoreRpcMethodCall, CacheStoreRpcMethodResult};
use crate::cluster::message::NetworkMessage;
use crate::cluster::transport::MetaStoreTransport;
use crate::CubeError;
use async_trait::async_trait;
use std::sync::Arc;

/// CACHE and QUEUE use the same authoritative MetaStore endpoint on every router.
/// Do not retry here: a lost mutation response must be reconciled by its caller.
pub struct RemoteCacheStoreTransport {
    transport: Arc<dyn MetaStoreTransport>,
}

impl RemoteCacheStoreTransport {
    pub fn new(transport: Arc<dyn MetaStoreTransport>) -> Arc<Self> {
        Arc::new(Self { transport })
    }
}

#[async_trait]
impl CacheStoreRpcClientTransport for RemoteCacheStoreTransport {
    async fn invoke_method(
        &self,
        call: CacheStoreRpcMethodCall,
    ) -> Result<CacheStoreRpcMethodResult, CubeError> {
        match self
            .transport
            .meta_store_call(NetworkMessage::CacheStoreCall(call))
            .await?
        {
            NetworkMessage::CacheStoreCallResult(result) => Ok(result),
            other => Err(CubeError::internal(format!(
                "Unexpected CacheStore RPC response: {:?}",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cachestore::{CacheItem, CacheStore, CacheStoreRpcClient, CacheStoreRpcServer};
    use crate::config::Config;
    use serde::{Deserialize, Serialize};

    struct Loopback {
        store: Arc<dyn CacheStore>,
    }
    crate::di_service!(Loopback, [MetaStoreTransport]);

    fn wire(message: NetworkMessage) -> NetworkMessage {
        let mut serializer = flexbuffers::FlexbufferSerializer::new();
        message.serialize(&mut serializer).unwrap();
        let bytes = serializer.take_buffer();
        NetworkMessage::deserialize(flexbuffers::Reader::get_root(bytes.as_slice()).unwrap())
            .unwrap()
    }

    #[async_trait]
    impl MetaStoreTransport for Loopback {
        async fn meta_store_call(
            &self,
            message: NetworkMessage,
        ) -> Result<NetworkMessage, CubeError> {
            match wire(message) {
                NetworkMessage::CacheStoreCall(call) => {
                    let server = CacheStoreRpcServer::new(self.store.clone());
                    Ok(wire(NetworkMessage::CacheStoreCallResult(
                        server.invoke_method(call).await,
                    )))
                }
                _ => Err(CubeError::internal("Expected CacheStore RPC".to_string())),
            }
        }
    }

    #[tokio::test]
    async fn remote_cache_shared_nx_survives_router_client_replacement() {
        Config::test("remote_cache_shared_nx_survives_router_client_replacement")
            .start_test(|services| async move {
                let store = services
                    .injector
                    .get_service_typed::<dyn CacheStore>()
                    .await;
                let transport = Arc::new(Loopback { store });
                let first =
                    CacheStoreRpcClient::new(RemoteCacheStoreTransport::new(transport.clone()));
                let second =
                    CacheStoreRpcClient::new(RemoteCacheStoreTransport::new(transport.clone()));
                let key = "PRE_AGG_MANIFEST_V1:test.target".to_string();
                let (a, b) = tokio::join!(
                    first.cache_set(CacheItem::new(key.clone(), None, "first".to_string()), true),
                    second.cache_set(
                        CacheItem::new(key.clone(), None, "second".to_string()),
                        true
                    ),
                );
                let winner = if a? {
                    assert!(!b?);
                    "first"
                } else {
                    assert!(b?);
                    "second"
                };
                drop(first);
                drop(second);
                let replacement =
                    CacheStoreRpcClient::new(RemoteCacheStoreTransport::new(transport));
                assert_eq!(
                    replacement
                        .cache_get(key)
                        .await?
                        .unwrap()
                        .get_row()
                        .get_value(),
                    winner
                );
                Ok(())
            })
            .await;
    }
}
