use super::{SystemChunksTableDef, SystemIndexesTableDef, SystemPartitionsTableDef};
use crate::cluster::message::NetworkMessage;
use crate::cluster::transport::MetaStoreTransport;
use crate::cluster::ClusterMetaStoreClient;
use crate::config::Config;
use crate::metastore::{
    Column, ColumnType, MetaStore, MetaStoreRpcClient, MetaStoreRpcServer, MetaStoreTable,
    RocksMetaStore,
};
use crate::queryplanner::query_executor::batches_to_dataframe;
use crate::queryplanner::InfoSchemaTableDef;
use crate::store::DataFrame;
use crate::table::{Row, TableValue};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct WireMetaStore {
    store: Arc<dyn MetaStore>,
    calls: Arc<AtomicUsize>,
}
crate::di_service!(WireMetaStore, [MetaStoreTransport]);

fn wire(message: NetworkMessage) -> NetworkMessage {
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    message.serialize(&mut serializer).unwrap();
    let bytes = serializer.take_buffer();
    NetworkMessage::deserialize(flexbuffers::Reader::get_root(bytes.as_slice()).unwrap()).unwrap()
}

#[async_trait]
impl MetaStoreTransport for WireMetaStore {
    async fn meta_store_call(&self, message: NetworkMessage) -> Result<NetworkMessage, CubeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match wire(message) {
            NetworkMessage::MetaStoreCall(call) => {
                let server = MetaStoreRpcServer::new(self.store.clone());
                Ok(wire(NetworkMessage::MetaStoreCallResult(
                    server.invoke_method(call).await,
                )))
            }
            _ => Err(CubeError::internal("Expected MetaStore RPC call".into())),
        }
    }
}

fn local_frame<D: InfoSchemaTableDef>(
    definition: D,
    rows: Vec<D::T>,
) -> Result<DataFrame, CubeError> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(definition.schema())),
        definition.columns(rows),
    )?;
    batches_to_dataframe(vec![batch])
}

#[tokio::test]
async fn metadata_sql_via_remote_metastore_preserves_local_rows_and_columns() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    Config::test("metadata_sql_via_remote_metastore_preserves_local_rows_and_columns")
        .start_with_injector_override(
            async move |injector| {
                // Initialize only the concrete origin. Resolving dyn MetaStore here
                // would cache the local service and silently bypass the RPC regression.
                let store = injector.get_service_typed::<RocksMetaStore>().await;
                assert!(injector.try_get_service_typed::<dyn MetaStore>().await.is_none());
                let transport = Arc::new(WireMetaStore { store, calls: count });
                injector.register_typed::<dyn MetaStore, _, _, _>(async move |_| {
                    Arc::new(MetaStoreRpcClient::new(ClusterMetaStoreClient::new(transport)))
                }).await;
            },
            async move |services| {
                let sql = services.sql_service;
                let store = services.injector.get_service_typed::<RocksMetaStore>().await;
                sql.exec_query("CREATE SCHEMA rpc_metadata").await?.collect().await?;
                sql.exec_query("CREATE TABLE rpc_metadata.events (id int)").await?.collect().await?;
                let table = store.get_table("rpc_metadata".into(), "events".into()).await?;
                let indexes = store.get_table_indexes(table.get_id()).await?;
                let partitions = store.get_active_partitions_by_index_id(indexes[0].get_id()).await?;
                // Include a staged, unuploaded chunk: a ready/active-only RPC
                // would silently change the system table's row semantics.
                let chunk = store.create_chunk(partitions[0].get_id(), 7, None, None, false).await?;
                assert!(!chunk.get_row().uploaded());

                let before = calls.load(Ordering::SeqCst);
                let schemas = sql.exec_query(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'rpc_metadata'",
                ).await?.collect().await?;
                assert!(calls.load(Ordering::SeqCst) > before);
                assert_eq!(schemas.as_ref(), &DataFrame::new(
                    vec![Column::new("schema_name".into(), ColumnType::String, 0)],
                    vec![Row::new(vec![TableValue::String("rpc_metadata".into())])],
                ));
                assert!(sql.exec_query(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'missing_rpc_schema'",
                ).await?.collect().await?.get_rows().is_empty());

                let expected_show = DataFrame::from(store.schemas_table().all_rows().await?);
                let actual_show = sql.exec_query("SHOW SCHEMAS").await?.collect().await?;
                assert_eq!(actual_show.as_ref(), &expected_show, "SHOW SCHEMAS must preserve its original columns and rows");
                assert!(sql.exec_query("SHOW SCHEMAS LIKE 'missing_rpc_%'").await.is_err(),
                    "unsupported SHOW modifiers must not return an unfiltered inventory");

                let mut chunks = store.chunks_table().all_rows().await?;
                chunks.sort_by_key(|row| row.get_id());
                assert!(!chunks.is_empty());
                let expected_show = DataFrame::from(chunks.clone());
                let actual_show = sql.exec_query("SHOW CHUNKS").await?.collect().await?;
                assert_eq!(actual_show.as_ref(), &expected_show, "SHOW CHUNKS must include unuploaded rows and preserve its original columns");
                let expected = local_frame(SystemChunksTableDef, chunks)?;
                let actual = sql.exec_query("SELECT * FROM system.chunks ORDER BY id").await?.collect().await?;
                assert_eq!(actual.as_ref(), &expected, "chunks must preserve all columns and unuploaded rows");

                let mut indexes = store.index_table().all_rows().await?;
                indexes.sort_by_key(|row| row.get_id());
                assert!(!indexes.is_empty());
                let expected_show = DataFrame::from(indexes.clone());
                let actual_show = sql.exec_query("SHOW INDEXES").await?.collect().await?;
                assert_eq!(actual_show.as_ref(), &expected_show, "SHOW INDEXES must preserve its original columns and rows");
                let expected = local_frame(SystemIndexesTableDef, indexes)?;
                let actual = sql.exec_query("SELECT * FROM system.indexes ORDER BY id").await?.collect().await?;
                assert_eq!(actual.as_ref(), &expected, "indexes must preserve the full local inventory");

                let mut partitions = store.partition_table().all_rows().await?;
                partitions.sort_by_key(|row| row.get_id());
                assert!(!partitions.is_empty());
                let expected_show = DataFrame::from(partitions.clone());
                let actual_show = sql.exec_query("SHOW PARTITIONS").await?.collect().await?;
                assert_eq!(actual_show.as_ref(), &expected_show, "SHOW PARTITIONS must preserve its original columns and rows");
                let expected = local_frame(SystemPartitionsTableDef, partitions)?;
                let actual = sql.exec_query("SELECT * FROM system.partitions ORDER BY id").await?.collect().await?;
                assert_eq!(actual.as_ref(), &expected, "partitions must preserve all columns and values");
                Ok(())
            },
        ).await;
}
