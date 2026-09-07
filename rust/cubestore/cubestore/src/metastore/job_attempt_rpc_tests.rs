//! Loopback fault tests: production RPC client/server and framing writer, real
//! RocksMetaStore, with faults only at the transport's request/response boundary.
use crate::cluster::message::NetworkMessage;
use crate::cluster::transport::MetaStoreTransport;
use crate::cluster::ClusterMetaStoreClient;
use crate::config::Config;
use crate::import::ImportService;
use crate::metastore::job::{Job, JobAttempt, JobRunnerPool, JobStatus, JobType, JOB_ATTEMPT};
use crate::metastore::{
    Column, ColumnType, ImportFormat, MetaStore, MetaStoreRpcClient, MetaStoreRpcMethodCall,
    MetaStoreRpcMethodResult, MetaStoreRpcServer, RocksMetaStore, RowKey, TableId,
};
use crate::table::{Row, TableValue};
use crate::util::aborting_join_handle::AbortingJoinHandle;
use crate::CubeError;
use async_trait::async_trait;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::timeout;
use warp::Filter;

#[derive(Default)]
struct Faults {
    pause_publish: AtomicBool,
    drop_publish_response: AtomicBool,
    drop_finish_response: AtomicBool,
    publish_arrived: Notify,
    release_publish: Notify,
    owned_calls: AtomicUsize,
}

struct TcpMetaStore {
    address: SocketAddr,
}
crate::di_service!(TcpMetaStore, [MetaStoreTransport]);

// Match the existing network_message_compat fixture's v1 framing. A dropped
// socket must be an actual read error, not a fabricated successful RPC result.
async fn read_frame(stream: &mut TcpStream) -> Result<NetworkMessage, CubeError> {
    let magic = stream.read_u32().await?;
    let version = stream.read_u32().await?;
    if magic != 94107 || version != 1 {
        return Err(CubeError::internal("Unexpected loopback RPC frame".into()));
    }
    let size = stream.read_u64().await?;
    if size > 16 * 1024 * 1024 {
        return Err(CubeError::internal("Loopback RPC frame is too large".into()));
    }
    let mut bytes = vec![0; size as usize];
    stream.read_exact(&mut bytes).await?;
    Ok(NetworkMessage::deserialize(flexbuffers::Reader::get_root(bytes.as_slice())?)?)
}

#[async_trait]
impl MetaStoreTransport for TcpMetaStore {
    async fn meta_store_call(&self, message: NetworkMessage) -> Result<NetworkMessage, CubeError> {
        timeout(Duration::from_secs(10), async {
            let mut stream = TcpStream::connect(self.address).await?;
            message.send(&mut stream).await?;
            read_frame(&mut stream).await
        }).await?
    }
}

async fn serve_request(
    mut stream: TcpStream,
    store: Arc<dyn MetaStore>,
    faults: Arc<Faults>,
) -> Result<(), CubeError> {
    let (attempt, call) = match read_frame(&mut stream).await? {
        NetworkMessage::MetaStoreCallWithAttempt(attempt, call) => {
            faults.owned_calls.fetch_add(1, Ordering::SeqCst);
            (Some(attempt), call)
        }
        NetworkMessage::MetaStoreCall(call) => (None, call),
        _ => return Err(CubeError::internal("Expected MetaStore RPC".into())),
    };
    let publishing = matches!(&call, MetaStoreRpcMethodCall::publishImportChunks(..));
    let finishing = matches!(&call, MetaStoreRpcMethodCall::finishJobAttempt(..));
    if publishing && faults.pause_publish.swap(false, Ordering::SeqCst) {
        faults.publish_arrived.notify_one();
        timeout(Duration::from_secs(10), faults.release_publish.notified()).await?;
    }
    let server = MetaStoreRpcServer::new(store);
    let result = JOB_ATTEMPT.scope(attempt, server.invoke_method(call)).await;
    if publishing && matches!(&result, MetaStoreRpcMethodResult::publishImportChunks(Ok(())))
        && faults.drop_publish_response.swap(false, Ordering::SeqCst)
    {
        return Ok(()); // Commit completed; deliberately close TCP without its response.
    }
    if finishing && matches!(&result, MetaStoreRpcMethodResult::finishJobAttempt(Ok(_)))
        && faults.drop_finish_response.swap(false, Ordering::SeqCst)
    {
        return Ok(());
    }
    NetworkMessage::MetaStoreCallResult(result).send(&mut stream).await?;
    Ok(())
}

async fn remote_store(
    store: Arc<dyn MetaStore>,
    faults: Arc<Faults>,
) -> (Arc<MetaStoreRpcClient>, AbortingJoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let transport = Arc::new(TcpMetaStore { address: listener.local_addr().unwrap() });
    let task = AbortingJoinHandle::new(tokio::spawn(async move {
        // Concurrent connections let reclaim overtake a paused old request.
        // Dropping this fixture aborts both the listener and all connection tasks.
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let store = store.clone();
                    let faults = faults.clone();
                    connections.spawn(async move { serve_request(stream, store, faults).await });
                }
                result = connections.join_next(), if !connections.is_empty() => {
                    result.unwrap().unwrap().unwrap();
                }
            }
        }
    }));
    (Arc::new(MetaStoreRpcClient::new(ClusterMetaStoreClient::new(transport))), task)
}

async fn prepare_import(store: &dyn MetaStore, location: &str) -> Result<(u64, u64, JobAttempt), CubeError> {
    store.create_schema("task4_rpc".into(), false).await?;
    let table = store.create_table(
        "task4_rpc".into(), "rows".into(),
        vec![Column::new("n".into(), ColumnType::Int, 0)],
        Some(vec![location.to_string()]), Some(ImportFormat::CSVNoHeader), vec![], false,
        None, None, None, None, None, None, None, None, None, false, None,
    ).await?;
    store.add_job(Job::new(
        RowKey::Table(TableId::Tables, table.get_id()),
        JobType::TableImportCSV(location.to_string()), "task4-fixture-worker".into(),
    )).await?;
    let job = store.start_processing_job("task4-fixture-worker".into(), JobRunnerPool::Regular)
        .await?.unwrap();
    let index = store.get_table_indexes(table.get_id()).await?[0].get_id();
    Ok((table.get_id(), index, job.get_row().attempt().unwrap().clone()))
}

#[tokio::test]
async fn task4_rpc_old_attempt_is_rejected_after_tcp_pause() -> Result<(), CubeError> {
    let name = "task4_rpc_old_attempt_is_rejected_after_tcp_pause";
    let (_, origin) = RocksMetaStore::prepare_test_metastore(name);
    let faults = Arc::new(Faults::default());
    let (remote, listener) = remote_store(origin.clone(), faults.clone()).await;
    let (table_id, index, old) = prepare_import(remote.as_ref(), "file.csv").await?;
    let partitions = remote.get_active_partitions_by_index_id(index).await?;
    let chunk = JOB_ATTEMPT.scope(Some(old.clone()),
        remote.create_chunk(partitions[0].get_id(), 3, None, None, false)).await?;
    let chunk_id = chunk.get_id();
    faults.pause_publish.store(true, Ordering::SeqCst);
    let client = remote.clone();
    let token = old.clone();
    let pending = AbortingJoinHandle::new(tokio::spawn(async move {
        JOB_ATTEMPT.scope(Some(token.clone()), client.publish_import_chunks(
            token, table_id, "file.csv".into(), vec![(chunk_id, Some(10))],
        )).await
    }));
    timeout(Duration::from_secs(10), faults.publish_arrived.notified()).await?;
    remote.recover_job(remote.get_job(old.job_id).await?, "task4-fixture-worker".into(), Duration::ZERO)
        .await?.unwrap();
    let current = remote.start_processing_job("task4-fixture-worker".into(), JobRunnerPool::Regular)
        .await?.unwrap();
    assert_ne!(current.get_row().attempt(), Some(&old));
    faults.release_publish.notify_one();
    assert!(timeout(Duration::from_secs(10), pending).await??.is_err());
    assert!(!remote.get_chunk(chunk_id).await?.get_row().active());
    assert!(remote.get_job(old.job_id).await?.get_row().completed_imports().is_empty());
    // Untagged router/legacy finalization cannot bypass missing import receipts.
    assert!(remote.table_ready(table_id, true).await.is_err());
    assert!(remote.activate_chunks(table_id, vec![(chunk_id, Some(10))], None).await.is_err());
    assert!(!remote.get_table_by_id(table_id).await?.get_row().is_ready());
    assert!(faults.owned_calls.load(Ordering::SeqCst) >= 2,
        "the production client must put ambient attempts on the actual wire");
    drop(remote);
    drop(listener);
    drop(origin);
    // Give abort-on-drop tasks a chance to release the DB before test cleanup.
    tokio::task::yield_now().await;
    RocksMetaStore::cleanup_test_metastore(name);
    Ok(())
}

#[tokio::test]
async fn task4_rpc_source_import_reconciles_lost_publish_and_finish_responses() {
    let faults = Arc::new(Faults::default());
    let setup_faults = faults.clone();
    let (setup_tx, setup_rx) = tokio::sync::oneshot::channel();
    Config::test("task4_rpc_source_import_reconciles_lost_publish_and_finish_responses")
        .start_with_injector_override(
            async move |injector| {
                let origin = injector.get_service_typed::<RocksMetaStore>().await;
                assert!(injector.try_get_service_typed::<dyn MetaStore>().await.is_none());
                let (remote, rpc_listener) = remote_store(origin, setup_faults).await;
                let requests = Arc::new(AtomicUsize::new(0));
                let count = requests.clone();
                let route = warp::path("rows.csv").map(move || {
                    count.fetch_add(1, Ordering::SeqCst);
                    "11\n22\n33\n"
                });
                let (address, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
                let source_listener = AbortingJoinHandle::new(tokio::spawn(server));
                let source = format!("http://{}/rows.csv", address);
                let injected = remote.clone();
                injector.register_typed::<dyn MetaStore, _, _, _>(async move |_| injected).await;
                // Hold the event receiver before setup writes, without starting
                // processing loops or caching the local MetaStore in services.
                let _scheduler = injector.get_service_typed::<crate::scheduler::SchedulerImpl>().await;
                // Claim before processing loops start, so this test controls its
                // worker lifetime instead of racing the background job runner.
                let (table_id, index, attempt) = prepare_import(remote.as_ref(), &source).await.unwrap();
                assert!(setup_tx.send((remote, rpc_listener, source_listener, source, requests,
                    table_id, index, attempt)).is_ok());
            },
            async move |services| {
                let (remote, rpc_listener, source_listener, source, requests, table_id, index, old) = setup_rx.await?;
                let importer = services.injector.get_service_typed::<dyn ImportService>().await;
                faults.drop_publish_response.store(true, Ordering::SeqCst);
                let result = timeout(Duration::from_secs(20), JOB_ATTEMPT.scope(Some(old.clone()),
                    importer.clone().import_table_part(table_id, &source, None))).await?;
                assert!(result.is_err(), "a committed but lost response must not report success");
                assert!(!faults.drop_publish_response.load(Ordering::SeqCst), "fault must follow a successful commit");
                assert!(requests.load(Ordering::SeqCst) > 0, "the importer must read the actual HTTP CSV source");
                assert_eq!(remote.get_job(old.job_id).await?.get_row().completed_imports(), &[source.clone()]);
                let before = remote.get_active_partitions_and_chunks_by_index_id_for_select(vec![index]).await?;
                let mut before_ids = before[0].iter().flat_map(|(_, chunks)| chunks.iter().map(|c| c.get_id())).collect::<Vec<_>>();
                before_ids.sort_unstable();
                assert!(!before_ids.is_empty());
                // Model the job runner persisting the transport error, then an
                // explicit retry. Recovery must retain the committed receipt.
                remote.finish_job_attempt(old.clone(), JobStatus::Error("publish response lost".into())).await?;
                assert!(remote.table_ready(table_id, true).await.is_err());
                remote.recover_job(remote.get_job(old.job_id).await?, "task4-fixture-worker".into(), Duration::ZERO)
                    .await?.unwrap();
                let recovered = remote.start_processing_job("task4-fixture-worker".into(), JobRunnerPool::Regular)
                    .await?.unwrap().get_row().attempt().unwrap().clone();
                assert_ne!(recovered.generation, old.generation);
                timeout(Duration::from_secs(20), JOB_ATTEMPT.scope(Some(recovered.clone()),
                    importer.clone().import_table_part(table_id, &source, None))).await??;
                assert!(JOB_ATTEMPT.scope(Some(old.clone()), remote.publish_import_chunks(
                    old, table_id, source.clone(), Vec::new())).await.is_err());
                faults.drop_finish_response.store(true, Ordering::SeqCst);
                assert!(JOB_ATTEMPT.scope(Some(recovered.clone()), remote.finish_job_attempt(
                    recovered.clone(), JobStatus::Completed)).await.is_err());
                assert!(!faults.drop_finish_response.load(Ordering::SeqCst));
                assert_eq!(remote.get_job(recovered.job_id).await?.get_row().status(), &JobStatus::Completed);
                JOB_ATTEMPT.scope(Some(recovered.clone()), remote.finish_job_attempt(
                    recovered.clone(), JobStatus::Completed)).await?;
                assert!(remote.table_ready(table_id, true).await?.get_row().is_ready());
                let after = remote.get_active_partitions_and_chunks_by_index_id_for_select(vec![index]).await?;
                let mut after_ids = after[0].iter().flat_map(|(_, chunks)| chunks.iter().map(|c| c.get_id())).collect::<Vec<_>>();
                after_ids.sort_unstable();
                assert_eq!(before_ids, after_ids, "recovery must not publish a second set of chunks");
                let result = timeout(Duration::from_secs(20), async {
                    services.sql_service.exec_query("SELECT n FROM task4_rpc.rows ORDER BY n")
                        .await?.collect().await
                }).await??;
                assert_eq!(result.get_rows(), &vec![
                    Row::new(vec![TableValue::Int(11)]),
                    Row::new(vec![TableValue::Int(22)]),
                    Row::new(vec![TableValue::Int(33)]),
                ]);
                assert_eq!(remote.get_job(recovered.job_id).await?.get_row().completed_imports(), &[source]);
                drop(importer);
                drop(remote);
                drop(source_listener);
                // The running service container still owns the remote client;
                // keep its listener alive through this callback's last RPC.
                drop(rpc_listener);
                Ok(())
            },
        ).await;
}
