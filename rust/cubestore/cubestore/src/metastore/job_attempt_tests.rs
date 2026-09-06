use super::*;
use crate::cluster::message::NetworkMessage;

async fn claim(store: &RocksMetaStore, node: &str) -> IdRow<Job> {
    store.start_processing_job(node.to_string(), JobRunnerPool::Regular)
        .await.unwrap().unwrap()
}

async fn import_table(store: &RocksMetaStore) -> Result<IdRow<Table>, CubeError> {
    store.create_schema("s".to_string(), false).await?;
    store.create_table(
        "s".to_string(), "t".to_string(),
        vec![Column::new("n".to_string(), ColumnType::Int, 0)],
        Some(vec!["file.csv".to_string()]), None, vec![], false,
        None, None, None, None, None, None, None, None, None, false, None,
    ).await
}

async fn import_job(store: &RocksMetaStore, table_id: u64) -> IdRow<Job> {
    store.add_job(Job::new(
        RowKey::Table(TableId::Tables, table_id),
        JobType::TableImportCSV("file.csv".to_string()), "worker".to_string(),
    )).await.unwrap();
    claim(store, "worker").await
}

#[tokio::test]
async fn job_attempt_competing_claim_reclaim_heartbeat_timeout() -> Result<(), CubeError> {
    let name = "job_attempt_competing_claim_reclaim_heartbeat_timeout";
    let (_, store) = RocksMetaStore::prepare_test_metastore(name);
    let definition = Job::new(RowKey::Table(TableId::Partitions, 1), JobType::PartitionCompaction, "worker".to_string());
    store.add_job(definition.clone()).await?;
    let (a, b) = tokio::join!(
        store.start_processing_job("worker".to_string(), JobRunnerPool::Regular),
        store.start_processing_job("worker".to_string(), JobRunnerPool::Regular),
    );
    let (a, b) = (a?, b?);
    assert_ne!(a.is_some(), b.is_some(), "only one atomic claimant may win");
    let first = a.or(b).unwrap();
    let old = first.get_row().attempt().unwrap().clone();
    assert!(store.recover_job(first.clone(), "other".to_string(), Duration::from_secs(3600)).await?.is_none());
    store.heartbeat_job_attempt(old.clone()).await?;
    assert!(store.recover_job(first, "other".to_string(), Duration::ZERO).await?.is_none(), "a heartbeat after the sweep snapshot wins");
    let latest = store.get_job(old.job_id).await?;
    store.recover_job(latest, "worker".to_string(), Duration::ZERO).await?.unwrap();
    assert!(store.add_job(definition.clone()).await?.is_none(), "reclaim preserves the unique job key");
    let second = claim(&store, "worker").await;
    let current = second.get_row().attempt().unwrap().clone();
    assert_eq!(current.job_id, old.job_id);
    assert_eq!(current.generation, old.generation + 1);
    assert!(store.heartbeat_job_attempt(old.clone()).await.is_err());
    assert!(store.finish_job_attempt(old.clone(), JobStatus::Completed).await.is_err());
    assert!(store.update_heart_beat(old.job_id).await.is_err());
    assert!(store.update_status(old.job_id, JobStatus::Completed).await.is_err());
    assert!(store.delete_job(old.job_id).await.is_err());
    assert!(JOB_ATTEMPT.scope(Some(old), store.write_operation("stale_write", |_, _| Ok(()))).await.is_err());
    JOB_ATTEMPT.scope(Some(current.clone()), store.write_operation("owned_write", |_, _| Ok(()))).await?;
    let timed_out = store.finish_job_attempt(current.clone(), JobStatus::Timeout).await?;
    assert!(store.heartbeat_job_attempt(current).await.is_err());
    assert_eq!(store.get_job(timed_out.get_id()).await?.get_row().status(), &JobStatus::Timeout);
    store.recover_job(timed_out, "worker".to_string(), Duration::ZERO).await?.unwrap();
    let third = claim(&store, "worker").await;
    store.finish_job_attempt(third.get_row().attempt().unwrap().clone(), JobStatus::Completed).await?;
    assert!(store.add_job(definition).await?.is_some(), "non-import completion releases dedupe");
    drop(store);
    RocksMetaStore::cleanup_test_metastore(name);
    Ok(())
}

#[tokio::test]
async fn job_attempt_stale_upload_cannot_publish_and_receipt_survives_reclaim() -> Result<(), CubeError> {
    let name = "job_attempt_stale_upload_cannot_publish_and_receipt_survives_reclaim";
    let (_, store) = RocksMetaStore::prepare_test_metastore(name);
    let table = import_table(&store).await?;
    let table_id = table.get_id();
    let indexes = store.get_table_indexes(table_id).await?;
    let partitions = store.get_active_partitions_and_chunks_by_index_id_for_select(vec![indexes[0].get_id()]).await?;
    let partition_id = partitions[0][0].0.get_id();
    let first = import_job(&store, table_id).await;
    let old = first.get_row().attempt().unwrap().clone();
    let old_chunk = JOB_ATTEMPT.scope(Some(old.clone()), store.create_chunk(partition_id, 1, None, None, false)).await?;
    assert!(store.all_inactive_not_uploaded_chunks().await?.is_empty(), "live staged imports are protected from GC");
    // An older worker that ignores the new serde fields cannot publish raw CSV
    // batches under a new-format claim, even when its chunks carry no token.
    let untagged = store.create_chunk(partition_id, 1, None, None, false).await?;
    assert!(store.activate_chunks(table_id, vec![(untagged.get_id(), Some(10))], None).await.is_err());
    assert!(!store.get_chunk(untagged.get_id()).await?.get_row().active());
    store.recover_job(store.get_job(first.get_id()).await?, "worker".to_string(), Duration::ZERO).await?.unwrap();
    let second = claim(&store, "worker").await;
    let current = second.get_row().attempt().unwrap().clone();
    assert!(store.publish_import_chunks(old.clone(), table_id, "file.csv".to_string(), vec![(old_chunk.get_id(), Some(10))]).await.is_err());
    // Even an unscoped/legacy publisher cannot activate a stale owned output.
    assert!(store.activate_chunks(table_id, vec![(old_chunk.get_id(), Some(10))], None).await.is_err());
    assert!(!store.get_chunk(old_chunk.get_id()).await?.get_row().active());
    assert!(store.table_ready(table_id, true).await.is_err());

    // Exercise the wire payload and the RPC server, not only local prechecks.
    let wire = NetworkMessage::MetaStoreCallWithAttempt(old.clone(), MetaStoreRpcMethodCall::activateChunks(table_id, vec![(old_chunk.get_id(), Some(10))], None));
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    wire.serialize(&mut serializer).unwrap();
    let bytes = serializer.take_buffer();
    let decoded = NetworkMessage::deserialize(flexbuffers::Reader::get_root(bytes.as_slice()).unwrap()).unwrap();
    if let NetworkMessage::MetaStoreCallWithAttempt(attempt, call) = decoded {
        let server = MetaStoreRpcServer::new(store.clone());
        let result = JOB_ATTEMPT.scope(Some(attempt), server.invoke_method(call)).await;
        assert!(matches!(result, MetaStoreRpcMethodResult::activateChunks(Err(_))));
    } else { panic!("attempt envelope was lost"); }

    let new_chunk = JOB_ATTEMPT.scope(Some(current.clone()), store.create_chunk(partition_id, 1, None, None, false)).await?;
    store.publish_import_chunks(current.clone(), table_id, "file.csv".to_string(), vec![(new_chunk.get_id(), Some(10))]).await?;
    store.publish_import_chunks(current.clone(), table_id, "file.csv".to_string(), vec![(new_chunk.get_id(), Some(10))]).await?;
    assert!(store.get_chunk(new_chunk.get_id()).await?.get_row().active());
    assert!(!store.get_chunk(old_chunk.get_id()).await?.get_row().active());
    // A GC snapshot taken before publication must not delete the now-active chunk.
    store.delete_chunks_without_checks(vec![new_chunk.get_id()]).await?;
    assert!(store.get_chunk(new_chunk.get_id()).await?.get_row().active());
    assert!(JOB_ATTEMPT.scope(Some(old.clone()), store.table_ready(table_id, true)).await.is_err());
    assert!(JOB_ATTEMPT.scope(Some(old), store.swap_chunks_without_check(vec![new_chunk.get_id()], Vec::new(), None)).await.is_err());

    // Crash after the location commit but before job completion: a new attempt
    // reads the receipt and cannot append its own second copy.
    store.recover_job(store.get_job(second.get_id()).await?, "worker".to_string(), Duration::ZERO).await?.unwrap();
    let third = claim(&store, "worker").await;
    let recovered = third.get_row().attempt().unwrap().clone();
    assert_eq!(third.get_row().completed_imports(), &["file.csv".to_string()]);
    store.publish_import_chunks(recovered.clone(), table_id, "file.csv".to_string(), Vec::new()).await?;
    store.finish_job_attempt(recovered, JobStatus::Completed).await?;
    assert!(store.table_ready(table_id, true).await?.get_row().is_ready());
    assert_eq!(store.get_job(third.get_id()).await?.get_row().status(), &JobStatus::Completed);
    assert!(store.add_job(Job::new(RowKey::Table(TableId::Tables, table_id), JobType::TableImportCSV("file.csv".to_string()), "worker".to_string())).await?.is_none());
    drop(store);
    RocksMetaStore::cleanup_test_metastore(name);
    Ok(())
}

#[tokio::test]
async fn job_attempt_import_error_is_durable_and_explicit_retry_clears_it() -> Result<(), CubeError> {
    let name = "job_attempt_import_error_is_durable_and_explicit_retry_clears_it";
    let (_, store) = RocksMetaStore::prepare_test_metastore(name);
    let table = import_table(&store).await?;
    let first = import_job(&store, table.get_id()).await;
    let error = "invalid CSV".to_string();
    let failed = store.finish_job_attempt(first.get_row().attempt().unwrap().clone(), JobStatus::Error(error.clone())).await?;
    assert_eq!(store.get_table_by_id(table.get_id()).await?.get_row().import_error(), Some(&error));
    // A different file's claim must not erase this table's existing failure.
    store.add_job(Job::new(
        RowKey::Table(TableId::Tables, table.get_id()),
        JobType::TableImportCSV("second.csv".to_string()), "second-worker".to_string(),
    )).await?;
    let _other = claim(&store, "second-worker").await;
    assert_eq!(store.get_table_by_id(table.get_id()).await?.get_row().import_error(), Some(&error));
    assert!(store.table_ready(table.get_id(), true).await.is_err());
    assert!(store.get_orphaned_jobs(Duration::ZERO).await?.iter().all(|j| j.get_id() != failed.get_id()), "terminal file failures are not swept away");
    assert!(store.delete_job(failed.get_id()).await.is_err());
    store.recover_job(failed, "worker".to_string(), Duration::ZERO).await?.unwrap();
    let retry = claim(&store, "worker").await;
    assert!(store.get_table_by_id(table.get_id()).await?.get_row().import_error().is_none());
    let attempt = retry.get_row().attempt().unwrap().clone();
    assert!(store.finish_job_attempt(attempt.clone(), JobStatus::Completed).await.is_err());
    // Empty files also require and receive a durable result identity.
    store.publish_import_chunks(attempt.clone(), table.get_id(), "file.csv".to_string(), Vec::new()).await?;
    store.finish_job_attempt(attempt, JobStatus::Completed).await?;
    assert!(store.table_ready(table.get_id(), true).await?.get_row().is_ready());
    drop(store);
    RocksMetaStore::cleanup_test_metastore(name);
    Ok(())
}
