use super::*;
use crate::cluster::message::NetworkMessage;

#[tokio::test]
async fn job_attempt_router_child_retains_fence_after_parent_cancellation() -> Result<(), CubeError> {
    use crate::metastore::job::spawn_job;
    use crate::sql::ha::MutationGate;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let name = "job_attempt_router_child_retains_fence_after_parent_cancellation";
    let (_, store) = RocksMetaStore::prepare_test_metastore(name);
    let epoch = Arc::new(AtomicUsize::new(1));
    let observed = epoch.clone();
    let gate = MutationGate::with_checker(true, Arc::new(move || {
        Ok(observed.load(Ordering::SeqCst).to_string())
    }));
    let guard = gate.begin()?;
    let child_store = store.clone();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let (child_tx, child_rx) = tokio::sync::oneshot::channel();
    let parent = tokio::spawn(async move {
        guard.run(async move {
            let child = spawn_job(async move {
                release_rx.await.unwrap();
                child_store.create_schema("late".to_string(), false).await
            });
            child_tx.send(child).unwrap();
            std::future::pending::<Result<(), CubeError>>().await
        }).await
    });
    let child = tokio::time::timeout(Duration::from_secs(2), child_rx).await.unwrap().unwrap();
    parent.abort();
    assert!(parent.await.unwrap_err().is_cancelled());
    assert_eq!(gate.drain(Duration::ZERO).await.in_flight, 1,
        "a detached child must retain the router admission permit");
    epoch.store(2, Ordering::SeqCst);
    release_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), child).await.unwrap().unwrap().is_err());
    // A failed publication must not have created a schema, not merely returned an error.
    store.create_schema("late".to_string(), false).await?;
    assert!(gate.drain(Duration::ZERO).await.drained);
    drop(store);
    RocksMetaStore::cleanup_test_metastore(name);
    Ok(())
}

#[tokio::test]
async fn job_attempt_healthy_child_survives_epoch_and_unknown_completion_response() -> Result<(), CubeError> {
    use crate::metastore::job::spawn_job;
    use crate::sql::ha::MutationGate;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let name = "job_attempt_healthy_child_survives_epoch_and_unknown_completion_response";
    let (_, store) = RocksMetaStore::prepare_test_metastore(name);
    let table = import_table(&store).await?;
    let table_id = table.get_id();
    let indexes = store.get_table_indexes(table_id).await?;
    let partitions = store.get_active_partitions_and_chunks_by_index_id_for_select(vec![indexes[0].get_id()]).await?;
    let partition_id = partitions[0][0].0.get_id();
    let claimed = import_job(&store, table_id).await;
    let attempt = claimed.get_row().attempt().unwrap().clone();
    let chunk = JOB_ATTEMPT.scope(Some(attempt.clone()), store.create_chunk(partition_id, 7, None, None, false)).await?;
    let chunk_id = chunk.get_id();
    let epoch = Arc::new(AtomicUsize::new(1));
    let observed = epoch.clone();
    let gate = MutationGate::with_checker(true, Arc::new(move || {
        Ok(observed.load(Ordering::SeqCst).to_string())
    }));
    let guard = gate.begin()?;
    let child_store = store.clone();
    let child_attempt = attempt.clone();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let child = guard.run(JOB_ATTEMPT.scope(Some(attempt.clone()), async move {
        Ok(spawn_job(async move {
            release_rx.await.unwrap();
            assert!(crate::sql::ha::current_mutation().is_none());
            // Simulate lost publication and completion responses by discarding
            // their values and retrying under the actual RPC attempt scope.
            child_store.publish_import_chunks(child_attempt.clone(), table_id, "file.csv".to_string(), vec![(chunk_id, Some(10))]).await?;
            child_store.publish_import_chunks(child_attempt.clone(), table_id, "file.csv".to_string(), vec![(chunk_id, Some(10))]).await?;
            child_store.finish_job_attempt(child_attempt.clone(), JobStatus::Completed).await?;
            child_store.finish_job_attempt(child_attempt, JobStatus::Completed).await?;
            Ok::<_, CubeError>(())
        }))
    })).await?;
    drop(guard);
    epoch.store(2, Ordering::SeqCst);
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), child).await.unwrap().unwrap()?;
    let visible = store.get_active_partitions_and_chunks_by_index_id_for_select(vec![indexes[0].get_id()]).await?;
    assert_eq!(visible[0].iter().map(|(_, chunks)| chunks.len()).sum::<usize>(), 1);
    assert!(store.get_chunk(chunk_id).await?.get_row().active());
    let completed = store.get_job(attempt.job_id).await?;
    assert_eq!(completed.get_row().completed_imports(), &["file.csv".to_string()]);
    assert_eq!(completed.get_row().status(), &JobStatus::Completed);
    assert!(JOB_ATTEMPT.scope(Some(attempt), store.create_schema("after_completion".to_string(), false)).await.is_err());
    drop(store);
    RocksMetaStore::cleanup_test_metastore(name);
    Ok(())
}

#[tokio::test]
async fn job_attempt_detached_old_child_cannot_publish_after_reclaim() -> Result<(), CubeError> {
    let name = "job_attempt_detached_old_child_cannot_publish_after_reclaim";
    let (_, store) = RocksMetaStore::prepare_test_metastore(name);
    let table = import_table(&store).await?;
    let table_id = table.get_id();
    let indexes = store.get_table_indexes(table_id).await?;
    let partitions = store.get_active_partitions_and_chunks_by_index_id_for_select(vec![indexes[0].get_id()]).await?;
    let partition_id = partitions[0][0].0.get_id();
    let claimed = import_job(&store, table_id).await;
    let old = claimed.get_row().attempt().unwrap().clone();
    let chunk = JOB_ATTEMPT.scope(Some(old.clone()), store.create_chunk(partition_id, 3, None, None, false)).await?;
    let chunk_id = chunk.get_id();
    let child_store = store.clone();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let child = JOB_ATTEMPT.scope(Some(old.clone()), async move {
        crate::metastore::job::spawn_job(async move {
            release_rx.await.unwrap();
            // Use the ambient token, not an explicit publish token, to exercise
            // the generic final-visibility fence inherited by detached children.
            child_store.swap_chunks_without_check(Vec::new(), vec![(chunk_id, Some(10))], None).await
        })
    }).await;
    store.recover_job(store.get_job(old.job_id).await?, "worker".to_string(), Duration::ZERO).await?.unwrap();
    let current = claim(&store, "worker").await;
    assert_ne!(current.get_row().attempt(), Some(&old));
    release_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), child).await.unwrap().unwrap().is_err());
    assert!(!store.get_chunk(chunk_id).await?.get_row().active());
    assert!(store.get_job(old.job_id).await?.get_row().completed_imports().is_empty());
    drop(store);
    RocksMetaStore::cleanup_test_metastore(name);
    Ok(())
}

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
