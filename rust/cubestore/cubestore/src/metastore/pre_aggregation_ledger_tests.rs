use super::*;
use super::pre_aggregation_ledger::*;

fn atomic_create(generation: &str) -> LedgerCommand {
    LedgerCommand::Create {
        key: "orders".into(), generation: generation.into(), owner: "a".into(),
        create: LedgerCreate {
            schema: "s".into(), table: "orders_content_structure_123".into(),
            columns: vec![LedgerCreateColumn { name: "n".into(), column_type: "int".into() }],
            locations: vec!["file.csv".into()], indexes: vec![], import_format: "csv".into(),
            content_version: "content".into(), structure_version: "structure".into(),
            aggregations: None, build_range_end: None, delimiter: None, disable_quoting: None,
        },
    }
}

async fn complete_atomic_import(store: &RocksMetaStore, table_id: u64) {
    store.add_job(Job::new(RowKey::Table(TableId::Tables, table_id),
        JobType::TableImportCSV("file.csv".into()), "atomic-create-worker".into())).await.unwrap();
    let job = store.start_processing_job("atomic-create-worker".into(), JobRunnerPool::Regular).await.unwrap().unwrap();
    let attempt = job.get_row().attempt().unwrap().clone();
    store.publish_import_chunks(attempt.clone(), table_id, "file.csv".into(), vec![]).await.unwrap();
    store.finish_job_attempt(attempt, JobStatus::Completed).await.unwrap();
    // Do not forge or finalize ready: Publish must do that atomically itself.
}

#[tokio::test]
async fn pre_aggregation_ledger_atomic_create_replay_bind_preserves_provenance() {
    let (_dir, store, _, _) = setup("atomic_create_rebind");
    store.create_schema("s".into(), false).await.unwrap();
    let claimed = ok(&store, "claim", claim("a", None, "60000")).await;
    let req = request("create", atomic_create(&claimed.generation));
    let (first, replay) = tokio::join!(
        store.mutate_pre_aggregation_ledger(req.clone()),
        store.mutate_pre_aggregation_ledger(req.clone()),
    );
    let first = first.unwrap();
    assert_eq!(first, replay.unwrap());
    assert_eq!(first.rejection, None);
    let created = first.record.as_ref().unwrap();
    assert_eq!(created.state, LedgerState::Bound);
    let manifest = created.manifest.clone().unwrap();
    let table_id = manifest.table_id.parse().unwrap();
    assert!(!store.get_table_by_id(table_id).await.unwrap().get_row().is_ready());
    assert!(store.drop_table(table_id).await.is_err());
    assert!(store.tables_table().delete(table_id).await.is_err());
    assert_eq!(store.mutate_pre_aggregation_ledger(request("premature", publish(&claimed.generation, "a"))).await.unwrap().rejection.as_deref(), Some("LEDGER_IMPORT_RECEIPT_MISSING"));
    ok(&store, "rebind", LedgerCommand::Bind { key: created.key.clone(), generation: created.generation.clone(), owner: "a".into(), manifest }).await;
    complete_atomic_import(&store, table_id).await;
    assert!(!store.get_table_by_id(table_id).await.unwrap().get_row().is_ready());
    assert_eq!(ok(&store, "publish", publish(&claimed.generation, "a")).await.state, LedgerState::Published);
    assert!(store.get_table_by_id(table_id).await.unwrap().get_row().is_ready());
    assert_eq!(store.mutate_pre_aggregation_ledger(req).await.unwrap(), first);
}

#[tokio::test]
async fn pre_aggregation_ledger_atomic_create_schema_rejection_is_durable() {
    let (dir, store, fs, config) = setup("atomic_create_schema_replay");
    let claimed = ok(&store, "claim", claim("a", None, "60000")).await;
    let req = request("create-missing-schema", atomic_create(&claimed.generation));
    let rejected = store.mutate_pre_aggregation_ledger(req.clone()).await.unwrap();
    assert_eq!(rejected.rejection.as_deref(), Some("LEDGER_SCHEMA_NOT_FOUND"));
    assert_eq!(store.get_pre_aggregation_ledger("orders".into(), None).await.unwrap().unwrap(), claimed);
    store.create_schema("s".into(), false).await.unwrap();
    let db = store.store.db.clone();
    store.stop_processing_loops().await;
    drop(store);
    tokio::time::timeout(Duration::from_secs(10), async {
        while Arc::strong_count(&db) != 1 { tokio::task::yield_now().await; }
    }).await.expect("database still owned before reopen");
    drop(Arc::try_unwrap(db).unwrap_or_else(|_| panic!("database still open")));
    let store = RocksMetaStore::new(&dir.path().join("db"), fs, config).unwrap();
    assert_eq!(store.mutate_pre_aggregation_ledger(req).await.unwrap(), rejected);
    assert_eq!(store.get_pre_aggregation_ledger("orders".into(), None).await.unwrap().unwrap(), claimed);
    let created = ok(&store, "create-after-schema", atomic_create(&claimed.generation)).await;
    assert_eq!(created.state, LedgerState::Bound);
    let old_owner = match atomic_create(&claimed.generation) {
        LedgerCommand::Create { key, generation, create, .. } => LedgerCommand::Create { key, generation, owner: "old".into(), create },
        _ => unreachable!(),
    };
    assert_eq!(store.mutate_pre_aggregation_ledger(request("old-owner", old_owner)).await.unwrap().rejection.as_deref(), Some("LEDGER_OWNER_MISMATCH"));
}

#[tokio::test]
async fn pre_aggregation_ledger_query_refs_atomic_set_idempotency_and_retire() {
    let (_dir, store, _, _) = setup("atomic_query_set");
    let bound = bound(&store, true).await;
    let published = ok(&store, "publish", publish(&bound.generation, "a")).await;
    let id = published.manifest.as_ref().unwrap().table_id.clone();
    assert!(store.acquire_pre_aggregation_query_refs("partial".into(), vec![id.clone(), u64::MAX.to_string()]).await.is_err());
    assert_eq!(store.get_pre_aggregation_ledger("orders".into(), None).await.unwrap().unwrap().references, "0");
    let refs = store.acquire_pre_aggregation_query_refs("query".into(), vec![id.clone(), id.clone()]).await.unwrap();
    assert_eq!(refs.table_ids, vec![id.clone()]);
    assert_eq!(refs.bindings.len(), 1);
    assert_eq!(store.acquire_pre_aggregation_query_refs("query".into(), vec![id.clone()]).await.unwrap(), refs);
    assert_eq!(store.get_pre_aggregation_ledger("orders".into(), None).await.unwrap().unwrap().references, "1");
    assert_eq!(store.mutate_pre_aggregation_ledger(request("blocked", retire(&published.generation))).await.unwrap().rejection.as_deref(), Some("LEDGER_REFERENCED"));
    let released = store.release_pre_aggregation_query_refs("query".into()).await.unwrap();
    assert!(released.released);
    assert_eq!(store.release_pre_aggregation_query_refs("query".into()).await.unwrap(), released);
    assert!(store.acquire_pre_aggregation_query_refs("query".into(), vec![id.clone()]).await.is_err());
    store.release_pre_aggregation_query_refs("cancel-before-acquire".into()).await.unwrap();
    assert!(store.acquire_pre_aggregation_query_refs("cancel-before-acquire".into(), vec![id.clone()]).await.is_err());
    let (pin, retired) = tokio::join!(
        store.acquire_pre_aggregation_query_refs("racing".into(), vec![id]),
        store.mutate_pre_aggregation_ledger(request("race-retire", retire(&published.generation))),
    );
    assert_ne!(pin.is_ok(), retired.unwrap().record.is_some());
}

fn request(id: &str, command: LedgerCommand) -> LedgerRequest {
    LedgerRequest { request_id: id.into(), command }
}

fn claim(owner: &str, expected: Option<String>, lease: &str) -> LedgerCommand {
    LedgerCommand::Claim { key: "orders".into(), owner: owner.into(), lease_millis: lease.into(), expected_generation: expected }
}

fn setup(name: &str) -> (tempfile::TempDir, Arc<RocksMetaStore>, Arc<dyn MetaStoreFs>, Arc<dyn ConfigObj>) {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::test(name).config_obj();
    let remote = LocalDirRemoteFs::new(Some(dir.path().join("remote")), dir.path().join("cache"));
    let fs: Arc<dyn MetaStoreFs> = BaseRocksStoreFs::new_for_metastore(remote, config.clone());
    let store = RocksMetaStore::new(&dir.path().join("db"), fs.clone(), config.clone()).unwrap();
    (dir, store, fs, config)
}

async fn ok(store: &RocksMetaStore, id: &str, command: LedgerCommand) -> LedgerRecord {
    let result = store.mutate_pre_aggregation_ledger(request(id, command)).await.unwrap();
    assert_eq!(result.rejection, None);
    result.record.unwrap()
}

async fn table(store: &RocksMetaStore, ready: bool) -> LedgerManifest {
    store.create_schema("s".into(), false).await.unwrap();
    let table = store.create_table("s".into(), "orders_content_structure_123".into(),
        vec![Column::new("n".into(), ColumnType::Int, 0)], Some(vec!["file.csv".into()]),
        None, vec![], false, None, None, None, None, None, None, None, None, None, false, None,
    ).await.unwrap();
    if ready { ready_table(store, table.get_id()).await; }
    LedgerManifest { table_id: table.get_id().to_string(), schema: "s".into(),
        table: "orders_content_structure_123".into(), locations: vec!["file.csv".into()],
        content_version: "content".into(), structure_version: "structure".into() }
}

async fn ready_table(store: &RocksMetaStore, table_id: u64) {
    store.add_job(Job::new(RowKey::Table(TableId::Tables, table_id),
        JobType::TableImportCSV("file.csv".into()), "ledger-worker".into())).await.unwrap();
    let job = store.start_processing_job("ledger-worker".into(), JobRunnerPool::Regular).await.unwrap().unwrap();
    let attempt = job.get_row().attempt().unwrap().clone();
    store.publish_import_chunks(attempt.clone(), table_id, "file.csv".into(), vec![]).await.unwrap();
    store.finish_job_attempt(attempt, JobStatus::Completed).await.unwrap();
    store.table_ready(table_id, true).await.unwrap();
}

async fn bound(store: &RocksMetaStore, ready: bool) -> LedgerRecord {
    let manifest = table(store, ready).await;
    let record = ok(store, "claim", claim("a", None, "60000")).await;
    ok(store, "bind", LedgerCommand::Bind { key: record.key, generation: record.generation,
        owner: "a".into(), manifest }).await
}

fn publish(g: &str, owner: &str) -> LedgerCommand {
    LedgerCommand::Publish { key: "orders".into(), generation: g.into(), owner: owner.into() }
}
fn retire(g: &str) -> LedgerCommand {
    LedgerCommand::Retire { key: "orders".into(), generation: g.into(), owner: "a".into() }
}
fn acquire(g: &str, id: &str) -> LedgerCommand {
    LedgerCommand::Acquire { key: "orders".into(), generation: g.into(), reference_id: id.into() }
}
fn release(g: &str, id: &str) -> LedgerCommand {
    LedgerCommand::Release { key: "orders".into(), generation: g.into(), reference_id: id.into() }
}

#[tokio::test]
async fn pre_aggregation_ledger_concurrent_claim_and_durable_idempotency() {
    let (_dir, store, _, _) = setup("atomic_claim");
    let a = request("a", claim("a", None, "60000"));
    let b = request("b", claim("b", None, "60000"));
    let (ra, rb) = tokio::join!(store.mutate_pre_aggregation_ledger(a.clone()), store.mutate_pre_aggregation_ledger(b.clone()));
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    assert_ne!(ra.record.is_some(), rb.record.is_some());
    assert_eq!(store.mutate_pre_aggregation_ledger(a).await.unwrap(), ra);
    assert_eq!(store.mutate_pre_aggregation_ledger(b).await.unwrap(), rb);
    assert!(store.mutate_pre_aggregation_ledger(request("a", claim("different", None, "60000"))).await.is_err());
    let current = store.get_pre_aggregation_ledger("orders".into(), None).await.unwrap().unwrap();
    assert_eq!(current.generation, "1");
    let call = MetaStoreRpcMethodCall::getPreAggregationLedger("orders".into(), None);
    let wire = serde_json::to_value(&call).unwrap();
    assert_eq!(wire, serde_json::json!({"getPreAggregationLedger":["orders",null]}));
    let server = MetaStoreRpcServer::new(store.clone());
    assert!(matches!(server.invoke_method(call).await, MetaStoreRpcMethodResult::getPreAggregationLedger(Ok(Some(_)))));
    let wire_request = request("wire", claim("wire", None, "60000"));
    let wire = MetaStoreRpcMethodCall::mutatePreAggregationLedger(wire_request.clone());
    assert_eq!(serde_json::to_value(wire).unwrap(), serde_json::json!({"mutatePreAggregationLedger":wire_request}));
}

#[tokio::test]
async fn pre_aggregation_ledger_old_owner_generation_and_immutable_manifest() {
    let (_dir, store, _, _) = setup("atomic_owner");
    let manifest = table(&store, true).await;
    let old = ok(&store, "old", claim("a", None, "1")).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let next = ok(&store, "next", claim("b", Some(old.generation.clone()), "60000")).await;
    assert_eq!(next.generation, "2");
    assert!(store.mutate_pre_aggregation_ledger(request("old-publish", publish(&old.generation, "a"))).await.unwrap().rejection.is_some());
    assert_eq!(store.mutate_pre_aggregation_ledger(request("wrong-owner", publish(&next.generation, "a"))).await.unwrap().rejection.as_deref(), Some("LEDGER_OWNER_MISMATCH"));
    let bound = ok(&store, "bind", LedgerCommand::Bind { key: next.key, generation: next.generation.clone(), owner: "b".into(), manifest: manifest.clone() }).await;
    let mut changed = manifest; changed.locations = vec!["other.csv".into()];
    assert_eq!(store.mutate_pre_aggregation_ledger(request("rebind", LedgerCommand::Bind { key: bound.key, generation: bound.generation, owner: "b".into(), manifest: changed })).await.unwrap().rejection.as_deref(), Some("LEDGER_MANIFEST_IMMUTABLE"));
    assert_eq!(store.mutate_pre_aggregation_ledger(request("old", claim("a", None, "1"))).await.unwrap().record.unwrap(), old);
}

#[tokio::test]
async fn pre_aggregation_ledger_publish_checks_table_receipts_and_rejection_replay() {
    let (_dir, store, _, _) = setup("atomic_ready");
    let record = bound(&store, false).await;
    let command = request("publish-not-ready", publish(&record.generation, "a"));
    let rejected = store.mutate_pre_aggregation_ledger(command.clone()).await.unwrap();
    assert_eq!(rejected.rejection.as_deref(), Some("LEDGER_TABLE_NOT_READY"));
    let table_id = record.manifest.unwrap().table_id.parse().unwrap();
    // Even a forged ready metadata flag without receipts cannot publish.
    store.write_operation("test_ready_flag", move |db, pipe| {
        TableRocksTable::new(db).update_with_fn(table_id, |r| r.update_is_ready(true), pipe)
    }).await.unwrap();
    assert_eq!(store.mutate_pre_aggregation_ledger(request("forged-ready", publish(&record.generation, "a"))).await.unwrap().rejection.as_deref(), Some("LEDGER_IMPORT_RECEIPT_MISSING"));
    ready_table(&store, table_id).await;
    assert_eq!(store.mutate_pre_aggregation_ledger(command).await.unwrap(), rejected);
    assert_eq!(ok(&store, "publish-ready", publish(&record.generation, "a")).await.state, LedgerState::Published);
}

#[tokio::test]
async fn pre_aggregation_ledger_publish_retire_race() {
    let (_dir, store, _, _) = setup("atomic_publish_retire");
    let record = bound(&store, true).await;
    let (publish_result, retire_result) = tokio::join!(
        store.mutate_pre_aggregation_ledger(request("publish", publish(&record.generation, "a"))),
        store.mutate_pre_aggregation_ledger(request("retire", retire(&record.generation))),
    );
    let published = publish_result.unwrap();
    assert_eq!(retire_result.unwrap().record.unwrap().state, LedgerState::Retired);
    assert!(published.record.is_some() || published.rejection.as_deref() == Some("LEDGER_INVALID_STATE"));
    assert_eq!(store.get_pre_aggregation_ledger("orders".into(), None).await.unwrap().unwrap().state, LedgerState::Retired);
    assert!(store.mutate_pre_aggregation_ledger(request("late-publish", publish(&record.generation, "a"))).await.unwrap().rejection.is_some());
}

#[tokio::test]
async fn pre_aggregation_ledger_reference_retire_race_and_atomic_drop() {
    let (_dir, store, _, _) = setup("atomic_reference_retire");
    let bound = bound(&store, true).await;
    let record = ok(&store, "publish", publish(&bound.generation, "a")).await;
    let table_id = record.manifest.as_ref().unwrap().table_id.parse().unwrap();
    assert!(store.drop_table(table_id).await.is_err());
    assert!(store.tables_table().delete(table_id).await.is_err());
    let (a, r) = tokio::join!(
        store.mutate_pre_aggregation_ledger(request("acquire", acquire(&record.generation, "q"))),
        store.mutate_pre_aggregation_ledger(request("retire", retire(&record.generation))),
    );
    let (a, r) = (a.unwrap(), r.unwrap());
    assert_ne!(a.record.is_some(), r.record.is_some());
    if a.record.is_some() {
        assert_eq!(r.rejection.as_deref(), Some("LEDGER_REFERENCED"));
        ok(&store, "release", release(&record.generation, "q")).await;
        ok(&store, "retire-again", retire(&record.generation)).await;
    }
    assert!(store.drop_table(table_id).await.is_err(), "retire must not create an unprotected SQL drop window");
    let drop_request = request("drop", LedgerCommand::Drop { key: "orders".into(), generation: record.generation, owner: "a".into() });
    let dropped = store.mutate_pre_aggregation_ledger(drop_request.clone()).await.unwrap();
    assert_eq!(dropped.record.as_ref().unwrap().state, LedgerState::Dropped);
    assert!(store.get_table_by_id(table_id).await.is_err());
    assert_eq!(store.mutate_pre_aggregation_ledger(drop_request).await.unwrap(), dropped);
}

#[tokio::test]
async fn pre_aggregation_ledger_reference_tombstone_and_reopen() {
    let (dir, store, fs, config) = setup("atomic_reopen");
    let bound = bound(&store, true).await;
    let record = ok(&store, "publish", publish(&bound.generation, "a")).await;
    ok(&store, "release-first", release(&record.generation, "cancelled")).await;
    assert_eq!(store.mutate_pre_aggregation_ledger(request("late-acquire", acquire(&record.generation, "cancelled"))).await.unwrap().rejection.as_deref(), Some("LEDGER_REFERENCE_RELEASED"));
    let held = ok(&store, "acquire", acquire(&record.generation, "running")).await;
    assert_eq!(held.references, "1");
    let db = store.store.db.clone();
    store.stop_processing_loops().await;
    drop(store);
    tokio::time::timeout(Duration::from_secs(10), async {
        while Arc::strong_count(&db) != 1 { tokio::task::yield_now().await; }
    }).await.expect("database still owned before reopen");
    drop(Arc::try_unwrap(db).unwrap_or_else(|_| panic!("database still open")));
    let store = RocksMetaStore::new(&dir.path().join("db"), fs, config).unwrap();
    assert_eq!(store.get_pre_aggregation_ledger("orders".into(), None).await.unwrap().unwrap(), held);
    assert_eq!(ok(&store, "acquire", acquire(&record.generation, "running")).await, held);
    assert_eq!(store.mutate_pre_aggregation_ledger(request("retire-blocked", retire(&record.generation))).await.unwrap().rejection.as_deref(), Some("LEDGER_REFERENCED"));
    ok(&store, "release", release(&record.generation, "running")).await;
    let retired = ok(&store, "retire", retire(&record.generation)).await;
    assert_eq!(retired.references, "0");
    let next = ok(&store, "next", claim("b", Some(record.generation), "60000")).await;
    assert_eq!(next.generation, "2");
}
