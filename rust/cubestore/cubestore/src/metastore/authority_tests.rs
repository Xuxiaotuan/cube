use super::*;
use crate::metastore::{MetaStore, Column, ColumnType};
use crate::metastore::job::{Job, JobRunnerPool, JobType};
use std::path::Path;
use std::process::Command;

fn openssl(directory: &Path, arguments: &[&str]) {
    let result = Command::new("openssl").current_dir(directory).args(arguments).output().unwrap();
    assert!(result.status.success(), "test PKI generation failed");
}

fn pki() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    openssl(dir.path(), &["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.crt", "-days", "1", "-subj", "/CN=task4-test-ca", "-addext", "basicConstraints=critical,CA:TRUE"]);
    openssl(dir.path(), &["req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", "tls.key", "-out", "tls.csr", "-subj", "/CN=localhost"]);
    std::fs::write(dir.path().join("extensions"), "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n").unwrap();
    openssl(dir.path(), &["x509", "-req", "-in", "tls.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial", "-out", "tls.crt", "-days", "1", "-extfile", "extensions"]);
    dir
}

#[derive(Clone)]
struct ApiState { holder: String, epoch: u64, token: String, unavailable: bool }

fn settings(api_url: String, dir: &Path) -> Settings {
    let ca = dir.join("ca.crt").to_string_lossy().to_string();
    let token = dir.join("api.token"); std::fs::write(&token, "api-test-token").unwrap();
    Settings { cluster_uid: "cube-uid".into(), lease_cluster_id: "cube/router".into(), namespace: "test".into(),
        lease_name: "router".into(), lease_uid: "lease-uid".into(), router_sa: "router-sa".into(), worker_sa: "worker-sa".into(),
        api: tls_client(&api_url, &ca, Duration::from_secs(2)).unwrap(), k8s_url: api_url,
        k8s_token_file: token.to_string_lossy().to_string(), validation_timeout: Duration::from_secs(10), clock_skew: Duration::ZERO }
}

async fn post(http: &reqwest::Client, url: &str, path: &str, token: Option<&str>, body: Value) -> (u16, Value) {
    let request = http.post(format!("{}{}", url, path)).json(&body);
    let request = if let Some(token) = token { request.bearer_auth(token) } else { request };
    let response = request.send().await.unwrap();
    (response.status().as_u16(), response.json().await.unwrap())
}

fn envelope(call: MetaStoreRpcMethodCall, ack: Option<&Value>, attempt: Option<JobAttempt>) -> Value {
    serde_json::to_value(RpcEnvelope { protocol_version: 1,
        server_incarnation: ack.and_then(|v| v["serverIncarnation"].as_str().map(str::to_string)),
        grant_id: ack.and_then(|v| v["grantId"].as_str().map(str::to_string)),
        request_id: uuid::Uuid::new_v4().to_string(), call, attempt }).unwrap()
}

#[test]
fn authority_secret_debug_is_redacted() {
    assert_eq!(format!("{:?}", Secret("must-not-appear".into())), "[REDACTED]");
}

#[tokio::test]
async fn authority_https_identity_grant_and_worker_matrix() -> Result<(), CubeError> {
    let dir = pki();
    let api_state = Arc::new(Mutex::new(ApiState { holder: "router-a".into(), epoch: 20, token: "first-token".into(), unavailable: false }));
    let lease_state = api_state.clone();
    let lease = warp::path!("apis" / "coordination.k8s.io" / "v1" / "namespaces" / String / "leases" / String)
        .map(move |_: String, _: String| {
            let state = lease_state.lock().unwrap().clone();
            if state.unavailable { return warp::reply::with_status(warp::reply::json(&json!({})), warp::http::StatusCode::SERVICE_UNAVAILABLE); }
            let now = Utc::now().to_rfc3339();
            warp::reply::with_status(warp::reply::json(&json!({"metadata": {"name":"router","namespace":"test","uid":"lease-uid","resourceVersion":"opaque-rv",
                "annotations":{"cubejs.io/lease-cluster-id":"cube/router","cubejs.io/lease-generation":"1","cubejs.io/lease-holder-uid":"","cubejs.io/lease-token":state.token}},
                "spec":{"holderIdentity":state.holder,"leaseTransitions":state.epoch,"renewTime":now,"acquireTime":now,"leaseDurationSeconds":30}})), warp::http::StatusCode::OK)
        });
    let reviews = warp::path!("apis" / "authentication.k8s.io" / "v1" / "tokenreviews")
        .and(warp::post()).and(warp::body::json()).map(|body: Value| {
            let token = body["spec"]["token"].as_str().unwrap_or("");
            let authenticated = matches!(token, "router-a" | "router-b" | "worker");
            let sa = if token == "worker" { "worker-sa" } else { "router-sa" };
            warp::reply::json(&json!({"status":{"authenticated":authenticated,"audiences":[AUDIENCE],
                "user":{"uid":"sa-uid","username":format!("system:serviceaccount:test:{}",sa),"extra":{
                    "authentication.kubernetes.io/pod-name":[token],"authentication.kubernetes.io/pod-uid":[format!("{}-uid",token)]}}}}))
        });
    let pods = warp::path!("api" / "v1" / "namespaces" / String / "pods" / String).map(|_: String, pod: String| {
        let sa = if pod == "worker" { "worker-sa" } else { "router-sa" };
        warp::reply::json(&json!({"metadata":{"name":pod,"namespace":"test","uid":format!("{}-uid",pod)},"spec":{"serviceAccountName":sa}}))
    });
    let cert = dir.path().join("tls.crt").to_string_lossy().to_string();
    let key = dir.path().join("tls.key").to_string_lossy().to_string();
    let (api_addr, api_future) = warp::serve(reviews.or(pods).or(lease)).tls().cert_path(&cert).key_path(&key).bind_ephemeral(([127,0,0,1],0));
    let api_task = AbortingJoinHandle::new(tokio::spawn(api_future));
    let api_url = format!("https://{}", api_addr);
    let name = "authority_https_identity_grant_and_worker_matrix";
    let (_, store) = RocksMetaStore::prepare_test_metastore(name);
    store.create_schema("input".into(), false).await?;
    let table = store.create_table("input".into(), "rows".into(), vec![Column::new("n".into(),ColumnType::Int,0)],
        Some(vec!["file.csv".into()]), None, vec![], false, None,None,None,None,None,None,None,None,None,false,None).await?;
    let index = store.get_table_indexes(table.get_id()).await?[0].get_id();
    let partition = store.get_active_partitions_by_index_id(index).await?[0].get_id();
    store.add_job(Job::new(RowKey::Table(TableId::Tables,table.get_id()),JobType::TableImportCSV("file.csv".into()),"worker".into())).await?;
    let state = State::attach(&store, settings(api_url.clone(), dir.path()));
    let (address, listener) = listen(state.clone(), store.clone(), "127.0.0.1:0".parse().unwrap(), cert.clone(), key.clone());
    let url = format!("https://{}", address);
    let http = tls_client(&url, &dir.path().join("ca.crt").to_string_lossy(), Duration::from_secs(10))?;
    let install = json!({"protocolVersion":1});
    assert_eq!(post(&http,&url,"/v1/router-authority/install",None,install.clone()).await.0,401);
    assert_eq!(post(&http,&url,"/v1/router-authority/install",Some("forged"),install.clone()).await.0,403);
    assert_eq!(post(&http,&url,"/v1/router-authority/install",Some("worker"),install.clone()).await.0,403);
    let (status, ack_a) = post(&http,&url,"/v1/router-authority/install",Some("router-a"),install.clone()).await;
    assert_eq!(status,200);
    let (_, same) = post(&http,&url,"/v1/router-authority/install",Some("router-a"),install.clone()).await;
    assert_eq!(ack_a["grantId"],same["grantId"]);
    assert_eq!(ack_a["routerEpoch"],"20"); assert_eq!(ack_a["leaseGeneration"],"1");
    assert!(!ack_a.to_string().contains("first-token"));
    let (_, good) = post(&http,&url,"/v1/metastore/rpc",Some("router-a"),envelope(MetaStoreRpcMethodCall::createSchema("authorized".into(),false),Some(&ack_a),None)).await;
    assert!(good["result"]["createSchema"]["Ok"].is_object(),"{}",good);
    // Plain/legacy invocation reaches the same real server but has no validated
    // HTTPS context, so no bearer or forged attempt can bypass the writer.
    let legacy = MetaStoreRpcServer::new(store.clone()).invoke_method(MetaStoreRpcMethodCall::createSchema("legacy".into(),false)).await;
    assert!(matches!(legacy,MetaStoreRpcMethodResult::createSchema(Err(_))));
    let (arrived_tx,arrived_rx) = tokio::sync::oneshot::channel();
    let (release_tx,release_rx) = tokio::sync::oneshot::channel();
    *state.pause.lock().unwrap() = Some((arrived_tx,release_rx));
    let old_http=http.clone(); let old_url=url.clone(); let old_ack=ack_a.clone();
    let pending = AbortingJoinHandle::new(tokio::spawn(async move {
        post(&old_http,&old_url,"/v1/metastore/rpc",Some("router-a"),envelope(MetaStoreRpcMethodCall::createSchema("stale".into(),false),Some(&old_ack),None)).await
    }));
    tokio::time::timeout(Duration::from_secs(5),arrived_rx).await.unwrap().unwrap();
    { let mut api=api_state.lock().unwrap(); api.holder="router-b".into(); api.epoch=21; api.token="second-token".into(); }
    let (status,ack_b)=post(&http,&url,"/v1/router-authority/install",Some("router-b"),install.clone()).await;
    assert_eq!(status,200); assert_ne!(ack_a["grantId"],ack_b["grantId"]);
    release_tx.send(()).unwrap();
    let (_,rejected)=pending.await?;
    assert!(rejected.to_string().contains("GRANT_STALE"),"{}",rejected);
    assert!(store.get_schema("stale".into()).await.is_err());
    api_state.lock().unwrap().unavailable=true;
    assert_eq!(post(&http,&url,"/v1/metastore/rpc",Some("router-b"),envelope(MetaStoreRpcMethodCall::createSchema("offline".into(),false),Some(&ack_b),None)).await.0,503);
    api_state.lock().unwrap().unavailable=false;
    let (_,claim)=post(&http,&url,"/v1/metastore/rpc",Some("worker"),envelope(MetaStoreRpcMethodCall::startProcessingJob("worker".into(),JobRunnerPool::Regular),None,None)).await;
    let claim: RpcReply=serde_json::from_value(claim).unwrap();
    let attempt=match claim.result { MetaStoreRpcMethodResult::startProcessingJob(Ok(Some(job)))=>job.get_row().attempt().unwrap().clone(), _=>panic!("worker claim failed") };
    let (_,chunk)=post(&http,&url,"/v1/metastore/rpc",Some("worker"),envelope(MetaStoreRpcMethodCall::createChunk(partition,3,None,None,false),None,Some(attempt.clone()))).await;
    let chunk: RpcReply=serde_json::from_value(chunk).unwrap();
    let chunk=match chunk.result { MetaStoreRpcMethodResult::createChunk(Ok(chunk))=>chunk, _=>panic!("worker staging failed") };
    let (_,publication)=post(&http,&url,"/v1/metastore/rpc",Some("worker"),envelope(MetaStoreRpcMethodCall::publishImportChunks(attempt.clone(),table.get_id(),"file.csv".into(),vec![(chunk.get_id(),Some(10))]),None,Some(attempt.clone()))).await;
    let publication:RpcReply=serde_json::from_value(publication).unwrap();
    assert!(matches!(publication.result,MetaStoreRpcMethodResult::publishImportChunks(Ok(()))));
    assert!(store.get_chunk(chunk.get_id()).await?.get_row().active());
    // New process incarnation retains the same durable epoch high-water mark.
    drop(listener);
    let restarted=State::attach(&store,settings(api_url,dir.path()));
    let (address,new_listener)=listen(restarted,store.clone(),"127.0.0.1:0".parse().unwrap(),cert,key);
    let new_url=format!("https://{}",address);
    assert_eq!(post(&http,&new_url,"/v1/metastore/rpc",Some("router-b"),envelope(MetaStoreRpcMethodCall::createSchema("restart-old".into(),false),Some(&ack_b),None)).await.0,409);
    { let mut api=api_state.lock().unwrap(); api.holder="router-a".into(); api.epoch=20; api.token="first-token".into(); }
    assert_eq!(post(&http,&new_url,"/v1/router-authority/install",Some("router-a"),install).await.0,409);
    drop(new_listener); drop(state); drop(store); drop(api_task);
    tokio::task::yield_now().await;
    RocksMetaStore::cleanup_test_metastore(name);
    Ok(())
}
