//! Strict HTTPS workload admission. Kubernetes validation is not atomic with a
//! RocksDB commit: grant replacement linearizes on the MetaStore writer. Reads
//! never install a grant; neither a grant ID nor a JobAttempt authenticates a Pod.
use super::{DbTableRef, MetaStoreRpcMethodCall, MetaStoreRpcMethodResult, MetaStoreRpcServer,
    RocksMetaStore, RowKey, TableId};
use crate::cluster::message::NetworkMessage;
use crate::config::Config;
use crate::metastore::job::{JobAttempt, JOB_ATTEMPT};
use crate::util::aborting_join_handle::AbortingJoinHandle;
use crate::CubeError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use warp::{Filter, Reply};

const AUDIENCE: &str = "cubestore-metastore-authority-v1";
const MAX_BODY: usize = 1024 * 1024;

fn error(code: &'static str) -> CubeError { CubeError::user(code.to_string()) }

struct Secret(String);
impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("[REDACTED]") }
}

pub fn strict_enabled() -> bool {
    std::env::var("CUBESTORE_AUTHORITY_STRICT")
        .map(|v| v != "false" && v != "0").unwrap_or(false)
}

fn setting(name: &str) -> Result<String, CubeError> {
    std::env::var(format!("CUBESTORE_AUTHORITY_{}", name))
        .ok().filter(|v| !v.is_empty()).ok_or_else(|| error("AUTHORITY_CONFIG_INVALID"))
}

fn duration_setting(name: &str, allow_zero: bool) -> Result<Duration, CubeError> {
    let n: u64 = setting(name)?.parse().map_err(|_| error("AUTHORITY_CONFIG_INVALID"))?;
    if (!allow_zero && n == 0) || n > i64::MAX as u64 {
        return Err(error("AUTHORITY_CONFIG_INVALID"));
    }
    Ok(Duration::from_millis(n))
}

fn secret_file(path: &str) -> Result<Secret, CubeError> {
    let value = std::fs::read_to_string(path).map_err(|_| error("AUTHENTICATION_REQUIRED"))?;
    let value = value.trim();
    if value.is_empty() || value.len() > 16384 || value.chars().any(char::is_whitespace) {
        return Err(error("AUTHENTICATION_REQUIRED"));
    }
    Ok(Secret(value.to_string()))
}

fn tls_client(url: &str, ca: &str, timeout: Duration) -> Result<reqwest::Client, CubeError> {
    let parsed = reqwest::Url::parse(url).map_err(|_| error("AUTHORITY_CONFIG_INVALID"))?;
    if parsed.scheme() != "https" || parsed.host_str().is_none() || !parsed.username().is_empty()
        || parsed.password().is_some() || parsed.query().is_some() || parsed.fragment().is_some()
    { return Err(error("AUTHORITY_CONFIG_INVALID")); }
    let pem = std::fs::read(ca).map_err(|_| error("AUTHORITY_CONFIG_INVALID"))?;
    let cert = reqwest::Certificate::from_pem(&pem).map_err(|_| error("AUTHORITY_CONFIG_INVALID"))?;
    reqwest::Client::builder().https_only(true).no_proxy()
        .redirect(reqwest::redirect::Policy::none()).timeout(timeout).connect_timeout(timeout)
        .tls_built_in_root_certs(false).add_root_certificate(cert).build()
        .map_err(|_| error("AUTHORITY_CONFIG_INVALID"))
}

async fn response_json(mut response: reqwest::Response) -> Result<Value, CubeError> {
    if !response.status().is_success() { return Err(error("AUTHORITY_REMOTE_REJECTED")); }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| error("AUTHORITY_UNAVAILABLE"))? {
        if body.len().saturating_add(chunk.len()) > MAX_BODY { return Err(error("AUTHORITY_RESPONSE_INVALID")); }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| error("AUTHORITY_RESPONSE_INVALID"))
}

#[derive(Clone, Copy, PartialEq)]
enum Role { Router, Worker }

struct Settings {
    cluster_uid: String,
    lease_cluster_id: String,
    namespace: String,
    lease_name: String,
    lease_uid: String,
    router_sa: String,
    worker_sa: String,
    k8s_url: String,
    k8s_token_file: String,
    api: reqwest::Client,
    validation_timeout: Duration,
    clock_skew: Duration,
}

impl Settings {
    fn from_env() -> Result<Self, CubeError> {
        if setting("TOKEN_AUDIENCE")? != AUDIENCE { return Err(error("AUTHORITY_CONFIG_INVALID")); }
        let api_timeout = duration_setting("API_TIMEOUT_MS", false)?;
        let validation_timeout = duration_setting("VALIDATION_TIMEOUT_MS", false)?;
        let clock_skew = duration_setting("MAX_CLOCK_SKEW_MS", true)?;
        let k8s_url = setting("K8S_API_URL")?.trim_end_matches('/').to_string();
        let api = tls_client(&k8s_url, &setting("K8S_CA_FILE")?, api_timeout)?;
        let value = Self {
            cluster_uid: setting("CLUSTER_UID")?,
            lease_cluster_id: setting("EXPECTED_LEASE_CLUSTER_ID")?,
            namespace: setting("LEASE_NAMESPACE")?, lease_name: setting("LEASE_NAME")?,
            lease_uid: setting("LEASE_UID")?, router_sa: setting("ROUTER_SERVICE_ACCOUNT")?,
            worker_sa: setting("WORKER_SERVICE_ACCOUNT")?, k8s_url,
            k8s_token_file: setting("K8S_TOKEN_FILE")?, api, validation_timeout, clock_skew,
        };
        if value.router_sa == value.worker_sa || [&value.namespace, &value.lease_name,
            &value.router_sa, &value.worker_sa].iter().any(|v| !v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.'))
        { return Err(error("AUTHORITY_CONFIG_INVALID")); }
        secret_file(&value.k8s_token_file)?;
        Ok(value)
    }
}

#[derive(Clone)]
struct Principal { role: Role, pod_name: String, pod_uid: String, until: Instant }

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Fence {
    cluster_uid: String, lease_cluster_id: String, lease_uid: String,
    lease_generation: u64, epoch: u64, holder_pod_name: String, holder_pod_uid: String,
    fence_digest: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallAck {
    protocol_version: u32,
    server_incarnation: String,
    grant_id: String,
    cluster_uid: String,
    lease_uid: String,
    holder_pod_name: String,
    holder_pod_uid: String,
    router_epoch: String,
    lease_generation: String,
    fence_digest: String,
    lease_resource_version: String,
    valid_for_ms: u64,
}

#[derive(Serialize, Deserialize)]
struct DurableGrant { fence: Fence, ack: InstallAck }

#[derive(Clone)]
pub(crate) enum RequestContext {
    Install { state: Arc<State>, fence: Fence, until: Instant, revision: u64 },
    Router { state: Arc<State>, fence: Fence, grant_id: String, until: Instant, revision: u64 },
    Worker { state: Arc<State>, principal: Principal },
}

tokio::task_local! { static REQUEST: Option<RequestContext>; }

pub(crate) fn current_request() -> Option<RequestContext> {
    REQUEST.try_with(Clone::clone).ok().flatten()
}

pub(crate) fn current_worker_uid() -> Option<String> {
    match current_request() { Some(RequestContext::Worker { principal, .. }) => Some(principal.pod_uid), _ => None }
}

pub(crate) fn check_worker_identity(bound_uid: Option<&str>) -> Result<(), CubeError> {
    match (bound_uid, current_worker_uid()) {
        (Some(expected), Some(actual)) if expected == actual => Ok(()),
        (None, None) => Ok(()), // Standalone/legacy jobs outside strict authenticated admission.
        _ => Err(error("IDENTITY_REJECTED")),
    }
}

lazy_static! {
    static ref AUTHORITIES: Mutex<HashMap<usize, Weak<State>>> = Mutex::new(HashMap::new());
    static ref CLIENT: Mutex<Option<Arc<HttpsClient>>> = Mutex::new(None);
}

pub(crate) struct State {
    settings: Settings,
    incarnation: String,
    revoked: AtomicU64,
    grants: Mutex<HashMap<String, (Instant, u64)>>,
    install_lock: AsyncMutex<()>,
    #[cfg(test)]
    pause: Mutex<Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>>,
}

pub(crate) fn registered(key: usize) -> Option<Arc<State>> {
    AUTHORITIES.lock().ok().and_then(|m| m.get(&key).and_then(Weak::upgrade))
}

fn record_key() -> Vec<u8> { RowKey::Table(TableId::RouterAuthority, 1).to_bytes() }

fn read_record(db: &DbTableRef<'_>) -> Result<Option<DurableGrant>, CubeError> {
    db.snapshot.get(record_key())?.map(|v| serde_json::from_slice(&v)
        .map_err(|_| error("AUTHORITY_STATE_INVALID"))).transpose()
}

pub(crate) fn with_write<T>(
    state: Option<&Arc<State>>, request: Option<RequestContext>, db: &DbTableRef<'_>,
    operation: &str, write: impl FnOnce() -> Result<T, CubeError>,
) -> Result<T, CubeError> {
    let state = match state {
        Some(state) => state,
        None if !strict_enabled() => return write(),
        None => return Err(error("AUTHORITY_NOT_READY")),
    };
    let context = request.ok_or_else(|| error("AUTHENTICATION_REQUIRED"))?;
    match &context {
        RequestContext::Install { state: source, until, revision, .. } => {
            if operation != "authority_install" || !Arc::ptr_eq(state, source)
                || Instant::now() >= *until || state.revoked.load(Ordering::SeqCst) != *revision
            { return Err(error("GRANT_STALE")); }
        }
        RequestContext::Router { state: source, fence, grant_id, until, revision } => {
            let record = read_record(db)?.ok_or_else(|| error("GRANT_REQUIRED"))?;
            let grants = state.grants.lock().map_err(|_| error("AUTHORITY_NOT_READY"))?;
            let valid = grants.get(grant_id).map(|(deadline, rev)| Instant::now() < *deadline && rev == revision).unwrap_or(false);
            if !Arc::ptr_eq(state, source) || Instant::now() >= *until || !valid
                || state.revoked.load(Ordering::SeqCst) != *revision
                || record.ack.server_incarnation != state.incarnation
                || record.ack.grant_id != *grant_id || record.fence != *fence
            { return Err(error("GRANT_STALE")); }
        }
        RequestContext::Worker { state: source, principal } => {
            if !Arc::ptr_eq(state, source) || Instant::now() >= principal.until {
                return Err(error("IDENTITY_REJECTED"));
            }
            // No Router epoch is consulted for authenticated workers. Unknown
            // mutation paths are denied rather than silently becoming a bypass.
            if !matches!(operation, "start_processing_job" | "heartbeat_job_attempt" | "finish_job_attempt"
                | "create_chunk" | "activate_chunks" | "publish_import_chunks" | "update_location_download_size"
                | "swap_chunks" | "swap_chunks_without_check" | "swap_compacted_chunks" | "swap_active_partitions")
            { return Err(error("IDENTITY_REJECTED")); }
            if operation != "start_processing_job" && crate::metastore::job::current_job_attempt().is_none() {
                // The HTTP request context is restored around the RPC, and the
                // captured attempt is restored by the existing writer below.
                // Explicit heartbeat/completion calls are checked by their methods.
                if !matches!(operation, "heartbeat_job_attempt" | "finish_job_attempt") {
                    return Err(error("JOB_ATTEMPT_REQUIRED"));
                }
            }
        }
    }
    REQUEST.sync_scope(Some(context), write)
}

impl State {
    fn attach(store: &Arc<RocksMetaStore>, settings: Settings) -> Arc<Self> {
        let state = Arc::new(Self {
            settings, incarnation: uuid::Uuid::new_v4().to_string(), revoked: AtomicU64::new(0),
            grants: Mutex::new(HashMap::new()), install_lock: AsyncMutex::new(()),
            #[cfg(test)] pause: Mutex::new(None),
        });
        AUTHORITIES.lock().unwrap().insert(Arc::as_ptr(&store.store) as usize, Arc::downgrade(&state));
        state
    }

    async fn api(&self, path: &str, body: Option<Value>) -> Result<Value, CubeError> {
        let token = secret_file(&self.settings.k8s_token_file)?;
        let url = format!("{}{}", self.settings.k8s_url, path);
        let request = match body { Some(body) => self.settings.api.post(url).json(&body), None => self.settings.api.get(url) };
        let response = request.bearer_auth(&token.0).send().await.map_err(|_| error("LEASE_UNAVAILABLE"))?;
        response_json(response).await.map_err(|_| error("LEASE_UNAVAILABLE"))
    }

    async fn authenticate(&self, header: Option<String>) -> Result<Principal, CubeError> {
        let start = Instant::now();
        let header = Secret(header.ok_or_else(|| error("AUTHENTICATION_REQUIRED"))?);
        let token = header.0.strip_prefix("Bearer ").filter(|v| !v.is_empty() && v.len() <= 16384
            && !v.chars().any(char::is_whitespace)).ok_or_else(|| error("AUTHENTICATION_REQUIRED"))?;
        let review = self.api("/apis/authentication.k8s.io/v1/tokenreviews", Some(json!({
            "apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
            "spec": { "token": token, "audiences": [AUDIENCE] }
        }))).await?;
        let status = &review["status"];
        if status["authenticated"].as_bool() != Some(true)
            || !status["audiences"].as_array().map(|a| a.iter().any(|v| v.as_str() == Some(AUDIENCE))).unwrap_or(false)
            || status["user"]["uid"].as_str().unwrap_or("").is_empty()
        { return Err(error("IDENTITY_REJECTED")); }
        let username = status["user"]["username"].as_str().unwrap_or("");
        let router = format!("system:serviceaccount:{}:{}", self.settings.namespace, self.settings.router_sa);
        let worker = format!("system:serviceaccount:{}:{}", self.settings.namespace, self.settings.worker_sa);
        let (role, sa) = if username == router { (Role::Router, &self.settings.router_sa) }
            else if username == worker { (Role::Worker, &self.settings.worker_sa) }
            else { return Err(error("IDENTITY_REJECTED")); };
        let extra = &status["user"]["extra"];
        let one = |key: &str| -> Result<String, CubeError> {
            let values = extra[key].as_array().ok_or_else(|| error("IDENTITY_REJECTED"))?;
            if values.len() != 1 { return Err(error("IDENTITY_REJECTED")); }
            values[0].as_str().filter(|v| !v.is_empty()).map(str::to_string).ok_or_else(|| error("IDENTITY_REJECTED"))
        };
        let pod_name = one("authentication.kubernetes.io/pod-name")?;
        let pod_uid = one("authentication.kubernetes.io/pod-uid")?;
        if !pod_name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.') {
            return Err(error("IDENTITY_REJECTED"));
        }
        let pod = self.api(&format!("/api/v1/namespaces/{}/pods/{}", self.settings.namespace, pod_name), None).await?;
        if pod["metadata"]["name"].as_str() != Some(&pod_name)
            || pod["metadata"]["namespace"].as_str() != Some(&self.settings.namespace)
            || pod["metadata"]["uid"].as_str() != Some(&pod_uid)
            || !pod["metadata"]["deletionTimestamp"].is_null()
            || pod["spec"]["serviceAccountName"].as_str() != Some(sa)
        { return Err(error("IDENTITY_REJECTED")); }
        let until = start.checked_add(self.settings.validation_timeout).ok_or_else(|| error("AUTHORITY_CONFIG_INVALID"))?;
        if Instant::now() >= until { return Err(error("IDENTITY_REJECTED")); }
        Ok(Principal { role, pod_name, pod_uid, until })
    }

    async fn lease(&self, principal: &Principal) -> Result<(Fence, Instant, String), CubeError> {
        let result = self.lease_inner(principal).await;
        if result.is_err() { self.revoked.fetch_add(1, Ordering::SeqCst); }
        result
    }

    async fn lease_inner(&self, principal: &Principal) -> Result<(Fence, Instant, String), CubeError> {
        let lease = self.api(&format!("/apis/coordination.k8s.io/v1/namespaces/{}/leases/{}",
            self.settings.namespace, self.settings.lease_name), None).await?;
        let metadata = &lease["metadata"];
        let annotations = &metadata["annotations"];
        let spec = &lease["spec"];
        if principal.role != Role::Router || metadata["uid"].as_str() != Some(&self.settings.lease_uid)
            || metadata["name"].as_str() != Some(&self.settings.lease_name)
            || metadata["namespace"].as_str() != Some(&self.settings.namespace)
            || !metadata["deletionTimestamp"].is_null()
            || annotations["cubejs.io/lease-cluster-id"].as_str() != Some(&self.settings.lease_cluster_id)
            || spec["holderIdentity"].as_str() != Some(&principal.pod_name)
        { return Err(error("LEASE_INVALID")); }
        let bound_uid = annotations["cubejs.io/lease-holder-uid"].as_str().unwrap_or("");
        if !bound_uid.is_empty() && bound_uid != principal.pod_uid { return Err(error("IDENTITY_REJECTED")); }
        let generation: u64 = annotations["cubejs.io/lease-generation"].as_str()
            .ok_or_else(|| error("LEASE_INVALID"))?.parse().map_err(|_| error("LEASE_INVALID"))?;
        let epoch = spec["leaseTransitions"].as_u64().ok_or_else(|| error("LEASE_INVALID"))?;
        let token = Secret(annotations["cubejs.io/lease-token"].as_str().filter(|v| !v.is_empty())
            .ok_or_else(|| error("LEASE_INVALID"))?.to_string());
        let renew = DateTime::parse_from_rfc3339(spec["renewTime"].as_str().ok_or_else(|| error("LEASE_INVALID"))?)
            .map_err(|_| error("LEASE_INVALID"))?.with_timezone(&Utc);
        let acquire = DateTime::parse_from_rfc3339(spec["acquireTime"].as_str().ok_or_else(|| error("LEASE_INVALID"))?)
            .map_err(|_| error("LEASE_INVALID"))?.with_timezone(&Utc);
        let seconds = spec["leaseDurationSeconds"].as_i64().filter(|v| *v > 0).ok_or_else(|| error("LEASE_INVALID"))?;
        let skew = chrono::Duration::from_std(self.settings.clock_skew).map_err(|_| error("AUTHORITY_CONFIG_INVALID"))?;
        let now = Utc::now();
        let duration = chrono::Duration::try_seconds(seconds).ok_or_else(|| error("LEASE_INVALID"))?;
        let expires = renew.checked_add_signed(duration).and_then(|v| v.checked_sub_signed(skew)).ok_or_else(|| error("LEASE_INVALID"))?;
        if acquire > renew || renew > now.checked_add_signed(skew).ok_or_else(|| error("LEASE_INVALID"))? || expires <= now {
            return Err(error("LEASE_INVALID"));
        }
        let remaining = (expires - now).to_std().map_err(|_| error("LEASE_INVALID"))?;
        let until = Instant::now().checked_add(remaining).ok_or_else(|| error("LEASE_INVALID"))?.min(principal.until);
        let mut hash = Sha256::new();
        // v1 canonical digest: each UTF-8 field has an unsigned u64 BE byte length.
        for part in ["cubestore-router-fence-v1", &self.settings.cluster_uid, &self.settings.lease_cluster_id,
            &self.settings.lease_uid, &generation.to_string(), &epoch.to_string(),
            &principal.pod_name, &principal.pod_uid, &token.0] {
            hash.update((part.as_bytes().len() as u64).to_be_bytes()); hash.update(part.as_bytes());
        }
        Ok((Fence { cluster_uid: self.settings.cluster_uid.clone(), lease_cluster_id: self.settings.lease_cluster_id.clone(),
            lease_uid: self.settings.lease_uid.clone(), lease_generation: generation, epoch,
            holder_pod_name: principal.pod_name.clone(), holder_pod_uid: principal.pod_uid.clone(),
            fence_digest: format!("sha256:{}", hex::encode(hash.finalize())),
        }, until, metadata["resourceVersion"].as_str().ok_or_else(|| error("LEASE_INVALID"))?.to_string()))
    }
}

impl RocksMetaStore {
    async fn install_authority(self: &Arc<Self>, state: Arc<State>, fence: Fence, until: Instant, rv: String) -> Result<InstallAck, CubeError> {
        let revision = state.revoked.load(Ordering::SeqCst);
        let context = RequestContext::Install { state: state.clone(), fence: fence.clone(), until, revision };
        let incarnation = state.incarnation.clone();
        let ack = REQUEST.scope(Some(context), self.write_operation("authority_install", move |db, pipe| {
            let old = read_record(&db)?;
            if let Some(ref old) = old {
                if old.fence.cluster_uid != fence.cluster_uid || old.fence.lease_cluster_id != fence.lease_cluster_id
                    || old.fence.lease_uid != fence.lease_uid || fence.epoch < old.fence.epoch
                    || fence.lease_generation < old.fence.lease_generation
                    || (fence.epoch == old.fence.epoch && old.fence.holder_pod_uid != fence.holder_pod_uid)
                { return Err(error("AUTHORITY_STATE_CONFLICT")); }
            }
            let grant_id = old.as_ref().filter(|old| old.fence == fence && old.ack.server_incarnation == incarnation)
                .map(|old| old.ack.grant_id.clone()).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let ack = InstallAck {
                protocol_version: 1, server_incarnation: incarnation, grant_id,
                cluster_uid: fence.cluster_uid.clone(), lease_uid: fence.lease_uid.clone(),
                holder_pod_name: fence.holder_pod_name.clone(), holder_pod_uid: fence.holder_pod_uid.clone(),
                router_epoch: fence.epoch.to_string(), lease_generation: fence.lease_generation.to_string(),
                fence_digest: fence.fence_digest.clone(), lease_resource_version: rv,
                valid_for_ms: until.saturating_duration_since(Instant::now()).as_millis() as u64,
            };
            let bytes = serde_json::to_vec(&DurableGrant { fence, ack: ack.clone() }).map_err(|_| error("AUTHORITY_STATE_INVALID"))?;
            pipe.batch().put(record_key(), bytes);
            Ok(ack)
        })).await?;
        // The caller holds install_lock through commit and validity publication.
        // Only the durable current grant can pass the writer; failed installs
        // must leave the previous grant and its deadline untouched.
        let mut grants = state.grants.lock().map_err(|_| error("AUTHORITY_NOT_READY"))?;
        grants.retain(|id, _| id == &ack.grant_id);
        grants.insert(ack.grant_id.clone(), (until, revision));
        Ok(ack)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InstallRequest { protocol_version: u32 }

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RpcEnvelope {
    protocol_version: u32,
    server_incarnation: Option<String>,
    grant_id: Option<String>,
    request_id: String,
    attempt: Option<JobAttempt>,
    call: MetaStoreRpcMethodCall,
}

#[derive(Deserialize, Serialize)]
struct RpcReply { result: MetaStoreRpcMethodResult }

fn method_name(call: &MetaStoreRpcMethodCall) -> Result<String, CubeError> {
    let value = serde_json::to_value(call).map_err(|_| error("AUTHORITY_REQUEST_INVALID"))?;
    value.as_str().map(str::to_string).or_else(|| value.as_object().and_then(|v| v.keys().next().cloned()))
        .ok_or_else(|| error("AUTHORITY_REQUEST_INVALID"))
}

fn is_read(call: &MetaStoreRpcMethodCall) -> bool {
    method_name(call).map(|name| ["get", "all", "find", "is", "can", "list"].iter().any(|p| name.starts_with(p))
        || matches!(name.as_str(), "notReadyTables" | "waitForCurrentSeqToSync")).unwrap_or(false)
}

fn http_error(err: CubeError) -> warp::reply::Response {
    let code = match err.message.as_str() {
        "AUTHENTICATION_REQUIRED" => "AUTHENTICATION_REQUIRED", "IDENTITY_REJECTED" => "IDENTITY_REJECTED",
        "LEASE_UNAVAILABLE" => "LEASE_UNAVAILABLE", "LEASE_INVALID" => "LEASE_INVALID",
        "GRANT_REQUIRED" => "GRANT_REQUIRED", "GRANT_STALE" => "GRANT_STALE",
        "AUTHORITY_STATE_CONFLICT" => "AUTHORITY_STATE_CONFLICT", _ => "AUTHORITY_NOT_READY",
    };
    let status = match code {
        "AUTHENTICATION_REQUIRED" => warp::http::StatusCode::UNAUTHORIZED,
        "IDENTITY_REJECTED" => warp::http::StatusCode::FORBIDDEN,
        "GRANT_STALE" | "GRANT_REQUIRED" | "AUTHORITY_STATE_CONFLICT" => warp::http::StatusCode::CONFLICT,
        _ => warp::http::StatusCode::SERVICE_UNAVAILABLE,
    };
    warp::reply::with_status(warp::reply::json(&json!({"code":code})), status).into_response()
}

async fn install_handler(state: Arc<State>, store: Arc<RocksMetaStore>, header: Option<String>, body: InstallRequest) -> Result<warp::reply::Response, Infallible> {
    let result = tokio::time::timeout(state.settings.validation_timeout, async {
        if body.protocol_version != 1 { return Err(error("AUTHORITY_REQUEST_INVALID")); }
        let principal = state.authenticate(header).await?;
        if principal.role != Role::Router { return Err(error("IDENTITY_REJECTED")); }
        // Serialize authoritative reads plus installation. Same-epoch token
        // rotation cannot be undone by an older overlapping install observation.
        let _lock = state.install_lock.lock().await;
        let (fence, until, rv) = state.lease(&principal).await?;
        store.install_authority(state.clone(), fence, until, rv).await
    }).await.unwrap_or_else(|_| Err(error("LEASE_UNAVAILABLE")));
    Ok(match result { Ok(ack) => warp::reply::json(&ack).into_response(), Err(err) => http_error(err) })
}

async fn rpc_handler(state: Arc<State>, store: Arc<RocksMetaStore>, header: Option<String>, mut body: RpcEnvelope) -> Result<warp::reply::Response, Infallible> {
    let result = async {
        if body.protocol_version != 1 || body.request_id.is_empty() || body.request_id.len() > 128 {
            return Err(error("AUTHORITY_REQUEST_INVALID"));
        }
        let context = tokio::time::timeout(state.settings.validation_timeout, async {
            let principal = state.authenticate(header).await?;
            match principal.role {
                Role::Worker => Ok(Some(RequestContext::Worker { state: state.clone(), principal })),
                Role::Router if is_read(&body.call) && body.attempt.is_none() => Ok(None),
                Role::Router => {
                    if body.attempt.is_some() { return Err(error("IDENTITY_REJECTED")); }
                    if body.server_incarnation.as_deref() != Some(state.incarnation.as_str()) { return Err(error("GRANT_STALE")); }
                    let revision = state.revoked.load(Ordering::SeqCst);
                    let (fence, until, _) = state.lease(&principal).await?;
                    Ok(Some(RequestContext::Router { state: state.clone(), fence,
                        grant_id: body.grant_id.clone().ok_or_else(|| error("GRANT_REQUIRED"))?, until, revision }))
                }
            }
        }).await.map_err(|_| error("LEASE_UNAVAILABLE"))??;
        let explicit = match &body.call {
            MetaStoreRpcMethodCall::heartbeatJobAttempt(a) | MetaStoreRpcMethodCall::finishJobAttempt(a, _)
                | MetaStoreRpcMethodCall::publishImportChunks(a, ..) => Some(a.clone()), _ => None,
        };
        if let Some(explicit) = explicit {
            if body.attempt.as_ref().map(|a| a != &explicit).unwrap_or(false)
                || !matches!(context, Some(RequestContext::Worker { .. })) { return Err(error("IDENTITY_REJECTED")); }
            body.attempt = Some(explicit);
        }
        #[cfg(test)]
        {
            let pause = state.pause.lock().unwrap().take();
            if let Some((arrived, release)) = pause {
                let _ = arrived.send(());
                tokio::time::timeout(Duration::from_secs(10), release).await.map_err(|_| error("AUTHORITY_NOT_READY"))?
                    .map_err(|_| error("AUTHORITY_NOT_READY"))?;
            }
        }
        let server = MetaStoreRpcServer::new(store);
        let result = REQUEST.scope(context, JOB_ATTEMPT.scope(body.attempt, server.invoke_method(body.call))).await;
        Ok(RpcReply { result })
    }.await;
    Ok(match result { Ok(reply) => warp::reply::json(&reply).into_response(), Err(err) => http_error(err) })
}

pub struct AuthorityServer {
    _state: Arc<State>,
    _task: AbortingJoinHandle<()>,
    stop: CancellationToken,
}
impl Drop for AuthorityServer { fn drop(&mut self) { self.stop.cancel(); } }

fn listen(state: Arc<State>, store: Arc<RocksMetaStore>, address: SocketAddr, cert: String, key: String) -> (SocketAddr, AuthorityServer) {
    let install_state = state.clone(); let install_store = store.clone();
    let install = warp::path!("v1" / "router-authority" / "install").and(warp::post())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::body::content_length_limit(MAX_BODY as u64)).and(warp::body::json())
        .and_then(move |header, body| install_handler(install_state.clone(), install_store.clone(), header, body));
    let rpc_state = state.clone();
    let rpc = warp::path!("v1" / "metastore" / "rpc").and(warp::post())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::body::content_length_limit(MAX_BODY as u64)).and(warp::body::json())
        .and_then(move |header, body| rpc_handler(rpc_state.clone(), store.clone(), header, body));
    let stop = CancellationToken::new(); let stopping = stop.clone();
    let (bound, future) = warp::serve(install.or(rpc)).tls().cert_path(cert).key_path(key)
        .bind_with_graceful_shutdown(address, async move { stopping.cancelled().await });
    (bound, AuthorityServer { _state: state, _task: AbortingJoinHandle::new(tokio::spawn(future)), stop })
}

struct HttpsClient {
    role: Role, url: String, token_file: String, http: reqwest::Client,
    grant: AsyncMutex<Option<InstallAck>>,
}

impl HttpsClient {
    fn from_env(role: Role) -> Result<Arc<Self>, CubeError> {
        if setting("TOKEN_AUDIENCE")? != AUDIENCE { return Err(error("AUTHORITY_CONFIG_INVALID")); }
        let url = setting("URL")?.trim_end_matches('/').to_string();
        let timeout = duration_setting("VALIDATION_TIMEOUT_MS", false)?;
        duration_setting("API_TIMEOUT_MS", false)?;
        duration_setting("MAX_CLOCK_SKEW_MS", true)?;
        let http = tls_client(&url, &setting("CA_FILE")?, timeout)?;
        let token_file = setting("TOKEN_FILE")?; secret_file(&token_file)?;
        Ok(Arc::new(Self { role, url, token_file, http, grant: AsyncMutex::new(None) }))
    }

    async fn post<T: Serialize>(&self, path: &str, body: &T) -> Result<Value, CubeError> {
        let token = secret_file(&self.token_file)?;
        let response = self.http.post(format!("{}{}", self.url, path)).bearer_auth(&token.0).json(body)
            .send().await.map_err(|_| error("AUTHORITY_UNAVAILABLE"))?;
        response_json(response).await
    }

    async fn call(&self, message: NetworkMessage) -> Result<NetworkMessage, CubeError> {
        let (attempt, call) = match message {
            NetworkMessage::MetaStoreCall(call) => (None, call),
            NetworkMessage::MetaStoreCallWithAttempt(attempt, call) => (Some(attempt), call),
            _ => return Err(error("AUTHORITY_REQUEST_INVALID")),
        };
        let ack = if self.role == Role::Router && !is_read(&call) {
            let mut grant = self.grant.lock().await;
            // Re-install is idempotent for the same full fence and refreshes the
            // short validity window. No mutation is automatically retried.
            let value = self.post("/v1/router-authority/install", &InstallRequest { protocol_version: 1 }).await?;
            let ack: InstallAck = serde_json::from_value(value).map_err(|_| error("AUTHORITY_RESPONSE_INVALID"))?;
            *grant = Some(ack.clone()); Some(ack)
        } else { None };
        let envelope = RpcEnvelope { protocol_version: 1,
            server_incarnation: ack.as_ref().map(|a| a.server_incarnation.clone()),
            grant_id: ack.map(|a| a.grant_id), request_id: uuid::Uuid::new_v4().to_string(), attempt, call };
        let reply: RpcReply = serde_json::from_value(self.post("/v1/metastore/rpc", &envelope).await?)
            .map_err(|_| error("AUTHORITY_RESPONSE_INVALID"))?;
        Ok(NetworkMessage::MetaStoreCallResult(reply.result))
    }
}

pub async fn https_call(message: NetworkMessage) -> Result<NetworkMessage, CubeError> {
    let client = CLIENT.lock().map_err(|_| error("AUTHORITY_NOT_READY"))?.clone()
        .ok_or_else(|| error("AUTHORITY_NOT_READY"))?;
    client.call(message).await
}

pub async fn start(config: &Config) -> Result<Option<AuthorityServer>, CubeError> {
    if !strict_enabled() { return Ok(None); }
    if !matches!(std::env::var("CUBESTORE_AUTHORITY_STRICT").as_deref(), Ok("true") | Ok("1")) {
        return Err(error("AUTHORITY_CONFIG_INVALID"));
    }
    match setting("ROLE")?.as_str() {
        "metastore" => {
            let settings = Settings::from_env()?;
            let cert = setting("TLS_CERT_FILE")?; let key = setting("TLS_KEY_FILE")?;
            std::fs::read(&cert).map_err(|_| error("AUTHORITY_CONFIG_INVALID"))?;
            std::fs::read(&key).map_err(|_| error("AUTHORITY_CONFIG_INVALID"))?;
            let address: SocketAddr = setting("BIND_ADDR")?.parse().map_err(|_| error("AUTHORITY_CONFIG_INVALID"))?;
            let store = config.injector().get_service_typed::<RocksMetaStore>().await;
            let state = State::attach(&store, settings);
            Ok(Some(listen(state, store, address, cert, key).1))
        }
        "router" | "worker" => {
            let role = if setting("ROLE")? == "router" { Role::Router } else { Role::Worker };
            *CLIENT.lock().map_err(|_| error("AUTHORITY_NOT_READY"))? = Some(HttpsClient::from_env(role)?);
            Ok(None)
        }
        _ => Err(error("AUTHORITY_CONFIG_INVALID")),
    }
}

#[cfg(test)]
#[path = "authority_tests.rs"]
mod tests;
