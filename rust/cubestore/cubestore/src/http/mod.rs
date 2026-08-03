pub mod status;

use std::sync::Arc;

use warp::{Filter, Rejection, Reply};

use crate::metastore::{Column, ColumnType, ImportFormat};
use crate::mysql::SqlAuthService;
use crate::sql::{
    InlineTable, InlineTables, QueryParameter, QueryParameters, SqlQueryContext, SqlService,
};
use crate::store::DataFrame;
use crate::table::{Row, TableValue};
use crate::util::WorkerLoop;
use crate::{app_metrics, CubeError};
use async_std::fs::File;
use cubeshared::codegen::{
    root_as_http_message, HttpColumnValue, HttpColumnValueArgs, HttpError, HttpErrorArgs,
    HttpMessageArgs, HttpParameterValue, HttpQuery, HttpQueryArgs, HttpQueryResult,
    HttpQueryResultArgs, HttpQueryResultArrow, HttpQueryResultArrowArgs, HttpQueryResultCompleted,
    HttpQueryResultCompletedArgs, HttpQueryResultData, HttpResultSet, HttpResultSetArgs, HttpRow,
    HttpRowArgs, QueryResultFormat,
};
use cubeshared::flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use datafusion::cube_ext;
use futures::{AsyncWriteExt, SinkExt, Stream, StreamExt};
use futures_timer::Delay;
use hex::ToHex;
use http_auth_basic::Credentials;
use log::error;
use log::info;
use log::trace;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::NamedTempFile;
use tokio::io::BufReader;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use warp::filters::ws::{Message, Ws};
use warp::http::StatusCode;
use warp::reject::Reject;

pub struct HttpServer {
    bind_address: String,
    leadership_file: String,
    promotion_file: String,
    sql_service: Arc<dyn SqlService>,
    auth: Arc<dyn SqlAuthService>,
    check_orphaned_messages_interval: Duration,
    drop_processing_messages_after: Duration,
    drop_complete_messages_after: Duration,
    worker_loop: WorkerLoop,
    drop_orphaned_messages_loop: WorkerLoop,
    cancel_token: CancellationToken,
    max_message_size: usize,
    max_frame_size: usize,
}

crate::di_service!(HttpServer, []);

#[derive(Debug)]
pub enum CubeRejection {
    NotAuthorized,
    Internal(String),
    NotLeader,
    LeaseFenced(String),
}

impl From<CubeError> for warp::reject::Rejection {
    fn from(e: CubeError) -> Self {
        warp::reject::custom(CubeRejection::Internal(e.message.to_string()))
    }
}

/// Rate limit errors are expected under load and are still returned to the client,
/// so they shouldn't pollute the error log.
fn is_rate_limit_error(e: &CubeError) -> bool {
    e.message.to_ascii_lowercase().contains("rate limit")
}

#[derive(Deserialize)]
pub struct UploadQuery {
    name: String,
}

#[derive(Debug, Deserialize)]
struct LocalLeaseFile {
    #[serde(rename = "holderId")]
    holder_id: String,
    epoch: i64,
    #[serde(rename = "tokenHash")]
    token_hash: String,
    #[serde(rename = "issuedAt")]
    issued_at: String,
    #[serde(rename = "expiresAt")]
    expires_at: String,
}

#[derive(Debug, Default, Deserialize)]
struct PromotionMarker {
    #[serde(rename = "activeLeader", default)]
    active_leader: String,
    #[serde(rename = "leaderEpoch", default)]
    leader_epoch: i64,
    #[serde(rename = "leaseClusterID", default)]
    lease_cluster_id: String,
    #[serde(rename = "leaseEpoch", default)]
    lease_epoch: i64,
    #[serde(rename = "leaseToken", default)]
    lease_token: String,
    #[serde(rename = "metaStoreReady", default = "default_meta_store_ready")]
    meta_store_ready: bool,
}

fn default_meta_store_ready() -> bool {
    true
}

impl Reject for CubeRejection {}

impl HttpServer {
    pub fn new(
        bind_address: String,
        leadership_file: String,
        promotion_file: String,
        auth: Arc<dyn SqlAuthService>,
        sql_service: Arc<dyn SqlService>,
        check_orphaned_messages_interval: Duration,
        drop_processing_messages_after: Duration,
        drop_complete_messages_after: Duration,
        max_message_size: usize,
        max_frame_size: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            bind_address,
            leadership_file,
            promotion_file,
            auth,
            sql_service,
            check_orphaned_messages_interval,
            drop_processing_messages_after,
            drop_complete_messages_after,
            max_message_size,
            max_frame_size,
            worker_loop: WorkerLoop::new("HttpServer message processing"),
            drop_orphaned_messages_loop: WorkerLoop::new("HttpServer drop orphaned messages"),
            cancel_token: CancellationToken::new(),
        })
    }

    pub async fn run_server(&self) -> Result<(), CubeError> {
        let (tx, mut rx) =
            mpsc::channel::<(mpsc::Sender<Arc<HttpMessage>>, SqlQueryContext, HttpMessage)>(100000);
        let auth_service = self.auth.clone();
        let tx_to_move_filter = warp::any().map(move || tx.clone());

        let auth_filter = warp::any()
            .and(warp::header::optional("authorization"))
            .and(warp::header::optional("x-process-id"))
            .and_then(
                move |auth_header: Option<String>, process_id: Option<String>| {
                    let auth_service = auth_service.clone();
                    async move {
                        if let Some(ref id) = process_id {
                            if id.len() > 64 {
                                return Err(warp::reject::custom(CubeRejection::Internal(
                                    "x-process-id header exceeds 64 characters".to_string(),
                                )));
                            }
                        }
                        let res = HttpServer::authorize(auth_service, auth_header).await;
                        match res {
                            Ok(user) => Ok(SqlQueryContext {
                                user,
                                inline_tables: InlineTables::new(),
                                parameters: None,
                                trace_obj: None,
                                process_id,
                            }),
                            Err(_) => Err(warp::reject::custom(CubeRejection::NotAuthorized)),
                        }
                    }
                },
            );

        let context_filter = tx_to_move_filter.and(auth_filter.clone());

        let context_filter_to_move = context_filter.clone();
        let max_frame_size = self.max_frame_size.clone();
        let max_message_size = self.max_message_size.clone();

        let query_route = warp::path!("ws")
            .and(context_filter_to_move)
            .and(warp::ws::ws())
            .and_then(move |tx: mpsc::Sender<(mpsc::Sender<Arc<HttpMessage>>, SqlQueryContext, HttpMessage)>, sql_query_context: SqlQueryContext, ws: Ws| async move {
                let tx_to_move = tx.clone();
                let sql_query_context = sql_query_context.clone();
                let reply = ws.max_frame_size(max_frame_size).max_message_size(max_message_size).on_upgrade(async move |mut web_socket| {
                    let process_id = sql_query_context.process_id.as_deref().unwrap_or("None");
                    trace!("WebSocket connection established (process_id: {})", process_id);
                    let (response_tx, mut response_rx) = mpsc::channel::<Arc<HttpMessage>>(10000);
                    loop {
                        tokio::select! {
                            Some(res) = response_rx.recv() => {
                                trace!("Sending web socket response (process_id: {})", process_id);
                                let send_res = web_socket.send(Message::binary(res.bytes())).await;
                                if let Err(e) = send_res {
                                    error!("Websocket message send error: {:?}", e)
                                }
                                if res.should_close_connection() {
                                   log::warn!("Websocket connection closed");
                                   break;
                                }
                            }
                            Some(msg) = web_socket.next() => {
                                match msg {
                                    Err(e) => {
                                        error!("Websocket error: {:?}", e);
                                        break;
                                    }
                                    Ok(msg) => {
                                        if msg.is_binary() {
                                            let message_buffer = msg.into_bytes();
                                            let http_message = match root_as_http_message(&message_buffer) {
                                                Err(e) => {
                                                    error!("Websocket message deserialization error: {:?}", e);
                                                    continue;
                                                },
                                                Ok(http_message) => http_message,
                                            };

                                            let message_id = http_message.message_id();
                                            let connection_id = http_message.connection_id().map(|s| s.to_string());

                                            match HttpMessage::read(http_message).await {
                                                Err(e) => {
                                                    error!("Websocket message read error: {:?}", e);

                                                    let send_res = web_socket.send(
                                                        Message::binary(HttpMessage { message_id, connection_id, command: HttpCommand::Error { error: e.to_string() } }.bytes())
                                                    ).await;
                                                    if let Err(e) = send_res {
                                                        error!("Websocket message send error: {:?}", e)
                                                    }
                                                    break;
                                                },
                                                Ok(msg) => {
                                                    trace!("Received web socket message (process_id: {})", process_id);
                                                    // TODO use timeout instead of try send for burst control however try_send is safer for now
                                                    if let Err(e) = tx_to_move.try_send((response_tx.clone(), sql_query_context.clone(), msg)) {
                                                        error!("Websocket channel error: {:?}", e);
                                                        let send_res = web_socket.send(
                                                            Message::binary(HttpMessage { message_id, connection_id, command: HttpCommand::Error { error: e.to_string() } }.bytes())
                                                        ).await;
                                                        if let Err(e) = send_res {
                                                            error!("Websocket message send error: {:?}", e)
                                                        }
                                                        break;
                                                    }
                                                }
                                            };
                                        } else if msg.is_ping() {
                                            let send_res = web_socket.send(Message::pong(Vec::new())).await;
                                            if let Err(e) = send_res {
                                                error!("Websocket ping send error: {:?}", e)
                                            }
                                        } else if msg.is_close() {
                                            break;
                                        } else {
                                            error!("Websocket received non binary msg: {:?}", msg);
                                            break;
                                        }
                                    }
                                }
                            }
                        };
                    };
                });
                Result::<_, Rejection>::Ok(warp::reply::with_header(reply, "X-CubeStore-Version", env!("CARGO_PKG_VERSION")))
            });

        let auth_filter_to_move = auth_filter.clone();
        let sql_service = self.sql_service.clone();
        let leadership_file = self.leadership_file.clone();
        let promotion_file = self.promotion_file.clone();

        let upload_route = warp::path!("upload-temp-file")
            .and(auth_filter_to_move)
            .and(warp::query::query::<UploadQuery>())
            .and(warp::body::stream())
            .and_then(move |sql_query_context, upload_query, body| {
                HttpServer::handle_upload(
                    sql_service.clone(),
                    sql_query_context,
                    upload_query,
                    leadership_file.clone(),
                    promotion_file.clone(),
                    body,
                )
            });

        let router_status_leadership_file = self.leadership_file.clone();
        let router_status_promotion_file = self.promotion_file.clone();
        let router_status_route = warp::path!("router" / "status").map(move || {
            warp::reply::json(&Self::router_status_payload(
                &router_status_leadership_file,
                &router_status_promotion_file,
            ))
        });

        let leadership_file = self.leadership_file.clone();
        let promotion_file = self.promotion_file.clone();
        let router_lease_route = warp::path!("router" / "lease")
            .and(warp::get())
            .map(move || match Self::local_lease_payload(&leadership_file, &promotion_file) {
                Ok(payload) => warp::reply::with_status(
                    warp::reply::json(&payload),
                    StatusCode::OK,
                ),
                Err(error) => warp::reply::with_status(
                    warp::reply::json(&json!({ "error": error })),
                    StatusCode::SERVICE_UNAVAILABLE,
                ),
            });

        let sql_service = self.sql_service.clone();

        let addr: SocketAddr = self.bind_address.parse().unwrap();
        info!("Http Server is listening on {}", self.bind_address);
        pub enum ProcessingState {
            Processing {
                subscribed_senders: Vec<Sender<Arc<HttpMessage>>>,
                last_touch: SystemTime,
            },
            Complete {
                result: Arc<HttpMessage>,
                last_touch: SystemTime,
            },
        }

        let messages_state = Arc::new(Mutex::new(
            HashMap::<(Option<String>, u32), ProcessingState>::new(),
        ));
        let process_loop = self.worker_loop.process_channel(
            Arc::new((
                sql_service,
                messages_state.clone(),
                self.leadership_file.clone(),
                self.promotion_file.clone(),
            )),
            &mut rx,
            async move |service,
                        (
                sender,
                sql_query_context,
                HttpMessage {
                    message_id,
                    connection_id,
                    command,
                },
            )| {
                let (sql_service, messages_state, leadership_file, promotion_file) = service.as_ref();
                let sql_service = sql_service.clone();
                let messages_state = messages_state.clone();
                if connection_id.is_some() {
                    let leadership_file = leadership_file.clone();
                    let promotion_file = promotion_file.clone();
                    cube_ext::spawn(async move {
                        let key = (connection_id.clone(), message_id);
                        {
                            let mut messages = messages_state.lock().await;
                            let state = messages.get_mut(&key);
                            match state {
                                None => {
                                    messages.insert(key.clone(), ProcessingState::Processing { subscribed_senders: vec![sender], last_touch: SystemTime::now() });
                                }
                                Some(ProcessingState::Processing { subscribed_senders, .. }) => {
                                    subscribed_senders.push(sender);
                                    return;
                                }
                                Some(ProcessingState::Complete { result, .. }) => {
                                    if let Err(e) = sender.send(result.clone()).await {
                                        error!("Websocket send completed message error: {:?}", e);
                                    } else {
                                        messages.remove(&key);
                                    }
                                    return;
                                }
                            }
                        };
                        let res = HttpServer::process_command(
                            sql_service.clone(),
                            sql_query_context,
                            &leadership_file,
                            &promotion_file,
                            command.clone(),
                        )
                            .await;
                        let message = Arc::new(match res {
                            Ok(command) => HttpMessage {
                                message_id,
                                connection_id,
                                command,
                            },
                            Err(e) => {
                                let command_text = match &command {
                                    HttpCommand::Query { query, .. } => format!("HttpCommand::Query {{ query: {:?} }}", query),
                                    HttpCommand::Error { error } => format!("HttpCommand::Error {{ error: {:?} }}", error),
                                    HttpCommand::CloseConnection { error } => format!("HttpCommand::CloseConnection {{ error: {:?} }}", error),
                                    HttpCommand::ResultSet { .. } => format!("HttpCommand::ResultSet {{}}"),
                                    HttpCommand::QueryResultArrow { .. } => format!("HttpCommand::QueryResultArrow {{}}"),
                                    HttpCommand::QueryResultCompleted => format!("HttpCommand::QueryResultCompleted"),
                                };
                                let level = if is_rate_limit_error(&e) {
                                    log::Level::Warn
                                } else {
                                    log::Level::Error
                                };
                                log::log!(
                                    level,
                                    "Error processing HTTP command (connection_id={}): {}\nThe command: {}",
                                    if let Some(c) = connection_id.as_ref() { c.as_str() } else { "(None)" },
                                    e.display_with_backtrace(),
                                    command_text,
                                );
                                let command = if e.is_wrong_connection() {
                                    HttpCommand::CloseConnection {
                                        error: e.to_string(),
                                    }

                                } else {
                                    HttpCommand::Error {
                                        error: e.to_string(),
                                    }
                                };

                                HttpMessage {
                                    message_id,
                                    connection_id,
                                    command,
                                }
                            }
                        });
                        let senders = {
                            let mut messages = messages_state.lock().await;
                            match messages.remove(&key) {
                                None => {
                                    trace!("Websocket message with '{:?}' key was already resolved: {:?}", key, command);
                                    return;
                                }
                                Some(ProcessingState::Processing { subscribed_senders, .. }) => {
                                    messages.insert(key.clone(), ProcessingState::Complete { result: message.clone(), last_touch: SystemTime::now() });
                                    subscribed_senders
                                }
                                Some(ProcessingState::Complete { .. }) => {
                                    trace!("Websocket message with '{:?}' key was already completed by another process: {:?}", key, command);
                                    return;
                                }
                            }
                        };
                        let mut sent_successfully = false;
                        for sender in senders.into_iter() {
                            if sender.is_closed() {
                                trace!("Websocket is closed. Skipping send for '{:?}' key: {:?}", key, command);
                                continue;
                            }
                            if let Err(e) = sender.send(message.clone()).await {
                                error!("Websocket send error. Skipping send for '{:?}' key: {:?}, {}", key, command, e);
                                continue;
                            }
                            sent_successfully = true;
                        }

                        {
                            let mut messages = messages_state.lock().await;
                            match messages.get(&key) {
                                None => {
                                    trace!("Websocket message was resolved just after send. Skipping send for '{:?}' key: {:?}", key, command);
                                    return;
                                }
                                Some(ProcessingState::Processing { .. }) => {
                                    trace!("Websocket message with '{:?}' key was switched to processing just after send: {:?}", key, command);
                                }
                                Some(ProcessingState::Complete { .. }) => {
                                    if sent_successfully {
                                        messages.remove(&key);
                                    }
                                }
                            }
                        }
                    });
                } else {
                    let leadership_file = leadership_file.clone();
                    let promotion_file = promotion_file.clone();
                    cube_ext::spawn(async move {
                        let command_text = match &command {
                            HttpCommand::Query { query, .. } => format!("HttpCommand::Query {{ query: {:?} }}", query),
                            HttpCommand::Error { error } => format!("HttpCommand::Error {{ error: {:?} }}", error),
                            HttpCommand::CloseConnection { error } => format!("HttpCommand::CloseConnection {{ error: {:?} }}", error),
                            HttpCommand::ResultSet { .. } => format!("HttpCommand::ResultSet {{}}"),
                            HttpCommand::QueryResultArrow { .. } => format!("HttpCommand::QueryResultArrow {{}}"),
                            HttpCommand::QueryResultCompleted => format!("HttpCommand::QueryResultCompleted"),
                        };
                        let res = HttpServer::process_command(
                            sql_service.clone(),
                            sql_query_context,
                            &leadership_file,
                            &promotion_file,
                            command,
                        )
                            .await;
                        let message = Arc::new(match res {
                            Ok(command) => HttpMessage {
                                message_id,
                                connection_id,
                                command,
                            },
                            Err(e) => {
                                let level = if is_rate_limit_error(&e) {
                                    log::Level::Warn
                                } else {
                                    log::Level::Error
                                };
                                log::log!(
                                    level,
                                    "Error processing HTTP command: {}\nThe command: {}",
                                    e.display_with_backtrace(),
                                    command_text,
                                );
                                HttpMessage {
                                    message_id,
                                    connection_id,
                                    command: HttpCommand::Error {
                                        error: e.to_string(),
                                    },
                                }
                            }
                        });
                        if sender.is_closed() {
                            trace!(
                                "Websocket is closed. Dropping message with id: {:?}",
                                message_id
                            );
                            return;
                        }
                        if let Err(e) = sender.send(message).await {
                            error!("Websocket send result channel error: {:?}", e);
                        }
                    });
                }
                Ok(())
            },
        );

        let check_orphaned_messages_interval = self.check_orphaned_messages_interval.clone();
        let drop_complete_messages_after = self.drop_complete_messages_after.clone();
        let drop_processing_messages_after = self.drop_processing_messages_after.clone();
        let drop_orphaned_messages_loop = self.drop_orphaned_messages_loop.process(
            messages_state,
            move |_| async move { Ok(Delay::new(check_orphaned_messages_interval.clone()).await) },
            move |messages_state, _| async move {
                let mut messages_state = messages_state.lock().await;
                let mut keys_to_remove = Vec::new();
                let mut orphaned_complete_results = 0;
                for (key, state) in messages_state.iter() {
                    match state {
                        ProcessingState::Processing { last_touch, .. } => {
                            if SystemTime::now()
                                .duration_since(last_touch.clone())
                                .unwrap()
                                > drop_processing_messages_after
                            {
                                trace!("Removing orphaned processing message with '{:?}' key", key);
                                keys_to_remove.push(key.clone());
                            }
                        }
                        ProcessingState::Complete { last_touch, .. } => {
                            if SystemTime::now()
                                .duration_since(last_touch.clone())
                                .unwrap()
                                > drop_complete_messages_after
                            {
                                trace!("Removing orphaned complete message with '{:?}' key", key);
                                keys_to_remove.push(key.clone());
                            } else {
                                orphaned_complete_results += 1;
                            }
                        }
                    }
                }
                if orphaned_complete_results > 100 {
                    log::warn!(
                        "Keeping {} orphaned complete results to be retrieved by reconnecting socket",
                        orphaned_complete_results
                    );
                }
                for key in keys_to_remove {
                    messages_state.remove(&key);
                }
                Ok(())
            },
        );
        let cancel_token = self.cancel_token.clone();
        let (_, server_future) = warp::serve(
            query_route
                .or(upload_route)
                .or(router_status_route)
                .or(router_lease_route)
                .recover(
            |err: Rejection| async move {
                let mut obj = HashMap::new();
                if let Some(ws_error) = err.find::<CubeRejection>() {
                    match ws_error {
                        CubeRejection::NotAuthorized => {
                            obj.insert("error".to_string(), "Not authorized".to_string());
                            Ok(warp::reply::with_status(
                                warp::reply::json(&obj),
                                StatusCode::FORBIDDEN,
                            ))
                        }
                        CubeRejection::NotLeader => {
                            obj.insert("error".to_string(), "Router not leader".to_string());
                            Ok(warp::reply::with_status(
                                warp::reply::json(&obj),
                                StatusCode::SERVICE_UNAVAILABLE,
                            ))
                        }
                        CubeRejection::LeaseFenced(e) => {
                            obj.insert("error".to_string(), e.to_string());
                            Ok(warp::reply::with_status(
                                warp::reply::json(&obj),
                                StatusCode::SERVICE_UNAVAILABLE,
                            ))
                        }
                        CubeRejection::Internal(e) => {
                            obj.insert("error".to_string(), e.to_string());
                            Ok(warp::reply::with_status(
                                warp::reply::json(&obj),
                                StatusCode::INTERNAL_SERVER_ERROR,
                            ))
                        }
                    }
                } else {
                    Err(err)
                }
            },
                ),
        )
        .bind_with_graceful_shutdown(addr, async move { cancel_token.cancelled().await });
        let _ = tokio::join!(process_loop, server_future, drop_orphaned_messages_loop);

        Ok(())
    }

    pub async fn handle_upload(
        sql_service: Arc<dyn SqlService>,
        sql_query_context: SqlQueryContext,
        upload_query: UploadQuery,
        leadership_file: String,
        promotion_file: String,
        mut body: impl Stream<Item = Result<impl warp::Buf, warp::Error>> + Unpin,
    ) -> Result<impl Reply, Rejection> {
        if let Err(error) = Self::ensure_write_fence(&leadership_file, &promotion_file) {
            return Err(warp::reject::custom(CubeRejection::LeaseFenced(error)));
        }

        let temp_file = NamedTempFile::new_in(
            sql_service
                .temp_uploads_dir(sql_query_context.clone())
                .await
                .map_err(|e| CubeRejection::Internal(e.to_string()))?,
        )
        .map_err(|e| CubeRejection::Internal(e.to_string()))?;
        {
            let mut file = File::create(temp_file.path())
                .await
                .map_err(|e| CubeRejection::Internal(e.to_string()))?;
            while let Some(item) = body.next().await {
                let item = item.map_err(|e| CubeRejection::Internal(e.to_string()))?;
                file.write_all(item.chunk())
                    .await
                    .map_err(|e| CubeRejection::Internal(e.to_string()))?;
            }
            file.flush()
                .await
                .map_err(|e| CubeRejection::Internal(e.to_string()))?;
            file.close()
                .await
                .map_err(|e| CubeRejection::Internal(e.to_string()))?;
        }

        sql_service
            .upload_temp_file(sql_query_context, upload_query.name, temp_file.path())
            .await
            .map_err(|e| CubeRejection::Internal(e.to_string()))?;

        Ok(warp::reply())
    }

    pub async fn process_command(
        sql_service: Arc<dyn SqlService>,
        sql_query_context: SqlQueryContext,
        leadership_file: &str,
        promotion_file: &str,
        command: HttpCommand,
    ) -> Result<HttpCommand, CubeError> {
        match command {
            HttpCommand::Query {
                query,
                inline_tables,
                trace_obj,
                parameters,
                response_format,
            } => {
                if !Self::is_read_query(&query) {
                    Self::ensure_write_fence(leadership_file, promotion_file)
                        .map_err(CubeError::wrong_connection)?;
                }
                let query_result = sql_service
                    .exec_query_with_context(
                        sql_query_context
                            .with_trace_obj(trace_obj)
                            .with_inline_tables(&inline_tables)
                            .with_parameters(&parameters),
                        &query,
                    )
                    .await?;
                match response_format {
                    QueryResultFormat::Legacy => Ok(HttpCommand::ResultSet {
                        data_frame: query_result.collect().await?,
                    }),
                    QueryResultFormat::Arrow => {
                        // Commands that complete without a result set (CREATE
                        // TABLE/INSERT, queue/cache writes) carry zero columns.
                        // There's no Arrow stream to build for them, so signal
                        // completion with a dedicated result instead.
                        if query_result.schema().fields().is_empty() {
                            Ok(HttpCommand::QueryResultCompleted)
                        } else {
                            let data = query_result.to_arrow_ipc_stream().await?;
                            Ok(HttpCommand::QueryResultArrow { data })
                        }
                    }
                    other => Err(CubeError::user(format!(
                        "Unsupported response_format: {:?}",
                        other
                    ))),
                }
            }
            x => Err(CubeError::user(format!("Unexpected command: {:?}", x))),
        }
    }

    fn is_read_query(query: &str) -> bool {
        matches!(
            query.trim_start().split_whitespace().next().map(|keyword| keyword.to_ascii_uppercase()),
            Some(keyword) if matches!(keyword.as_str(), "SELECT" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN")
        )
    }

    fn ensure_write_fence(leadership_file: &str, promotion_file: &str) -> Result<(), String> {
        let lease = Self::read_local_lease(leadership_file)?;
        let marker = Self::read_promotion_marker(promotion_file)?;
        let node = Self::current_node_name()
            .ok_or_else(|| "Router node identity unavailable".to_string())?;
        if !Self::promotion_matches(&lease, &marker, &node) {
            return Err("lease epoch, token, or promotion marker mismatch".to_string());
        }
        Ok(())
    }

    fn read_local_lease(path: &str) -> Result<LocalLeaseFile, String> {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("lease-agent unavailable: {e}"))?;
        let lease: LocalLeaseFile = serde_json::from_str(&raw)
            .map_err(|e| format!("invalid lease-agent contract: {e}"))?;
        if lease.holder_id.trim().is_empty()
            || lease.epoch <= 0
            || lease.token_hash.trim().is_empty()
            || lease.issued_at.trim().is_empty()
            || lease.expires_at.trim().is_empty()
        {
            return Err("invalid lease-agent contract: missing lease identity".to_string());
        }
        let expires_at = chrono::DateTime::parse_from_rfc3339(&lease.expires_at)
            .map_err(|e| format!("invalid lease expiry: {e}"))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("invalid local clock: {e}"))?
            .as_secs() as i64;
        if expires_at.timestamp() <= now {
            return Err("lease-agent lease expired".to_string());
        }
        Ok(lease)
    }

    fn read_promotion_marker(path: &str) -> Result<PromotionMarker, String> {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("promotion marker unavailable: {e}"))?;
        let marker: PromotionMarker = serde_json::from_str(raw.trim())
            .map_err(|e| format!("invalid promotion marker: {e}"))?;
        if marker.active_leader.trim().is_empty()
            || marker.leader_epoch <= 0
            || marker.lease_cluster_id.trim().is_empty()
            || marker.lease_epoch <= 0
            || marker.lease_token.trim().is_empty()
        {
            return Err("invalid promotion marker: missing promotion identity".to_string());
        }
        Ok(marker)
    }

    fn current_node_name() -> Option<String> {
        env::var("CUBESTORE_NODE_NAME")
            .or_else(|_| env::var("CUBESTORE_SERVER_NAME"))
            .or_else(|_| env::var("HOSTNAME"))
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    fn promotion_matches(lease: &LocalLeaseFile, marker: &PromotionMarker, node: &str) -> bool {
        marker.active_leader == node
            && lease.holder_id == node
            && marker.leader_epoch == lease.epoch
            && marker.lease_epoch == lease.epoch
            && marker.meta_store_ready
            && !marker.lease_cluster_id.trim().is_empty()
            && !marker.lease_token.trim().is_empty()
            && Self::hash_lease_token(&marker.lease_token) == lease.token_hash
    }

    fn local_lease_payload(leadership_file: &str, promotion_file: &str) -> Result<Value, String> {
        let lease = Self::read_local_lease(leadership_file)?;
        let marker = Self::read_promotion_marker(promotion_file).ok();
        let node = Self::current_node_name().unwrap_or_default();
        let write_ready = marker
            .as_ref()
            .map(|marker| Self::promotion_matches(&lease, marker, &node))
            .unwrap_or(false);
        let promotion = marker.map(|marker| {
            json!({
                "activeLeader": marker.active_leader,
                "leaderEpoch": marker.leader_epoch,
                "leaseEpoch": marker.lease_epoch,
                "leaseTokenHash": if marker.lease_token.is_empty() {
                    Value::Null
                } else {
                    Value::String(Self::hash_lease_token(&marker.lease_token))
                },
            })
        });
        Ok(json!({
            "holderId": lease.holder_id,
            "epoch": lease.epoch,
            "tokenHash": lease.token_hash,
            "issuedAt": lease.issued_at,
            "expiresAt": lease.expires_at,
            "promotionMarker": promotion,
            "writeReady": write_ready,
        }))
    }

    fn router_status_payload(leadership_file: &str, promotion_file: &str) -> Value {
        let node_name = Self::current_node_name().unwrap_or_else(|| "unknown".to_string());
        let lease = Self::read_local_lease(leadership_file).ok();
        let marker = Self::read_promotion_marker(promotion_file).ok();
        let is_leader = match (&lease, &marker) {
            (Some(lease), Some(marker)) => Self::promotion_matches(lease, marker, &node_name),
            _ => false,
        };
        let leader_epoch = marker
            .as_ref()
            .map(|marker| Value::from(marker.leader_epoch))
            .unwrap_or(Value::Null);
        let lease_epoch = marker
            .as_ref()
            .map(|marker| Value::from(marker.lease_epoch))
            .unwrap_or(Value::Null);
        let lease_token_hash = marker
            .as_ref()
            .map(|marker| Value::from(Self::hash_lease_token(&marker.lease_token)))
            .unwrap_or(Value::Null);
        let active_leader = marker
            .as_ref()
            .map(|marker| Value::from(marker.active_leader.clone()))
            .unwrap_or(Value::Null);
        let role = if is_leader { "leader" } else { "follower" };
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        json!({
            "nodeName": node_name,
            "isLeader": is_leader,
            "role": role,
            "mode": if is_leader { "primary" } else { "follower" },
            "leaderStateSource": "promotion",
            "activeLeader": active_leader,
            "leaderEpoch": leader_epoch,
            "leaseEpoch": lease_epoch,
            "leaseTokenHash": lease_token_hash,
            "metaStoreReady": marker.as_ref().map(|marker| marker.meta_store_ready).unwrap_or(false),
            "timestampUnixSecs": timestamp,
        })
    }

    fn hash_lease_token(token: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        format!("sha256:{}", hex::encode(hasher.finalize()))
    }

    pub async fn authorize(
        auth: Arc<dyn SqlAuthService>,
        auth_header: Option<String>,
    ) -> Result<Option<String>, CubeError> {
        let credentials = auth_header
            .map(|auth_header| Credentials::from_header(auth_header))
            .transpose()
            .map_err(|e| CubeError::from_error(e))?;
        if let Some(password) = auth
            .authenticate(credentials.as_ref().map(|c| c.user_id.to_string()))
            .await?
        {
            if Some(password) != credentials.as_ref().map(|c| c.password.to_string()) {
                Err(CubeError::user(
                    "User or password doesn't match".to_string(),
                ))
            } else {
                Ok(credentials.as_ref().map(|c| c.user_id.to_string()))
            }
        } else {
            Ok(credentials.as_ref().map(|c| c.user_id.to_string()))
        }
    }

    pub async fn stop_processing(&self) {
        self.worker_loop.stop();
        self.drop_orphaned_messages_loop.stop();
        self.cancel_token.cancel();
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct HttpMessage {
    message_id: u32,
    command: HttpCommand,
    connection_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HttpCommand {
    Query {
        query: String,
        inline_tables: InlineTables,
        trace_obj: Option<String>,
        parameters: Option<QueryParameters>,
        response_format: QueryResultFormat,
    },
    ResultSet {
        data_frame: Arc<DataFrame>,
    },
    QueryResultArrow {
        /// Pre-serialized Arrow IPC stream payload. May contain multiple
        /// RecordBatch messages following the schema header; consumers must
        /// decode it with a streaming IPC reader.
        data: Vec<u8>,
    },
    /// Command completed without a result set (zero columns) — e.g. CREATE
    /// TABLE/INSERT or queue/cache writes. Only produced for Arrow-format
    /// requests; the legacy path returns an empty `ResultSet` instead.
    QueryResultCompleted,
    CloseConnection {
        error: String,
    },
    Error {
        error: String,
    },
}

impl HttpMessage {
    pub fn bytes(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::with_capacity(1024);
        let mut data_frame_serialization_start = None::<SystemTime>;
        let args = HttpMessageArgs {
            message_id: self.message_id,
            command_type: match self.command {
                HttpCommand::Query { .. } => cubeshared::codegen::HttpCommand::HttpQuery,
                HttpCommand::ResultSet { .. } => cubeshared::codegen::HttpCommand::HttpResultSet,
                HttpCommand::QueryResultArrow { .. } | HttpCommand::QueryResultCompleted => {
                    cubeshared::codegen::HttpCommand::HttpQueryResult
                }
                HttpCommand::CloseConnection { .. } | HttpCommand::Error { .. } => {
                    cubeshared::codegen::HttpCommand::HttpError
                }
            },
            command: match &self.command {
                HttpCommand::Query {
                    query,
                    inline_tables,
                    trace_obj,
                    parameters,
                    response_format,
                } => {
                    let query_offset = builder.create_string(&query);
                    let trace_obj_offset = trace_obj.as_ref().map(|o| builder.create_string(o));

                    if !inline_tables.is_empty() {
                        panic!("serializing inline_tables is not implemented")
                    }

                    if parameters.is_some() {
                        panic!("serializing parameters is not implemented")
                    }

                    Some(
                        HttpQuery::create(
                            &mut builder,
                            &HttpQueryArgs {
                                query: Some(query_offset),
                                inline_tables: None,
                                trace_obj: trace_obj_offset,
                                parameters: None,
                                response_format: *response_format,
                            },
                        )
                        .as_union_value(),
                    )
                }
                HttpCommand::QueryResultArrow { data } => {
                    let payload = builder.create_vector(data);
                    let arrow_table = HttpQueryResultArrow::create(
                        &mut builder,
                        &HttpQueryResultArrowArgs {
                            data: Some(payload),
                            // We don't support streaming for now, but clients should implement it
                            // according to the protocol specification
                            is_last: true,
                        },
                    );
                    Some(
                        HttpQueryResult::create(
                            &mut builder,
                            &HttpQueryResultArgs {
                                data_type: HttpQueryResultData::HttpQueryResultArrow,
                                data: Some(arrow_table.as_union_value()),
                            },
                        )
                        .as_union_value(),
                    )
                }
                HttpCommand::QueryResultCompleted => {
                    let completed_table = HttpQueryResultCompleted::create(
                        &mut builder,
                        &HttpQueryResultCompletedArgs {},
                    );
                    Some(
                        HttpQueryResult::create(
                            &mut builder,
                            &HttpQueryResultArgs {
                                data_type: HttpQueryResultData::HttpQueryResultCompleted,
                                data: Some(completed_table.as_union_value()),
                            },
                        )
                        .as_union_value(),
                    )
                }
                HttpCommand::Error { error } | HttpCommand::CloseConnection { error } => {
                    let error_offset = builder.create_string(&error);
                    Some(
                        HttpError::create(
                            &mut builder,
                            &HttpErrorArgs {
                                error: Some(error_offset),
                            },
                        )
                        .as_union_value(),
                    )
                }
                HttpCommand::ResultSet { data_frame } => {
                    data_frame_serialization_start = Some(SystemTime::now());
                    let columns_vec =
                        HttpMessage::build_columns(&mut builder, data_frame.get_columns());
                    let rows = HttpMessage::build_rows(&mut builder, data_frame.clone());

                    Some(
                        HttpResultSet::create(
                            &mut builder,
                            &HttpResultSetArgs {
                                columns: Some(columns_vec),
                                rows: Some(rows),
                            },
                        )
                        .as_union_value(),
                    )
                }
            },
            connection_id: self
                .connection_id
                .as_ref()
                .map(|c| builder.create_string(c)),
        };
        let message = cubeshared::codegen::HttpMessage::create(&mut builder, &args);
        builder.finish(message, None);
        let result = builder.finished_data().to_vec(); // TODO copy
        if let Some(data_frame_serialization_start) = data_frame_serialization_start {
            app_metrics::HTTP_MESSAGE_DATA_FRAME_SERIALIZATION_TIME_US.report(
                data_frame_serialization_start
                    .elapsed()
                    .unwrap_or_else(|_| Duration::ZERO)
                    .as_micros() as i64,
            );
        }
        result
    }

    pub fn should_close_connection(&self) -> bool {
        matches!(self.command, HttpCommand::CloseConnection { .. })
    }

    fn build_columns<'a: 'ma, 'ma>(
        builder: &'ma mut FlatBufferBuilder<'a>,
        columns: &Vec<Column>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<&'a str>>> {
        let columns = columns
            .iter()
            .map(|c| builder.create_string(c.get_name()))
            .collect::<Vec<_>>();
        let columns_vec = builder.create_vector(columns.as_slice());
        columns_vec
    }

    fn build_rows<'a: 'ma, 'ma>(
        builder: &'ma mut FlatBufferBuilder<'a>,
        data_frame: Arc<DataFrame>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<HttpRow<'a>>>> {
        let columns = data_frame.get_columns();
        let rows = data_frame.get_rows();
        let mut row_offsets = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut value_offsets = Vec::with_capacity(row.values().len());
            for (i, value) in row.values().iter().enumerate() {
                let value = match value {
                    TableValue::Null => HttpColumnValue::create(
                        builder,
                        &HttpColumnValueArgs { string_value: None },
                    ),
                    TableValue::String(v) => {
                        let string_value = Some(builder.create_string(v));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Int(v) => {
                        let string_value = Some(builder.create_string(&v.to_string()));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Int96(v) => {
                        let string_value = Some(builder.create_string(&v.to_string()));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Decimal(v) => {
                        let scale =
                            u8::try_from(columns[i].get_column_type().target_scale()).unwrap();
                        let string_value = Some(builder.create_string(&v.to_string(scale)));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Decimal96(v) => {
                        let scale =
                            u8::try_from(columns[i].get_column_type().target_scale()).unwrap();
                        let string_value = Some(builder.create_string(&v.to_string(scale)));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Float(v) => {
                        let string_value = Some(builder.create_string(&v.to_string()));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Bytes(v) => {
                        let string_value = Some(
                            builder.create_string(&format!("0x{}", v.encode_hex_upper::<String>())),
                        );
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Timestamp(v) => {
                        let string_value = Some(builder.create_string(&v.to_string()));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Boolean(v) => {
                        let string_value = Some(builder.create_string(&v.to_string()));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                };
                value_offsets.push(value);
            }
            let values = Some(builder.create_vector(value_offsets.as_slice()));
            let row = HttpRow::create(builder, &HttpRowArgs { values });
            row_offsets.push(row);
        }

        let rows = builder.create_vector(row_offsets.as_slice());
        rows
    }

    pub async fn read<'a>(
        http_message: cubeshared::codegen::HttpMessage<'a>,
    ) -> Result<Self, CubeError> {
        Ok(HttpMessage {
            message_id: http_message.message_id(),
            connection_id: http_message.connection_id().map(|s| s.to_string()),
            command: match http_message.command_type() {
                cubeshared::codegen::HttpCommand::HttpQuery => {
                    let query = http_message.command_as_http_query().unwrap();

                    let mut inline_tables = Vec::new();
                    if let Some(query_inline_tables) = query.inline_tables() {
                        for inline_table in query_inline_tables.iter() {
                            let name = inline_table.name().unwrap().to_string();
                            let types = inline_table
                                .types()
                                .unwrap()
                                .iter()
                                .map(|column_type| ColumnType::from_string(column_type))
                                .collect::<Result<Vec<_>, CubeError>>()?;
                            let columns = inline_table
                                .columns()
                                .unwrap()
                                .iter()
                                .enumerate()
                                .map(|(i, name)| Column::new(name.to_string(), types[i].clone(), i))
                                .collect::<Vec<_>>();
                            let rows = if inline_table.csv_rows().is_some() {
                                let csv_rows = inline_table.csv_rows().unwrap().to_owned();
                                let csv_reader = Box::pin(BufReader::new(csv_rows.as_bytes()));
                                let mut rows_stream = ImportFormat::CSVNoHeader
                                    .row_stream_from_reader(csv_reader, columns.clone())?;
                                let mut rows = vec![];
                                while let Some(row) = rows_stream.next().await {
                                    if let Some(row) = row? {
                                        rows.push(row)
                                    }
                                }
                                rows
                            } else {
                                vec![]
                            };
                            inline_tables.push(InlineTable::new(
                                inline_tables.len() as u64 + 1,
                                name,
                                Arc::new(DataFrame::new(columns, rows)),
                            ));
                        }
                    };

                    let parameters = if let Some(http_params) = query.parameters() {
                        let mut res = Vec::new();

                        for http_param in http_params.iter() {
                            let value = match http_param.value_type() {
                                HttpParameterValue::NullValue => QueryParameter::Null,
                                HttpParameterValue::Int64Value => QueryParameter::Int64Value(
                                    http_param.value_as_int_64_value().unwrap().v(),
                                ),
                                HttpParameterValue::BoolValue => QueryParameter::BoolValue(
                                    http_param.value_as_bool_value().unwrap().v(),
                                ),
                                HttpParameterValue::StringValue => QueryParameter::StringValue(
                                    http_param.value_as_string_value().unwrap().v().to_string(),
                                ),
                                HttpParameterValue::BinaryValue => QueryParameter::BinaryValue(
                                    http_param
                                        .value_as_binary_value()
                                        .unwrap()
                                        .v()
                                        .iter()
                                        .collect::<Vec<u8>>(),
                                ),
                                HttpParameterValue::Float64Value => QueryParameter::Float64Value(
                                    http_param.value_as_float_64_value().unwrap().v(),
                                ),
                                value_type => {
                                    return Err(CubeError::internal(format!(
                                        "Unsupported parameter type: {:?}",
                                        value_type
                                    )))
                                }
                            };

                            res.push(value);
                        }

                        Some(res)
                    } else {
                        None
                    };

                    HttpCommand::Query {
                        query: query.query().unwrap().to_string(),
                        trace_obj: query.trace_obj().map(|q| q.to_string()),
                        inline_tables,
                        parameters,
                        response_format: query.response_format(),
                    }
                }
                cubeshared::codegen::HttpCommand::HttpResultSet => {
                    let result_set = http_message.command_as_http_result_set().unwrap();
                    let mut result_rows = Vec::new();
                    if let Some(rows) = result_set.rows() {
                        for row in rows.iter() {
                            let mut result_row = Vec::new();
                            if let Some(values) = row.values() {
                                for value in values.iter() {
                                    result_row.push(
                                        value
                                            .string_value()
                                            .map(|s| TableValue::String(s.to_string()))
                                            .unwrap_or(TableValue::Null),
                                    );
                                }
                            }
                            result_rows.push(Row::new(result_row));
                        }
                    }
                    let mut result_columns = Vec::new();
                    if let Some(columns) = result_set.columns() {
                        let mut index = 0;
                        for column in columns.iter() {
                            result_columns.push(Column::new(
                                column.to_string(),
                                ColumnType::String,
                                index,
                            ));
                            index += 1;
                        }
                    }
                    HttpCommand::ResultSet {
                        data_frame: Arc::new(DataFrame::new(result_columns, result_rows)),
                    }
                }
                command => {
                    return Err(CubeError::internal(format!(
                        "Unexpected command: {:?}",
                        command
                    )));
                }
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{init_test_logger, Config};
    use crate::http::{HttpCommand, HttpMessage, HttpServer};
    use crate::metastore::{Column, ColumnType};
    use crate::mysql::MockSqlAuthService;
    use crate::sql::{
        timestamp_from_string, InlineTable, QueryPlans, QueryResult, SqlQueryContext, SqlService,
    };
    use crate::store::DataFrame;
    use crate::table::{Row, TableValue};
    use crate::CubeError;
    use async_trait::async_trait;
    use bytes::Bytes;
    use cubeshared::codegen::{
        HttpMessageArgs, HttpQuery, HttpQueryArgs, HttpTable, HttpTableArgs,
    };
    use cubeshared::flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
    use datafusion::cube_ext;
    use futures_util::{SinkExt, StreamExt};
    use indoc::indoc;
    use log::trace;
    use pretty_assertions::assert_eq;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
    use url::Url;

    /// Minimal SqlService that always replies with a fixed DataFrame, used to
    /// drive process_command in unit tests.
    struct StubService(Arc<DataFrame>);
    crate::di_service!(StubService, [SqlService]);
    #[async_trait]
    impl SqlService for StubService {
        async fn exec_query(&self, _q: &str) -> Result<QueryResult, CubeError> {
            unimplemented!("Mock")
        }
        async fn exec_query_with_context(
            &self,
            _ctx: SqlQueryContext,
            _q: &str,
        ) -> Result<QueryResult, CubeError> {
            Ok(QueryResult::Frame(self.0.clone()))
        }
        async fn plan_query(&self, _q: &str) -> Result<QueryPlans, CubeError> {
            unimplemented!("Mock")
        }
        async fn plan_query_with_context(
            &self,
            _ctx: SqlQueryContext,
            _q: &str,
        ) -> Result<QueryPlans, CubeError> {
            unimplemented!("Mock")
        }
        async fn upload_temp_file(
            &self,
            _ctx: SqlQueryContext,
            _name: String,
            _path: &Path,
        ) -> Result<(), CubeError> {
            unimplemented!("Mock")
        }
        async fn temp_uploads_dir(&self, _ctx: SqlQueryContext) -> Result<String, CubeError> {
            unimplemented!("Mock")
        }
    }

    fn build_types<'a: 'ma, 'ma>(
        builder: &'ma mut FlatBufferBuilder<'a>,
        columns: &Vec<Column>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<&'a str>>> {
        let str_types = columns
            .iter()
            .map(|c| builder.create_string(&c.get_column_type().to_string()))
            .collect::<Vec<_>>();
        let types_vec = builder.create_vector(str_types.as_slice());
        types_vec
    }

    #[tokio::test]
    async fn query_test() -> Result<(), CubeError> {
        let message = HttpMessage {
            message_id: 1234,
            command: HttpCommand::Query {
                query: "test query".to_string(),
                inline_tables: vec![],
                trace_obj: Some("test trace".to_string()),
                parameters: None,
                response_format: QueryResultFormat::Legacy,
            },
            connection_id: Some("foo".to_string()),
        };
        let bytes = message.bytes();
        let output_message = HttpMessage::read(root_as_http_message(&bytes)?).await?;
        assert_eq!(message, output_message);
        Ok(())
    }

    #[tokio::test]
    async fn inline_tables_query_test() -> Result<(), CubeError> {
        let columns = vec![
            Column::new("A".to_string(), ColumnType::Int, 0),
            Column::new("B".to_string(), ColumnType::String, 1),
            Column::new("C".to_string(), ColumnType::Timestamp, 2),
        ];
        let rows = vec![
            Row::new(vec![
                TableValue::Int(1),
                TableValue::String("one".to_string()),
                TableValue::Timestamp(timestamp_from_string("2020-01-01T00:00:00.000Z").unwrap()),
            ]),
            Row::new(vec![
                TableValue::Null,
                TableValue::String("two".to_string()),
                TableValue::Timestamp(timestamp_from_string("2020-01-02T00:00:00.000Z").unwrap()),
            ]),
            Row::new(vec![
                TableValue::Int(3),
                TableValue::Null,
                TableValue::Timestamp(timestamp_from_string("2020-01-03T00:00:00.000Z").unwrap()),
            ]),
            Row::new(vec![
                TableValue::Int(4),
                TableValue::String("four".to_string()),
                TableValue::Null,
            ]),
        ];
        let csv_rows = indoc! {"
            1,one,2020-01-01T00:00:00.000Z
            ,two,2020-01-02T00:00:00.000Z
            3,,2020-01-03T00:00:00.000Z
            4,four,
        "};
        let mut builder = cubeshared::flatbuffers::FlatBufferBuilder::with_capacity(1024);
        let query_offset = builder.create_string("query");
        let mut inline_tables_offsets = Vec::with_capacity(1);
        let name_offset = builder.create_string("table");
        let columns_vec = HttpMessage::build_columns(&mut builder, &columns);
        let types_vec = build_types(&mut builder, &columns);
        let csv_rows_value = builder.create_string(csv_rows);
        let connection_id_offset = builder.create_string("foo");
        let inline_table_offset = HttpTable::create(
            &mut builder,
            &HttpTableArgs {
                name: Some(name_offset),
                columns: Some(columns_vec),
                types: Some(types_vec),
                csv_rows: Some(csv_rows_value),
            },
        );
        inline_tables_offsets.push(inline_table_offset);
        let inline_tables_offset = builder.create_vector(inline_tables_offsets.as_slice());
        let query_value = HttpQuery::create(
            &mut builder,
            &HttpQueryArgs {
                query: Some(query_offset),
                inline_tables: Some(inline_tables_offset),
                trace_obj: None,
                parameters: None,
                response_format: QueryResultFormat::Legacy,
            },
        );
        let args = HttpMessageArgs {
            message_id: 1234,
            command_type: cubeshared::codegen::HttpCommand::HttpQuery,
            command: Some(query_value.as_union_value()),
            connection_id: Some(connection_id_offset),
        };
        let message = cubeshared::codegen::HttpMessage::create(&mut builder, &args);
        builder.finish(message, None);
        let bytes = builder.finished_data().to_vec();
        let message = HttpMessage::read(root_as_http_message(&bytes)?).await?;
        assert_eq!(
            message,
            HttpMessage {
                message_id: 1234,
                command: HttpCommand::Query {
                    query: "query".to_string(),
                    inline_tables: vec![InlineTable::new(
                        1,
                        "table".to_string(),
                        Arc::new(DataFrame::new(columns, rows.clone()))
                    )],
                    trace_obj: None,
                    parameters: None,
                    response_format: QueryResultFormat::Legacy,
                },
                connection_id: Some("foo".to_string()),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn arrow_response_format_round_trip() -> Result<(), CubeError> {
        use crate::queryplanner::query_executor::batches_to_dataframe;
        use crate::sql::timestamp_from_string;
        use crate::util::decimal::{Decimal, Decimal96};
        use crate::util::int96::Int96;
        use cubeshared::codegen::{root_as_http_message, HttpQueryResultData};
        use datafusion::arrow::ipc::reader::StreamReader;
        use datafusion::arrow::record_batch::RecordBatch;

        // 1. Build a DataFrame with every TableValue variant + nulls.
        let columns = vec![
            Column::new("c_string".to_string(), ColumnType::String, 0),
            Column::new("c_int".to_string(), ColumnType::Int, 1),
            Column::new("c_int96".to_string(), ColumnType::Int96, 2),
            Column::new(
                "c_decimal".to_string(),
                ColumnType::Decimal {
                    scale: 4,
                    precision: 18,
                },
                3,
            ),
            Column::new(
                "c_decimal96".to_string(),
                ColumnType::Decimal96 {
                    scale: 6,
                    precision: 38,
                },
                4,
            ),
            Column::new("c_float".to_string(), ColumnType::Float, 5),
            Column::new("c_bytes".to_string(), ColumnType::Bytes, 6),
            Column::new("c_timestamp".to_string(), ColumnType::Timestamp, 7),
            Column::new("c_bool".to_string(), ColumnType::Boolean, 8),
        ];
        let rows = vec![
            Row::new(vec![
                TableValue::String("hello".to_string()),
                TableValue::Int(42),
                TableValue::Int96(Int96::new(123_456_789_012_345_i128)),
                TableValue::Decimal(Decimal::new(12345)),
                TableValue::Decimal96(Decimal96::new(67890)),
                TableValue::Float(3.5_f64.into()),
                TableValue::Bytes(vec![0x01, 0x02, 0x03]),
                TableValue::Timestamp(timestamp_from_string("2024-01-15T10:30:45.123Z")?),
                TableValue::Boolean(true),
            ]),
            Row::new(vec![
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
            ]),
        ];
        let original_df = Arc::new(DataFrame::new(columns.clone(), rows));

        // 2. Drive process_command with response_format = Arrow.
        let svc = Arc::new(StubService(original_df.clone()));
        let resp = HttpServer::process_command(
            svc,
            SqlQueryContext::default(),
            "/unavailable/lease-agent",
            "/missing/promotion",
            HttpCommand::Query {
                query: "select 1".to_string(),
                inline_tables: vec![],
                trace_obj: None,
                parameters: None,
                response_format: QueryResultFormat::Arrow,
            },
        )
        .await?;
        let arrow_bytes = match resp {
            HttpCommand::QueryResultArrow { data } => data,
            other => panic!("expected QueryResultArrow, got: {:?}", other),
        };

        // 3. Round-trip through HttpMessage::bytes() and verify the wire shape.
        let wire = HttpMessage {
            message_id: 99,
            command: HttpCommand::QueryResultArrow {
                data: arrow_bytes.clone(),
            },
            connection_id: None,
        }
        .bytes();
        let parsed = root_as_http_message(&wire)?;
        assert_eq!(
            parsed.command_type(),
            cubeshared::codegen::HttpCommand::HttpQueryResult
        );
        let result = parsed.command_as_http_query_result().unwrap();
        assert_eq!(
            result.data_type(),
            HttpQueryResultData::HttpQueryResultArrow
        );
        let arrow = result.data_as_http_query_result_arrow().unwrap();
        let payload: Vec<u8> = arrow.data().iter().collect();
        assert_eq!(payload, arrow_bytes);

        // 4. Decode the Arrow IPC stream, round-trip back to a DataFrame
        let reader = StreamReader::try_new(std::io::Cursor::new(payload), None).unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();

        let decoded = batches_to_dataframe(batches)?;
        // we don't compare directly both dataframes, because there is a difference with decimal96
        assert_eq!(decoded.get_columns().len(), original_df.get_columns().len());
        assert_eq!(decoded.get_rows().len(), original_df.get_rows().len());

        insta::assert_snapshot!("arrow_response_format_round_trip", decoded.print());

        Ok(())
    }

    #[tokio::test]
    async fn arrow_response_format_zero_columns_completed() -> Result<(), CubeError> {
        use cubeshared::codegen::{root_as_http_message, HttpQueryResultData};

        // Write commands (CREATE TABLE/INSERT, queue/cache writes) produce a
        // result with zero columns. For Arrow-format requests there's no Arrow
        // stream to build, so the server answers with QueryResultCompleted.
        let empty_df = Arc::new(DataFrame::new(vec![], vec![]));

        let svc = Arc::new(StubService(empty_df));
        let resp = HttpServer::process_command(
            svc,
            SqlQueryContext::default(),
            "/unavailable/lease-agent",
            "/missing/promotion",
            HttpCommand::Query {
                query: "SELECT 1".to_string(),
                inline_tables: vec![],
                trace_obj: None,
                parameters: None,
                response_format: QueryResultFormat::Arrow,
            },
        )
        .await?;
        assert!(
            matches!(resp, HttpCommand::QueryResultCompleted),
            "expected QueryResultCompleted, got: {:?}",
            resp
        );

        // Round-trip through HttpMessage::bytes() and verify the wire shape.
        let wire = HttpMessage {
            message_id: 7,
            command: HttpCommand::QueryResultCompleted,
            connection_id: None,
        }
        .bytes();
        let parsed = root_as_http_message(&wire)?;
        assert_eq!(
            parsed.command_type(),
            cubeshared::codegen::HttpCommand::HttpQueryResult
        );
        let result = parsed.command_as_http_query_result().unwrap();
        assert_eq!(
            result.data_type(),
            HttpQueryResultData::HttpQueryResultCompleted
        );
        assert!(result.data_as_http_query_result_completed().is_some());

        Ok(())
    }

    #[test]
    fn lease_contract_fails_closed_for_unavailable_or_expired_agent() {
        assert!(HttpServer::read_local_lease("/missing/lease-agent").is_err());

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("leadership.json");
        std::fs::write(
            &path,
            serde_json::json!({
                "holderId": "router-a",
                "epoch": 7,
                "tokenHash": "sha256:stale",
                "issuedAt": "2026-08-03T10:00:00Z",
                "expiresAt": "2020-01-01T00:00:00Z"
            })
            .to_string(),
        )
        .unwrap();
        let error = HttpServer::read_local_lease(path.to_str().unwrap()).unwrap_err();
        assert!(error.contains("expired"), "unexpected error: {error}");
    }

    #[test]
    fn promotion_marker_requires_exact_holder_epoch_and_token_hash() {
        let lease = LocalLeaseFile {
            holder_id: "router-a".to_string(),
            epoch: 7,
            token_hash: HttpServer::hash_lease_token("current-token"),
            issued_at: "2026-08-03T10:00:00Z".to_string(),
            expires_at: "2099-01-01T00:00:00Z".to_string(),
        };
        let marker = PromotionMarker {
            active_leader: "router-a".to_string(),
            leader_epoch: 7,
            lease_cluster_id: "cube-router".to_string(),
            lease_epoch: 7,
            lease_token: "current-token".to_string(),
            meta_store_ready: true,
        };
        assert!(HttpServer::promotion_matches(&lease, &marker, "router-a"));

        let cases = vec![
            (
                "old holder",
                PromotionMarker {
                    active_leader: "router-b".to_string(),
                    ..PromotionMarker {
                        active_leader: marker.active_leader.clone(),
                        leader_epoch: marker.leader_epoch,
                        lease_cluster_id: marker.lease_cluster_id.clone(),
                        lease_epoch: marker.lease_epoch,
                        lease_token: marker.lease_token.clone(),
                        meta_store_ready: marker.meta_store_ready,
                    }
                },
            ),
            (
                "old leader epoch",
                PromotionMarker {
                    leader_epoch: 6,
                    ..PromotionMarker {
                        active_leader: marker.active_leader.clone(),
                        leader_epoch: marker.leader_epoch,
                        lease_cluster_id: marker.lease_cluster_id.clone(),
                        lease_epoch: marker.lease_epoch,
                        lease_token: marker.lease_token.clone(),
                        meta_store_ready: marker.meta_store_ready,
                    }
                },
            ),
            (
                "old lease epoch",
                PromotionMarker {
                    lease_epoch: 6,
                    ..PromotionMarker {
                        active_leader: marker.active_leader.clone(),
                        leader_epoch: marker.leader_epoch,
                        lease_cluster_id: marker.lease_cluster_id.clone(),
                        lease_epoch: marker.lease_epoch,
                        lease_token: marker.lease_token.clone(),
                        meta_store_ready: marker.meta_store_ready,
                    }
                },
            ),
            (
                "old token",
                PromotionMarker {
                    lease_token: "old-token".to_string(),
                    ..PromotionMarker {
                        active_leader: marker.active_leader.clone(),
                        leader_epoch: marker.leader_epoch,
                        lease_cluster_id: marker.lease_cluster_id.clone(),
                        lease_epoch: marker.lease_epoch,
                        lease_token: marker.lease_token.clone(),
                        meta_store_ready: marker.meta_store_ready,
                    }
                },
            ),
            (
                "metastore not ready",
                PromotionMarker {
                    active_leader: marker.active_leader.clone(),
                    leader_epoch: marker.leader_epoch,
                    lease_cluster_id: marker.lease_cluster_id.clone(),
                    lease_epoch: marker.lease_epoch,
                    lease_token: marker.lease_token.clone(),
                    meta_store_ready: false,
                },
            ),
        ];
        for (name, stale) in cases {
            assert!(
                !HttpServer::promotion_matches(&lease, &stale, "router-a"),
                "{name} promotion marker was accepted"
            );
        }
    }

    #[tokio::test]
    async fn read_query_remains_available_without_lease_agent() -> Result<(), CubeError> {
        let service = Arc::new(StubService(Arc::new(DataFrame::new(vec![], vec![]))));
        let response = HttpServer::process_command(
            service,
            SqlQueryContext::default(),
            "/missing/lease-agent",
            "/missing/promotion",
            HttpCommand::Query {
                query: "SELECT 1".to_string(),
                inline_tables: vec![],
                trace_obj: None,
                parameters: None,
                response_format: QueryResultFormat::Legacy,
            },
        )
        .await?;
        assert!(matches!(response, HttpCommand::ResultSet { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn upload_temp_file_is_fenced_when_lease_agent_is_unreachable() {
        let body = futures::stream::iter(vec![Ok::<Bytes, warp::Error>(Bytes::from_static(
            b"payload",
        ))]);
        let result = HttpServer::handle_upload(
            Arc::new(StubService(Arc::new(DataFrame::new(vec![], vec![])))),
            SqlQueryContext::default(),
            UploadQuery {
                name: "upload.csv".to_string(),
            },
            "/missing/lease-agent".to_string(),
            "/missing/promotion".to_string(),
            body,
        )
        .await;

        let rejection = match result {
            Ok(_) => panic!("upload must be fenced without a lease agent"),
            Err(rejection) => rejection,
        };
        assert!(matches!(
            rejection.find::<CubeRejection>(),
            Some(CubeRejection::LeaseFenced(error)) if error.contains("lease-agent unavailable")
        ));
    }

    pub struct SqlServiceMock {
        message_counter: AtomicU64,
    }

    crate::di_service!(SqlServiceMock, [SqlService]);

    #[async_trait]
    impl SqlService for SqlServiceMock {
        async fn exec_query(&self, _query: &str) -> Result<QueryResult, CubeError> {
            todo!()
        }

        async fn exec_query_with_context(
            &self,
            _context: SqlQueryContext,
            query: &str,
        ) -> Result<QueryResult, CubeError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let counter = self.message_counter.fetch_add(1, Ordering::Relaxed);
            if query == "SELECT close_connection" {
                Err(CubeError::wrong_connection("wrong connection".to_string()))
            } else if query == "SELECT error" {
                Err(CubeError::internal("error".to_string()))
            } else {
                Ok(QueryResult::Frame(Arc::new(DataFrame::new(
                    vec![Column::new("foo".to_string(), ColumnType::String, 0)],
                    vec![Row::new(vec![TableValue::String(format!("{}", counter))])],
                ))))
            }
        }

        async fn plan_query(&self, _query: &str) -> Result<QueryPlans, CubeError> {
            todo!()
        }

        async fn plan_query_with_context(
            &self,
            _context: SqlQueryContext,
            _query: &str,
        ) -> Result<QueryPlans, CubeError> {
            todo!()
        }

        async fn upload_temp_file(
            &self,
            _context: SqlQueryContext,
            _name: String,
            _file_path: &Path,
        ) -> Result<(), CubeError> {
            todo!()
        }

        async fn temp_uploads_dir(&self, _context: SqlQueryContext) -> Result<String, CubeError> {
            todo!()
        }
    }

    #[tokio::test]
    async fn ws_test() -> Result<(), CubeError> {
        init_test_logger().await;

        let sql_service = SqlServiceMock {
            message_counter: AtomicU64::new(0),
        };
        let mut auth = MockSqlAuthService::new();
        auth.expect_authenticate().return_const(Ok(None));

        let config = Config::test("ws_test").config_obj();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let bind_address = listener.local_addr().unwrap();
        drop(listener);
        let websocket_url: &'static str =
            Box::leak(format!("ws://{bind_address}/ws").into_boxed_str());

        let http_server = Arc::new(HttpServer::new(
            bind_address.to_string(),
            "/unavailable/lease-agent".to_string(),
            "/unavailable/promotion".to_string(),
            Arc::new(auth),
            Arc::new(sql_service),
            Duration::from_millis(100),
            Duration::from_millis(10000),
            Duration::from_millis(1000),
            config.transport_max_message_size(),
            config.transport_max_frame_size(),
        ));
        {
            let http_server = http_server.clone();
            cube_ext::spawn(async move { http_server.run_server().await });
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        async fn connect(url: &str) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
            let (socket, _) = connect_async(Url::parse(url).unwrap())
                .await
                .unwrap();
            socket
        }

        async fn send_query(
            socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
            message_id: u32,
            connection_id: Option<String>,
            query: &str,
        ) {
            socket
                .send(Message::binary(
                    HttpMessage {
                        message_id,
                        command: HttpCommand::Query {
                            query: query.to_string(),
                            inline_tables: vec![],
                            trace_obj: None,
                            parameters: None,
                            response_format: QueryResultFormat::Legacy,
                        },
                        connection_id,
                    }
                    .bytes(),
                ))
                .await
                .unwrap();
        }

        async fn connect_and_send_query(
            url: &str,
            message_id: u32,
            connection_id: Option<String>,
            query: &str,
        ) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
            let mut socket = connect(url).await;
            send_query(&mut socket, message_id, connection_id, query).await;
            socket
        }

        async fn connect_and_send(
            url: &str,
            message_id: u32,
            connection_id: Option<String>,
        ) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
            connect_and_send_query(url, message_id, connection_id, "SELECT 1").await
        }

        async fn assert_message(
            socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
            counter: &str,
        ) {
            let msg = socket.next().await.unwrap().unwrap();
            let message = HttpMessage::read(root_as_http_message(&msg.into_data()).unwrap())
                .await
                .unwrap();
            if let HttpCommand::ResultSet { data_frame } = message.command {
                if let TableValue::String(v) = data_frame
                    .get_rows()
                    .iter()
                    .next()
                    .unwrap()
                    .values()
                    .iter()
                    .next()
                    .unwrap()
                {
                    trace!("Message: {}", v.as_str());
                    assert_eq!(v.as_str(), counter);
                } else {
                    panic!("String expected");
                }
            } else {
                panic!("Result set expected");
            }
        }

        async fn assert_error(socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) {
            let msg = socket.next().await.unwrap().unwrap();
            let data = msg.into_data();
            let message = root_as_http_message(&data).unwrap();
            let error = message
                .command_as_http_error()
                .and_then(|error| error.error())
                .expect("WebSocket mutation must return an HttpError");
            assert!(
                error.contains("lease-agent unavailable"),
                "unexpected error: {error}"
            );
        }

        tokio::join!(
            // Two sockets for the same message
            async move {
                let mut socket = connect_and_send(websocket_url, 1, Some("foo".to_string())).await;
                assert_message(&mut socket, "0").await;
                socket.close(None).await.unwrap();
            },
            async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let mut socket = connect_and_send(websocket_url, 1, Some("foo".to_string())).await;
                assert_message(&mut socket, "0").await;
                socket.close(None).await.unwrap();
            },
            // Orphaned complete message
            async move {
                // takes message 1
                tokio::time::sleep(Duration::from_millis(300)).await;
                let mut socket = connect_and_send(websocket_url, 1, Some("bar".to_string())).await;
                socket.close(None).await.unwrap();
            },
            async move {
                tokio::time::sleep(Duration::from_millis(4000)).await;
                let mut socket = connect_and_send(websocket_url, 1, Some("bar".to_string())).await;
                assert_message(&mut socket, "5").await;
                socket.close(None).await.unwrap();
            },
            // Retrieve complete message
            async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                // takes message 2
                let mut socket = connect_and_send(websocket_url, 2, Some("foo".to_string())).await;
                socket.close(None).await.unwrap();
            },
            async move {
                tokio::time::sleep(Duration::from_millis(3000)).await;
                let mut socket = connect_and_send(websocket_url, 2, Some("foo".to_string())).await;
                assert_message(&mut socket, "2").await;
                socket.close(None).await.unwrap();
            },
            async move {
                tokio::time::sleep(Duration::from_millis(3500)).await;
                let mut socket = connect_and_send(websocket_url, 2, Some("foo".to_string())).await;
                assert_message(&mut socket, "4").await;
                socket.close(None).await.unwrap();
            },
            // First message but after resolved
            async move {
                tokio::time::sleep(Duration::from_millis(2500)).await;
                let mut socket = connect_and_send(websocket_url, 1, Some("foo".to_string())).await;
                assert_message(&mut socket, "3").await;
                socket.close(None).await.unwrap();
            },
        );

        tokio::time::sleep(Duration::from_millis(2500)).await;
        let mut socket = connect_and_send(websocket_url, 3, Some("foo".to_string())).await;
        assert_message(&mut socket, "6").await;

        let mut socket2 = connect_and_send(websocket_url, 3, Some("foo2".to_string())).await;
        assert_message(&mut socket2, "7").await;

        send_query(
            &mut socket,
            3,
            Some("foo".to_string()),
            "SELECT close_connection",
        )
        .await;
        socket.next().await.unwrap().unwrap();

        send_query(&mut socket2, 3, Some("foo".to_string()), "SELECT error").await;
        socket2.next().await.unwrap().unwrap();

        send_query(&mut socket, 3, Some("foo".to_string()), "SELECT 1").await;
        assert!(socket.next().await.unwrap().is_err());

        send_query(
            &mut socket2,
            4,
            Some("foo2".to_string()),
            "INSERT INTO foo VALUES (1)",
        )
        .await;
        assert_error(&mut socket2).await;

        let mut socket2 = connect_and_send(websocket_url, 3, Some("foo2".to_string())).await;
        assert_message(&mut socket2, "10").await;

        http_server.stop_processing().await;
        Ok(())
    }
}
