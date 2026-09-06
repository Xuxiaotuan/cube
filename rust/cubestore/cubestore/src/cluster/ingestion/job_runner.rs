use crate::cluster::ingestion::job_processor::JobProcessor;
use crate::cluster::rate_limiter::{ProcessRateLimiter, TaskType, TraceIndex};
use crate::config::ConfigObj;
use crate::import::ImportService;
use crate::metastore::job::{Job, JobRunnerPool, JobStatus, JobType, JOB_ATTEMPT};
use crate::metastore::table::Table;
use crate::metastore::{IdRow, MetaStore, RowKey, TableId};
use crate::queryplanner::trace_data_loaded::DataLoadedSize;
use crate::store::compaction::CompactionService;
use crate::store::ChunkDataStore;
use crate::util::aborting_join_handle::AbortingJoinHandle;
use crate::CubeError;
use futures_timer::Delay;
use log::{debug, error, info};
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

pub struct JobRunner {
    pub config_obj: Arc<dyn ConfigObj>,
    pub meta_store: Arc<dyn MetaStore>,
    pub chunk_store: Arc<dyn ChunkDataStore>,
    pub compaction_service: Arc<dyn CompactionService>,
    pub import_service: Arc<dyn ImportService>,
    pub process_rate_limiter: Arc<dyn ProcessRateLimiter>,
    pub server_name: String,
    pub notify: Arc<Notify>,
    pub stop_token: CancellationToken,
    pub pool: JobRunnerPool,
    pub job_processor: Arc<dyn JobProcessor>,
}

impl JobRunner {
    pub async fn processing_loop(&self) {
        loop {
            let res = tokio::select! {
                _ = self.stop_token.cancelled() => {
                    let _ = self.job_processor.stop_processing_loops().await;
                    return;
                }
                _ = self.notify.notified() => {
                    self.fetch_and_process().await
                }
                _ = Delay::new(Duration::from_secs(5)) => {
                    // Notifications are hints: router loss must not strand durable jobs.
                    self.fetch_and_process().await
                }
            };
            if let Err(e) = res {
                error!("Error in processing loop: {}", e);
            }
        }
    }

    async fn fetch_and_process(&self) -> Result<(), CubeError> {
        let job = self
            .meta_store
            .start_processing_job(self.server_name.to_string(), self.pool)
            .await?;
        if let Some(to_process) = job {
            self.run_local(to_process).await?;
            // In case of job queue is in place jump to the next job immediately
            self.notify.notify_one();
        }
        Ok(())
    }

    fn job_timeout(&self, job: &IdRow<Job>) -> Option<Duration> {
        if let JobType::TableImportCSV(location) = job.get_row().job_type() {
            if Table::is_stream_location(location) {
                return None;
            }
        }
        Some(Duration::from_secs(self.config_obj.import_job_timeout()))
    }

    async fn run_local(&self, job: IdRow<Job>) -> Result<(), CubeError> {
        let start = SystemTime::now();
        let job_id = job.get_id();
        let attempt = job.get_row().attempt().cloned().ok_or_else(|| {
            CubeError::internal("Claimed job has no attempt token".to_string())
        })?;
        debug!("Running job: {:?}", job);
        let routed = JOB_ATTEMPT.scope(Some(attempt.clone()), async {
            self.route_job(job.get_row())
        }).await;
        let status = match routed {
            Err(e) => JobStatus::Error(e.to_string()),
            Ok(handle) => {
                // Dropping work on heartbeat failure/timeout aborts its outer task.
                // Already-running children still cannot commit after reclaim.
                let handle = AbortingJoinHandle::new(handle);
                let work = async {
                    if let Some(duration) = self.job_timeout(&job) {
                        match timeout(duration, handle).await {
                            Err(_) => JobStatus::Timeout,
                            Ok(Err(e)) => JobStatus::Error(e.to_string()),
                            Ok(Ok(Err(e))) => JobStatus::Error(e.to_string()),
                            Ok(Ok(Ok(()))) => JobStatus::Completed,
                        }
                    } else {
                        match handle.await {
                            Err(e) => JobStatus::Error(e.to_string()),
                            Ok(Err(e)) => JobStatus::Error(e.to_string()),
                            Ok(Ok(())) => JobStatus::Completed,
                        }
                    }
                };
                tokio::pin!(work);
                let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
                loop {
                    tokio::select! {
                        _ = self.stop_token.cancelled() => break JobStatus::Timeout,
                        result = &mut work => break result,
                        _ = heartbeat.tick() => {
                            match timeout(Duration::from_secs(30), self.meta_store.heartbeat_job_attempt(attempt.clone())).await {
                                Ok(Ok(_)) => {},
                                failure => {
                                    error!("Lost job ownership/heartbeat {:?}: {:?}", attempt, failure);
                                    break JobStatus::Timeout;
                                }
                            }
                        }
                    }
                }
            }
        };
        let completed = self.meta_store.finish_job_attempt(attempt, status.clone()).await?;
        if status == JobStatus::Completed {
            info!("Running job completed ({:?}): {:?}", start.elapsed()?, completed);
        } else {
            error!("Job {} stopped ({:?}): {:?}", job_id, start.elapsed()?, status);
        }
        Ok(())
    }

    fn route_job(&self, job: &Job) -> Result<JoinHandle<Result<(), CubeError>>, CubeError> {
        // spawn here is required in case there's a panic in a job. If job panics worker process loop will survive it.
        match job.job_type() {
            JobType::WalPartitioning => {
                if let RowKey::Table(TableId::WALs, wal_id) = job.row_reference() {
                    let chunk_store = self.chunk_store.clone();
                    let wal_id = *wal_id;
                    Ok(crate::metastore::job::spawn_job(async move {
                        chunk_store.partition(wal_id).await
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::Repartition => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    let chunk_store = self.chunk_store.clone();
                    let partition_id = *partition_id;
                    Ok(crate::metastore::job::spawn_job(async move {
                        chunk_store.repartition(partition_id).await
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::PartitionCompaction => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    let partition_id = *partition_id;
                    let process_rate_limiter = self.process_rate_limiter.clone();
                    let timeout = Some(Duration::from_secs(self.config_obj.import_job_timeout()));
                    let metastore = self.meta_store.clone();
                    let job_processor = self.job_processor.clone();
                    let job_to_move = job.clone();
                    Ok(crate::metastore::job::spawn_job(async move {
                        let wait_ms = process_rate_limiter
                            .wait_for_allow(TaskType::Job, timeout)
                            .await?; //TODO config, may be same ad orphaned timeout

                        let (_, _, table, _) =
                            metastore.get_partition_for_compaction(partition_id).await?;
                        let table_id = table.get_id();
                        let trace_obj = metastore.get_trace_obj_by_table_id(table_id).await?;
                        let trace_index = TraceIndex {
                            table_id: Some(table_id),
                            trace_obj,
                        };

                        match job_processor.process_job(job_to_move).await {
                            Ok(job_res) => {
                                process_rate_limiter
                                    .commit_task_usage(
                                        TaskType::Job,
                                        job_res.data_loaded_size() as i64,
                                        wait_ms,
                                        trace_index,
                                    )
                                    .await;
                                Ok(())
                            }
                            Err(e) => Err(e),
                        }
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::InMemoryChunksCompaction => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    let compaction_service = self.compaction_service.clone();
                    let partition_id = *partition_id;
                    log::warn!(
                        "JobType::InMemoryChunksCompaction is deprecated and should not be used"
                    );
                    Ok(crate::metastore::job::spawn_job(async move {
                        compaction_service
                            .compact_in_memory_chunks(partition_id)
                            .await
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::NodeInMemoryChunksCompaction(_) => {
                if let RowKey::Table(TableId::Tables, _) = job.row_reference() {
                    let compaction_service = self.compaction_service.clone();
                    let node_name = self.server_name.clone();
                    Ok(crate::metastore::job::spawn_job(async move {
                        compaction_service
                            .compact_node_in_memory_chunks(node_name)
                            .await
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::MultiPartitionSplit => {
                if let RowKey::Table(TableId::MultiPartitions, _) = job.row_reference() {
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(crate::metastore::job::spawn_job(async move {
                        job_processor.process_job(job_to_move).await.map(|_| ())
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::FinishMultiSplit => {
                if let RowKey::Table(TableId::MultiPartitions, _) = job.row_reference() {
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(crate::metastore::job::spawn_job(async move {
                        job_processor.process_job(job_to_move).await.map(|_| ())
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::TableImport => {
                if let RowKey::Table(TableId::Tables, _) = job.row_reference() {
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(crate::metastore::job::spawn_job(async move {
                        job_processor.process_job(job_to_move).await.map(|_| ())
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::TableImportCSV(location) => {
                if let RowKey::Table(TableId::Tables, table_id) = job.row_reference() {
                    let table_id = *table_id;
                    let import_service = self.import_service.clone();
                    let location = location.to_string();
                    let process_rate_limiter = self.process_rate_limiter.clone();
                    let timeout = Some(Duration::from_secs(self.config_obj.import_job_timeout()));
                    let metastore = self.meta_store.clone();
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(crate::metastore::job::spawn_job(async move {
                        let is_streaming = Table::is_stream_location(&location);
                        let data_loaded_size = if is_streaming {
                            None
                        } else {
                            Some(DataLoadedSize::new())
                        };
                        if !is_streaming {
                            let wait_ms = process_rate_limiter
                                .wait_for_allow(TaskType::Job, timeout)
                                .await?; //TODO config, may be same ad orphaned timeout
                            match job_processor.process_job(job_to_move).await {
                                Ok(job_res) => {
                                    let trace_obj =
                                        metastore.get_trace_obj_by_table_id(table_id).await?;
                                    let trace_index = TraceIndex {
                                        table_id: Some(table_id),
                                        trace_obj,
                                    };
                                    process_rate_limiter
                                        .commit_task_usage(
                                            TaskType::Job,
                                            job_res.data_loaded_size() as i64,
                                            wait_ms,
                                            trace_index,
                                        )
                                        .await;
                                    Ok(())
                                }
                                Err(e) => Err(e),
                            }
                        } else {
                            import_service
                                .clone()
                                .import_table_part(table_id, &location, data_loaded_size.clone())
                                .await
                        }
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::RepartitionChunk => {
                if let RowKey::Table(TableId::Chunks, chunk_id) = job.row_reference() {
                    let chunk_store = self.chunk_store.clone();
                    let chunk_id = *chunk_id;
                    let process_rate_limiter = self.process_rate_limiter.clone();
                    let timeout = Some(Duration::from_secs(self.config_obj.import_job_timeout()));
                    let metastore = self.meta_store.clone();
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(crate::metastore::job::spawn_job(async move {
                        let wait_ms = process_rate_limiter
                            .wait_for_allow(TaskType::Job, timeout)
                            .await?; //TODO config, may be same ad orphaned timeout
                        let chunk = metastore.get_chunk(chunk_id).await?;
                        if !chunk.get_row().in_memory() {
                            let (_, _, table, _) = metastore
                                .get_partition_for_compaction(chunk.get_row().get_partition_id())
                                .await?;
                            let table_id = table.get_id();
                            let trace_obj = metastore.get_trace_obj_by_table_id(table_id).await?;
                            let trace_index = TraceIndex {
                                table_id: Some(table_id),
                                trace_obj,
                            };
                            match job_processor.process_job(job_to_move).await {
                                Ok(job_res) => {
                                    process_rate_limiter
                                        .commit_task_usage(
                                            TaskType::Job,
                                            job_res.data_loaded_size() as i64,
                                            wait_ms,
                                            trace_index,
                                        )
                                        .await;
                                    Ok(())
                                }
                                Err(e) => Err(e),
                            }
                        } else {
                            chunk_store
                                .repartition_chunk(chunk_id, DataLoadedSize::new())
                                .await
                        }
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::RepartitionRange(_) => {
                if let RowKey::Table(TableId::Chunks, start_chunk_id) = job.row_reference() {
                    let start_chunk_id = *start_chunk_id;
                    let process_rate_limiter = self.process_rate_limiter.clone();
                    let timeout = Some(Duration::from_secs(self.config_obj.import_job_timeout()));
                    let metastore = self.meta_store.clone();
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(crate::metastore::job::spawn_job(async move {
                        let wait_ms = process_rate_limiter
                            .wait_for_allow(TaskType::Job, timeout)
                            .await?;
                        let chunk = metastore.get_chunk(start_chunk_id).await?;
                        let (_, _, table, _) = metastore
                            .get_partition_for_compaction(chunk.get_row().get_partition_id())
                            .await?;
                        let table_id = table.get_id();
                        let trace_obj = metastore.get_trace_obj_by_table_id(table_id).await?;
                        let trace_index = TraceIndex {
                            table_id: Some(table_id),
                            trace_obj,
                        };
                        match job_processor.process_job(job_to_move).await {
                            Ok(job_res) => {
                                process_rate_limiter
                                    .commit_task_usage(
                                        TaskType::Job,
                                        job_res.data_loaded_size() as i64,
                                        wait_ms,
                                        trace_index,
                                    )
                                    .await;
                                Ok(())
                            }
                            Err(e) => Err(e),
                        }
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            // Defense-in-depth: start_processing_job never selects an Unknown job, so
            // this arm is not a live path — it just guarantees we never panic on one.
            JobType::Unknown => Err(CubeError::internal(format!(
                "Unknown job type, likely written by a newer CubeStore version: {:?}",
                job
            ))),
        }
    }

    fn fail_job_row_key(job: &Job) -> Result<JoinHandle<Result<(), CubeError>>, CubeError> {
        Err(CubeError::internal(format!(
            "Incorrect row key for {:?}: {:?}",
            job,
            job.row_reference()
        )))
    }
}
