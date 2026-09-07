use super::{IndexId, RocksSecondaryIndex, TableId};
use crate::base_rocks_secondary_index;
use crate::metastore::table::Table;
use crate::metastore::{RocksEntity, RowKey};
use crate::rocks_table_impl;
use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, Utc};

use serde::{Deserialize, Deserializer, Serialize};
use std::io::{Cursor, Write};
use std::future::Future;
use crate::CubeError;

#[cfg(test)]
#[path = "job_attempt_rpc_tests.rs"]
mod rpc_tests;

/// Worker ownership, deliberately independent of the router leadership epoch.
/// The job id is never reused; reclaim preserves it and advances generation.
#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct JobAttempt {
    pub job_id: u64,
    pub generation: u64,
    pub owner: String,
}

tokio::task_local! {
    pub static JOB_ATTEMPT: Option<JobAttempt>;
}

pub fn current_job_attempt() -> Option<JobAttempt> {
    JOB_ATTEMPT.try_with(Clone::clone).ok().flatten()
}

/// Tokio task locals do not inherit through spawn. Job-owned async children must
/// use this wrapper, including children that outlive cancellation of their parent.
pub fn spawn_job<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let attempt = current_job_attempt();
    // This helper also serves router ingestion. Do not turn its children into
    // unguarded writes. Durable job owners, however, survive router epoch changes.
    let mutation = if attempt.is_none() {
        crate::sql::ha::current_mutation()
    } else {
        None
    };
    datafusion::cube_ext::spawn(JOB_ATTEMPT.scope(attempt, async move {
        match mutation {
            Some(guard) => guard.scope(future).await,
            None => future.await,
        }
    }))
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum JobType {
    WalPartitioning,
    PartitionCompaction,
    TableImport,
    Repartition,
    TableImportCSV(/*location*/ String),
    MultiPartitionSplit,
    FinishMultiSplit,
    RepartitionChunk,
    InMemoryChunksCompaction,
    NodeInMemoryChunksCompaction(/*node*/ String),
    // Repartition an inclusive [start, end] chunk-id range of an inactive parent in
    // one merge+swap. row_reference carries the start chunk; the end is data only and
    // is deliberately excluded from the job index key (see key_to_bytes) so a tail
    // that extends the trailing range dedups on the start instead of spawning a
    // second job for the same start.
    RepartitionRange(/*end_chunk_id*/ u64),
    /// Fallback for job types written by a newer binary that this binary does
    /// not know about. Lets the read path decode such rows instead of failing
    /// the whole job scan; the worker ignores `Unknown` jobs.
    #[serde(other)]
    Unknown,
}

fn get_job_type_index(j: &JobType) -> u32 {
    match j {
        JobType::WalPartitioning => 1,
        JobType::PartitionCompaction => 2,
        JobType::TableImport => 3,
        JobType::Repartition => 4,
        JobType::TableImportCSV(_) => 5,
        JobType::MultiPartitionSplit => 6,
        JobType::FinishMultiSplit => 7,
        JobType::RepartitionChunk => 8,
        JobType::InMemoryChunksCompaction => 9,
        JobType::NodeInMemoryChunksCompaction(_) => 10,
        JobType::Unknown => 11,
        JobType::RepartitionRange(_) => 12,
    }
}

/// Get the priority of a job type. Higher numbers are higher priority.
fn get_job_type_priority(j: &JobType) -> u32 {
    match j {
        JobType::WalPartitioning => 1000,
        JobType::PartitionCompaction => 1000,
        JobType::TableImport => 1000,
        JobType::Repartition => 1000,
        JobType::TableImportCSV(_) => 1000,
        JobType::MultiPartitionSplit => 1000,
        JobType::FinishMultiSplit => 1000,
        JobType::RepartitionChunk => 1000,
        JobType::RepartitionRange(_) => 1000,
        JobType::InMemoryChunksCompaction => 10000,
        JobType::NodeInMemoryChunksCompaction(_) => 10000,
        JobType::Unknown => 0,
    }
}

/// A job runner pool. Each runner serves exactly one pool and only picks up jobs that
/// belong to it. `CsvImport` overlaps with `Regular`: non-stream CSV imports stay eligible
/// for regular runners and additionally get a reserved pool so they progress even when all
/// regular runners are busy with compaction/repartition.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobRunnerPool {
    Regular,
    LongTerm,
    CsvImport,
}

#[derive(Clone, Serialize, Deserialize, Debug, Hash, Eq, PartialEq)]
pub enum JobStatus {
    Scheduled(String),
    ProcessingBy(String),
    Completed,
    Timeout,
    Error(String),
    Orphaned,
}

#[derive(Clone, Serialize, Deserialize, Debug, Hash)]
pub struct Job {
    row_reference: RowKey,
    job_type: JobType,
    last_heart_beat: DateTime<Utc>,
    status: JobStatus,
    #[serde(default)]
    attempt: Option<JobAttempt>,
    /// Durable receipts for complete file locations, committed with their chunks.
    /// Retained across reclaim and after completion until the table is dropped.
    #[serde(default)]
    completed_imports: Vec<String>,
}

impl RocksEntity for Job {}

impl Job {
    pub fn new(row_reference: RowKey, job_type: JobType, shard: String) -> Job {
        Job {
            row_reference,
            job_type,
            last_heart_beat: Utc::now(),
            status: JobStatus::Scheduled(shard),
            attempt: None,
            completed_imports: Vec::new(),
        }
    }

    pub fn job_type(&self) -> &JobType {
        &self.job_type
    }

    pub fn row_reference(&self) -> &RowKey {
        &self.row_reference
    }

    pub fn last_heart_beat(&self) -> &DateTime<Utc> {
        &self.last_heart_beat
    }

    pub fn status(&self) -> &JobStatus {
        &self.status
    }

    pub fn update_status(&self, status: JobStatus) -> Job {
        let mut job = self.clone();
        job.last_heart_beat = Utc::now();
        job.status = status;
        job
    }

    pub fn start_processing(&self, job_id: u64, node_name: String) -> Result<Job, CubeError> {
        let generation = self.attempt.as_ref().map_or(0, |a| a.generation)
            .checked_add(1)
            .ok_or_else(|| CubeError::internal("Job generation exhausted".to_string()))?;
        let mut job = self.update_status(JobStatus::ProcessingBy(node_name.clone()));
        job.attempt = Some(JobAttempt { job_id, generation, owner: node_name });
        Ok(job)
    }

    pub fn attempt(&self) -> Option<&JobAttempt> {
        self.attempt.as_ref()
    }

    pub fn completed_imports(&self) -> &[String] {
        &self.completed_imports
    }

    pub fn with_import_receipt(&self, location: String) -> Job {
        let mut job = self.clone();
        if !job.completed_imports.contains(&location) {
            job.completed_imports.push(location);
        }
        job
    }

    pub fn check_owner(&self, attempt: &JobAttempt) -> Result<(), CubeError> {
        if self.attempt.as_ref() != Some(attempt)
            || self.status != JobStatus::ProcessingBy(attempt.owner.clone())
        {
            return Err(CubeError::user(format!("Stale job attempt: {:?}", attempt)));
        }
        Ok(())
    }

    pub fn is_file_import(&self) -> bool {
        matches!(self.job_type, JobType::TableImport) || self.is_csv_import()
    }

    pub fn update_heart_beat(&self) -> Job {
        self.update_status(self.status.clone())
    }

    pub fn completed(&self) -> Job {
        self.update_status(JobStatus::Completed)
    }

    pub fn is_long_term(&self) -> bool {
        match &self.job_type {
            JobType::TableImportCSV(location) if Table::is_stream_location(location) => true,
            _ => false,
        }
    }

    pub fn is_csv_import(&self) -> bool {
        matches!(
            &self.job_type,
            JobType::TableImportCSV(location) if !Table::is_stream_location(location)
        )
    }

    pub fn matches_pool(&self, pool: JobRunnerPool) -> bool {
        match pool {
            JobRunnerPool::Regular => !self.is_long_term(),
            JobRunnerPool::LongTerm => self.is_long_term(),
            JobRunnerPool::CsvImport => self.is_csv_import(),
        }
    }

    pub fn priority(&self) -> u32 {
        get_job_type_priority(&self.job_type)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum JobRocksIndex {
    RowReference = 1,
    ByShard,
}

base_rocks_secondary_index!(Job, JobRocksIndex);

rocks_table_impl!(Job, JobRocksTable, TableId::Jobs, {
    vec![
        Box::new(JobRocksIndex::RowReference),
        Box::new(JobRocksIndex::ByShard),
    ]
});

#[derive(Hash, Clone, Debug)]
pub enum JobIndexKey {
    RowReference(RowKey, JobType),
    ScheduledByShard(Option<String>),
}

impl RocksSecondaryIndex<Job, JobIndexKey> for JobRocksIndex {
    fn typed_key_by(&self, row: &Job) -> JobIndexKey {
        match self {
            JobRocksIndex::RowReference => {
                JobIndexKey::RowReference(row.row_reference.clone(), row.job_type.clone())
            }
            JobRocksIndex::ByShard => match &row.status {
                JobStatus::Scheduled(shard) => {
                    JobIndexKey::ScheduledByShard(Some(shard.to_string()))
                }
                _ => JobIndexKey::ScheduledByShard(None),
            },
        }
    }

    fn key_to_bytes(&self, key: &JobIndexKey) -> Vec<u8> {
        match key {
            JobIndexKey::RowReference(row_key, job_type) => {
                let mut buf = Cursor::new(Vec::new());
                buf.write_all(row_key.to_bytes().as_slice()).unwrap();
                buf.write_u32::<BigEndian>(get_job_type_index(job_type))
                    .unwrap();
                match job_type {
                    JobType::TableImportCSV(l) | JobType::NodeInMemoryChunksCompaction(l) => {
                        buf.write_u64::<BigEndian>(l.len() as u64).unwrap();
                        buf.write(l.as_bytes()).unwrap();
                    }
                    _ => {}
                }
                buf.into_inner()
            }
            JobIndexKey::ScheduledByShard(shard) => {
                let mut buf = Cursor::new(Vec::new());
                buf.write_u32::<BigEndian>(shard.as_ref().map(|s| s.len() as u32).unwrap_or(0))
                    .unwrap();
                if let Some(v) = shard {
                    buf.write_all(v.as_bytes()).unwrap();
                }
                buf.into_inner()
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            JobRocksIndex::RowReference => true,
            JobRocksIndex::ByShard => false,
        }
    }

    fn version(&self) -> u32 {
        match self {
            JobRocksIndex::RowReference => 1,
            JobRocksIndex::ByShard => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    fn job(job_type: JobType) -> Job {
        Job::new(
            RowKey::Table(TableId::Tables, 1),
            job_type,
            "node".to_string(),
        )
    }

    #[test]
    fn matches_pool_routing() {
        let file_import = job(JobType::TableImportCSV("file.csv".to_string()));
        let stream_import = job(JobType::TableImportCSV("stream:topic".to_string()));
        let compaction = job(JobType::PartitionCompaction);

        // Non-stream CSV import stays eligible for the regular pool and additionally
        // for the reserved CSV pool, but is never long-term.
        assert!(file_import.matches_pool(JobRunnerPool::Regular));
        assert!(file_import.matches_pool(JobRunnerPool::CsvImport));
        assert!(!file_import.matches_pool(JobRunnerPool::LongTerm));

        // Streaming CSV import is long-term only.
        assert!(!stream_import.matches_pool(JobRunnerPool::Regular));
        assert!(!stream_import.matches_pool(JobRunnerPool::CsvImport));
        assert!(stream_import.matches_pool(JobRunnerPool::LongTerm));

        // Everything else belongs to the regular pool only.
        assert!(compaction.matches_pool(JobRunnerPool::Regular));
        assert!(!compaction.matches_pool(JobRunnerPool::CsvImport));
        assert!(!compaction.matches_pool(JobRunnerPool::LongTerm));
    }

    /// Mirrors `JobType` as it might look in a newer binary: it carries extra
    /// variants this binary does not know about (one unit, one data-carrying).
    /// We serialize with this enum and deserialize with the real `JobType` to
    /// emulate a forward version skew across `latest`/`release` channels.
    #[derive(Serialize)]
    enum NewerJobType {
        #[allow(dead_code)]
        WalPartitioning,
        #[allow(dead_code)]
        NodeInMemoryChunksCompaction(String),
        BrandNewUnitVariant,
        BrandNewDataVariant(String),
    }

    fn flex_roundtrip<S: Serialize, D: for<'de> Deserialize<'de>>(value: &S) -> Result<D, String> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        value.serialize(&mut ser).map_err(|e| e.to_string())?;
        let buffer = ser.take_buffer();
        let reader = flexbuffers::Reader::get_root(buffer.as_slice()).map_err(|e| e.to_string())?;
        D::deserialize(reader).map_err(|e| e.to_string())
    }

    #[test]
    fn unknown_unit_variant_decodes_to_unknown() {
        let decoded: JobType =
            flex_roundtrip(&NewerJobType::BrandNewUnitVariant).expect("unit variant must decode");
        assert_eq!(decoded, JobType::Unknown);
    }

    #[test]
    fn unknown_data_variant_decodes_to_unknown() {
        // A data-carrying unknown variant. flexbuffers must skip the payload and
        // land on `Unknown` rather than erroring; if this fails, `#[serde(other)]`
        // is insufficient for the job read path and Option B is required.
        let decoded: JobType =
            flex_roundtrip(&NewerJobType::BrandNewDataVariant("loc".to_string()))
                .expect("data variant must decode");
        assert_eq!(decoded, JobType::Unknown);
    }

    #[test]
    fn known_variants_still_roundtrip() {
        for jt in [
            JobType::WalPartitioning,
            JobType::TableImportCSV("s3://x".to_string()),
            JobType::NodeInMemoryChunksCompaction("node-1".to_string()),
        ] {
            let decoded: JobType = flex_roundtrip(&jt).expect("known variant must decode");
            assert_eq!(decoded, jt);
        }
    }

    #[test]
    fn unknown_variant_inside_job_struct_decodes() {
        // The real read path deserializes the whole `Job` struct, so verify the
        // fallback also works when the unknown enum is a nested field.
        #[derive(Serialize)]
        struct NewerJob {
            row_reference: RowKey,
            job_type: NewerJobType,
            last_heart_beat: DateTime<Utc>,
            status: JobStatus,
        }

        let newer = NewerJob {
            row_reference: RowKey::Table(TableId::Jobs, 1),
            job_type: NewerJobType::BrandNewDataVariant("payload".to_string()),
            last_heart_beat: Utc::now(),
            status: JobStatus::Scheduled("shard-1".to_string()),
        };
        let decoded: Job = flex_roundtrip(&newer).expect("job with unknown type must decode");
        assert_eq!(decoded.job_type(), &JobType::Unknown);
        assert!(matches!(decoded.status(), JobStatus::Scheduled(s) if s == "shard-1"));
        assert!(decoded.attempt().is_none());
        assert!(decoded.completed_imports().is_empty());
    }

    #[test]
    fn job_attempt_generation_preserves_dedupe_and_receipts() {
        let first = job(JobType::TableImportCSV("file.csv".to_string()))
            .start_processing(42, "worker".to_string()).unwrap();
        let receipt = first.with_import_receipt("file.csv".to_string());
        let next = receipt.update_status(JobStatus::Scheduled("worker".to_string()))
            .start_processing(42, "worker".to_string()).unwrap();
        assert_eq!(next.attempt().unwrap().generation, 2);
        assert_eq!(next.attempt().unwrap().job_id, 42);
        assert_eq!(next.row_reference(), first.row_reference());
        assert_eq!(next.completed_imports(), &["file.csv".to_string()]);
        assert!(next.check_owner(first.attempt().unwrap()).is_err());
        assert!(next.check_owner(next.attempt().unwrap()).is_ok());
        assert!(next.completed().check_owner(next.attempt().unwrap()).is_err());
        let decoded: Job = flex_roundtrip(&next).unwrap();
        assert_eq!(decoded.attempt(), next.attempt());
        assert_eq!(decoded.completed_imports(), next.completed_imports());
    }

    #[tokio::test]
    async fn job_attempt_spawn_inherits_and_abort_cancels() {
        let attempt = JobAttempt { job_id: 1, generation: 9, owner: "worker".to_string() };
        let expected = attempt.clone();
        JOB_ATTEMPT.scope(Some(attempt), async move {
            let actual = spawn_job(async { current_job_attempt() }).await.unwrap();
            assert_eq!(actual, Some(expected));
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
            struct OnDrop(Option<tokio::sync::oneshot::Sender<()>>);
            impl Drop for OnDrop {
                fn drop(&mut self) { let _ = self.0.take().unwrap().send(()); }
            }
            let handle = crate::util::aborting_join_handle::AbortingJoinHandle::new(spawn_job(async move {
                let _on_drop = OnDrop(Some(dropped_tx));
                started_tx.send(()).unwrap();
                std::future::pending::<()>().await;
            }));
            started_rx.await.unwrap();
            drop(handle);
            tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx).await.unwrap().unwrap();
        }).await;
        assert!(current_job_attempt().is_none());
    }
}
