//! Opt-in, single-writer ledger. SQL execution pins physical table ids before
//! dispatch, including detached cache refreshes. Unknown termination retains
//! references; they must never be removed on a timer or process disappearance.
//! Rows and lookup indexes are independent, bounded records, not a JSON root.
//! Like router authority, these private rows use the existing RocksDB batch.
//! No TTL: reference/request tombstones must survive retries and restarts.

use super::*;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

#[path = "pre_aggregation_query_refs.rs"]
mod query_refs;
pub(super) use query_refs::{acquire_query_refs, release_query_refs};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LedgerQueryBinding { pub table_id: String, pub key: String, pub generation: String }

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LedgerQueryRefs {
    pub query_id: String,
    pub table_ids: Vec<String>,
    pub bindings: Vec<LedgerQueryBinding>,
    pub released: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LedgerCreateColumn { pub name: String, pub column_type: String }

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LedgerCreateIndex {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(rename = "type")]
    pub index_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LedgerCreate {
    pub schema: String,
    pub table: String,
    pub columns: Vec<LedgerCreateColumn>,
    pub locations: Vec<String>,
    pub indexes: Vec<LedgerCreateIndex>,
    pub import_format: String,
    pub content_version: String,
    pub structure_version: String,
    pub aggregations: Option<Vec<String>>,
    pub build_range_end: Option<String>,
    pub delimiter: Option<String>,
    pub disable_quoting: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LedgerManifest {
    pub table_id: String,
    pub schema: String,
    pub table: String,
    pub locations: Vec<String>,
    pub content_version: String,
    pub structure_version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LedgerRecord {
    pub key: String,
    pub generation: String,
    pub owner: String,
    pub lease_until_millis: String,
    pub state: LedgerState,
    pub manifest: Option<LedgerManifest>,
    pub references: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LedgerState { Claimed, Bound, Published, Retired, Dropped }

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]
pub enum LedgerCommand {
    Create { key: String, generation: String, owner: String, create: LedgerCreate },
    Claim { key: String, owner: String, lease_millis: String, expected_generation: Option<String> },
    Renew { key: String, generation: String, owner: String, lease_millis: String },
    Bind { key: String, generation: String, owner: String, manifest: LedgerManifest },
    Publish { key: String, generation: String, owner: String },
    Retire { key: String, generation: String, owner: String },
    Drop { key: String, generation: String, owner: String },
    Acquire { key: String, generation: String, reference_id: String },
    Release { key: String, generation: String, reference_id: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LedgerRequest { pub request_id: String, pub command: LedgerCommand }

/// A rejection is durable too. Transport/storage failures remain CubeError.
/// A replay returns the historical result, NOT a renewed lease/reference.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LedgerResult { pub record: Option<LedgerRecord>, pub rejection: Option<String> }

#[derive(Serialize, Deserialize)]
struct Head { key: String, generation: String }
#[derive(Serialize, Deserialize)]
struct Receipt { request: LedgerRequest, result: LedgerResult }
#[derive(Serialize, Deserialize)]
struct Reference { key: String, generation: String, reference_id: String, released: bool }
#[derive(Serialize, Deserialize)]
struct Binding {
    table_id: u64, key: String, generation: String,
    #[serde(default)]
    created: bool,
}

// Only scoped synchronously inside the authorized ledger write closure, never
// returned over RPC. The delete and ledger transition commit in the SAME batch.
tokio::task_local! { static DROPPING: String; }

fn reject(code: &str) -> CubeError { CubeError::user(format!("LEDGER_{}", code)) }

fn number(s: &str) -> Result<u64, CubeError> {
    let n = s.parse::<u64>().map_err(|_| reject("INVALID_NUMBER"))?;
    if n.to_string() != s { return Err(reject("INVALID_NUMBER")); }
    Ok(n)
}

fn identifier(s: &str) -> Result<(), CubeError> {
    if s.is_empty() || s.len() > 1024 || s.chars().any(char::is_control) {
        return Err(reject("INVALID_IDENTIFIER"));
    }
    Ok(())
}

// Fixed-width primary lookup indexes. Every lookup compares the complete key;
// a digest collision is rejected, never overwritten or treated as a match.
fn slot(parts: &[&str]) -> u64 {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    let bytes = hash.finalize();
    u64::from_be_bytes(bytes[..8].try_into().unwrap())
}

fn read<T: DeserializeOwned>(db: &DbTableRef, table: TableId, id: u64) -> Result<Option<T>, CubeError> {
    db.snapshot.get(RowKey::Table(table, id).to_bytes())?
        .map(|bytes| serde_json::from_slice(&bytes).map_err(|e| CubeError::internal(e.to_string())))
        .transpose()
}

fn put<T: Serialize, S>(pipe: &mut BatchPipe<'_, S>, table: TableId, id: u64, value: &T) -> Result<(), CubeError> {
    let bytes = serde_json::to_vec(value).map_err(|e| CubeError::internal(e.to_string()))?;
    pipe.batch().put(RowKey::Table(table, id).to_bytes(), bytes);
    Ok(())
}

fn head(db: &DbTableRef, key: &str) -> Result<Option<Head>, CubeError> {
    let result: Option<Head> = read(db, TableId::PreAggregationHeads, slot(&[key]))?;
    if result.as_ref().map(|r| r.key != key).unwrap_or(false) { return Err(reject("INDEX_COLLISION")); }
    Ok(result)
}

pub(super) fn get(db: &DbTableRef, key: &str, generation: Option<&str>) -> Result<Option<LedgerRecord>, CubeError> {
    identifier(key)?;
    let generation = match generation {
        Some(g) => g.to_string(),
        None => match head(db, key)? { Some(h) => h.generation, None => return Ok(None) },
    };
    let result: Option<LedgerRecord> = read(db, TableId::PreAggregationBuilds, number(&generation)?)?;
    if result.as_ref().map(|r| r.key != key || r.generation != generation).unwrap_or(false) {
        return Err(reject("BUILD_IDENTITY_MISMATCH"));
    }
    Ok(result)
}

fn required(db: &DbTableRef, key: &str, generation: &str) -> Result<LedgerRecord, CubeError> {
    get(db, key, Some(generation))?.ok_or_else(|| reject("NOT_FOUND"))
}

fn owned(record: &LedgerRecord, owner: &str) -> Result<(), CubeError> {
    if record.owner != owner { return Err(reject("OWNER_MISMATCH")); }
    Ok(())
}

fn live(db: &DbTableRef, record: &LedgerRecord, now: u64) -> Result<(), CubeError> {
    if head(db, &record.key)?.map(|h| h.generation) != Some(record.generation.clone()) {
        return Err(reject("STALE_GENERATION"));
    }
    if number(&record.lease_until_millis)? <= now { return Err(reject("LEASE_EXPIRED")); }
    Ok(())
}

fn deadline(now: u64, duration: &str) -> Result<String, CubeError> {
    let duration = number(duration)?;
    if duration == 0 || duration > 3_600_000 { return Err(reject("INVALID_LEASE")); }
    Ok(now.checked_add(duration).ok_or_else(|| reject("OVERFLOW"))?.to_string())
}

fn validate_manifest(manifest: &LedgerManifest) -> Result<u64, CubeError> {
    let id = number(&manifest.table_id)?;
    identifier(&manifest.schema)?;
    identifier(&manifest.table)?;
    identifier(&manifest.content_version)?;
    identifier(&manifest.structure_version)?;
    // Preserve Cube's discovery naming convention; do not invent a new suffix.
    let parts: Vec<_> = manifest.table.rsplitn(4, '_').collect();
    if parts.len() != 4 || parts[0].is_empty() || parts[1] != manifest.structure_version
        || parts[2] != manifest.content_version || parts[3].is_empty() {
        return Err(reject("MANIFEST_NAME_MISMATCH"));
    }
    if manifest.locations.is_empty() || manifest.locations.len() > 256 {
        return Err(reject("MANIFEST_LOCATIONS_REQUIRED"));
    }
    let mut seen = HashSet::new();
    for location in &manifest.locations {
        if location.is_empty() || location.len() > 8192 || Table::is_stream_location(location)
            || !seen.insert(location) { return Err(reject("INVALID_LOCATION")); }
    }
    Ok(id)
}

fn validate_table(db: &DbTableRef, manifest: &LedgerManifest, ready: bool) -> Result<u64, CubeError> {
    let id = validate_manifest(manifest)?;
    let table = TableRocksTable::new(db.clone()).get_row(id)?.ok_or_else(|| reject("TABLE_NOT_FOUND"))?;
    let schema = SchemaRocksTable::new(db.clone()).get_row_or_not_found(table.get_row().get_schema_id())?;
    let locations = table.get_row().locations().unwrap_or_default().into_iter().cloned().collect::<Vec<_>>();
    if table.get_row().get_table_name() != &manifest.table || schema.get_row().get_name() != &manifest.schema
        || locations != manifest.locations { return Err(reject("TABLE_MANIFEST_MISMATCH")); }
    if ready {
        if table.get_row().import_error().is_some() { return Err(reject("IMPORT_FAILED")); }
        if !table.get_row().is_ready() {
            return Err(reject("TABLE_NOT_READY"));
        }
        // The durable import result, not a client-provided ready bit, proves
        // each immutable location completed for THIS physical table id.
        validate_import_receipts(db, id, manifest)?;
    }
    Ok(id)
}

fn validate_import_receipts(db: &DbTableRef, id: u64, manifest: &LedgerManifest) -> Result<(), CubeError> {
        let jobs = JobRocksTable::new(db.clone());
        let whole = jobs.get_rows_by_index(
            &JobIndexKey::RowReference(RowKey::Table(TableId::Tables, id), JobType::TableImport),
            &JobRocksIndex::RowReference,
        )?;
        for location in &manifest.locations {
            let part = jobs.get_rows_by_index(
                &JobIndexKey::RowReference(RowKey::Table(TableId::Tables, id), JobType::TableImportCSV(location.clone())),
                &JobRocksIndex::RowReference,
            )?;
            if !part.iter().chain(whole.iter()).any(|j| j.get_row().status() == &JobStatus::Completed
                && j.get_row().completed_imports().contains(location)) {
                return Err(reject("IMPORT_RECEIPT_MISSING"));
            }
        }
    Ok(())
}

struct Change {
    record: LedgerRecord,
    advance: bool,
    binding: Option<Binding>,
    reference: Option<Reference>,
    drop_table: Option<u64>,
    ready_table: Option<u64>,
}

impl Change {
    fn new(record: LedgerRecord) -> Self {
        Self { record, advance: false, binding: None, reference: None, drop_table: None, ready_table: None }
    }
}

// Pure preparation: ALL business checks happen before touching the WriteBatch.
// This lets a rejected request be durably memoized without partial mutations.
fn prepare(db: &DbTableRef, command: &LedgerCommand) -> Result<Change, CubeError> {
    use LedgerCommand::*;
    let now = u64::try_from(Utc::now().timestamp_millis()).map_err(|_| reject("CLOCK_INVALID"))?;
    if let Claim { key, owner, lease_millis, expected_generation } = command {
        identifier(key)?; identifier(owner)?;
        let previous = head(db, key)?;
        if previous.as_ref().map(|h| &h.generation) != expected_generation.as_ref() {
            return Err(reject("GENERATION_CONFLICT"));
        }
        if let Some(previous) = previous {
            let old = required(db, key, &previous.generation)?;
            if matches!(old.state, LedgerState::Claimed | LedgerState::Bound)
                && number(&old.lease_until_millis)? > now { return Err(reject("CLAIM_BUSY")); }
        }
        let counter = db.snapshot.get(RowKey::Sequence(TableId::PreAggregationBuilds).to_bytes())?;
        let previous = match counter {
            Some(bytes) => u64::from_be_bytes(bytes.as_slice().try_into()
                .map_err(|_| CubeError::internal("Invalid ledger sequence".into()))?),
            None => 0,
        };
        let generation = previous.checked_add(1).ok_or_else(|| reject("OVERFLOW"))?;
        let mut change = Change::new(LedgerRecord {
            key: key.clone(), generation: generation.to_string(), owner: owner.clone(),
            lease_until_millis: deadline(now, lease_millis)?, state: LedgerState::Claimed,
            manifest: None, references: "0".into(),
        });
        change.advance = true;
        return Ok(change);
    }
    let (key, generation) = match command {
        Renew { key, generation, .. } | Bind { key, generation, .. }
        | Publish { key, generation, .. } | Retire { key, generation, .. }
        | Drop { key, generation, .. } | Acquire { key, generation, .. }
        | Release { key, generation, .. } => (key, generation),
        Claim { .. } | Create { .. } => unreachable!(),
    };
    let mut change = Change::new(required(db, key, generation)?);
    let record = &mut change.record;
    match command {
        Renew { owner, lease_millis, .. } => {
            owned(record, owner)?; live(db, record, now)?;
            if !matches!(record.state, LedgerState::Claimed | LedgerState::Bound) { return Err(reject("INVALID_STATE")); }
            record.lease_until_millis = deadline(now, lease_millis)?;
        }
        Bind { owner, manifest, .. } => {
            owned(record, owner)?; live(db, record, now)?;
            if !matches!(record.state, LedgerState::Claimed | LedgerState::Bound) { return Err(reject("INVALID_STATE")); }
            if record.manifest.as_ref().map(|m| m != manifest).unwrap_or(false) { return Err(reject("MANIFEST_IMMUTABLE")); }
            let table_id = validate_table(db, manifest, false)?;
            query_refs::ensure_no_readers(db, table_id)?;
            let binding: Option<Binding> = read(db, TableId::PreAggregationBindings, table_id)?;
            if binding.as_ref().map(|b| b.table_id != table_id || b.key != *key || b.generation != *generation).unwrap_or(false) {
                return Err(reject("TABLE_ALREADY_BOUND"));
            }
            let created = binding.as_ref().map(|b| b.created).unwrap_or(false);
            change.binding = Some(Binding { table_id, key: key.clone(), generation: generation.clone(), created });
            record.manifest = Some(manifest.clone()); record.state = LedgerState::Bound;
        }
        Publish { owner, .. } => {
            owned(record, owner)?; live(db, record, now)?;
            if record.state != LedgerState::Bound { return Err(reject("INVALID_STATE")); }
            let manifest = record.manifest.as_ref().ok_or_else(|| reject("MANIFEST_REQUIRED"))?;
            let table_id = number(&manifest.table_id)?;
            let binding: Binding = read(db, TableId::PreAggregationBindings, table_id)?.ok_or_else(|| reject("BINDING_REQUIRED"))?;
            check_binding(db, table_id, record)?;
            if binding.created {
                validate_table(db, manifest, false)?;
                let table = TableRocksTable::new(db.clone()).get_row_or_not_found(table_id)?;
                if table.get_row().import_error().is_some() { return Err(reject("IMPORT_FAILED")); }
                validate_import_receipts(db, table_id, manifest)?;
                if !table.get_row().is_ready() { change.ready_table = Some(table_id); }
            } else {
                validate_table(db, manifest, true)?;
            }
            record.state = LedgerState::Published;
        }
        Retire { owner, .. } => {
            owned(record, owner)?;
            if record.state == LedgerState::Dropped { return Err(reject("INVALID_STATE")); }
            if number(&record.references)? != 0 { return Err(reject("REFERENCED")); }
            record.state = LedgerState::Retired;
        }
        Drop { owner, .. } => {
            owned(record, owner)?;
            if record.state != LedgerState::Retired || number(&record.references)? != 0 { return Err(reject("NOT_RETIRED")); }
            let manifest = record.manifest.as_ref().ok_or_else(|| reject("MANIFEST_REQUIRED"))?;
            let table_id = validate_table(db, manifest, false)?;
            check_binding(db, table_id, record)?;
            change.drop_table = Some(table_id); record.state = LedgerState::Dropped;
        }
        Acquire { reference_id, .. } | Release { reference_id, .. } => {
            identifier(reference_id)?;
            let id = slot(&[key, generation, reference_id]);
            let previous: Option<Reference> = read(db, TableId::PreAggregationReferences, id)?;
            if previous.as_ref().map(|r| r.key != *key || r.generation != *generation || r.reference_id != *reference_id).unwrap_or(false) {
                return Err(reject("INDEX_COLLISION"));
            }
            let acquire = matches!(command, Acquire { .. });
            let count = number(&record.references)?;
            if acquire {
                if record.state != LedgerState::Published { return Err(reject("NOT_PUBLISHED")); }
                let manifest = record.manifest.as_ref().ok_or_else(|| reject("MANIFEST_REQUIRED"))?;
                let table_id = validate_table(db, manifest, true)?;
                check_binding(db, table_id, record)?;
                if previous.as_ref().map(|r| r.released).unwrap_or(false) { return Err(reject("REFERENCE_RELEASED")); }
                if previous.is_none() { record.references = count.checked_add(1).ok_or_else(|| reject("OVERFLOW"))?.to_string(); }
            } else if previous.as_ref().map(|r| !r.released).unwrap_or(false) {
                record.references = count.checked_sub(1).ok_or_else(|| reject("REFERENCE_UNDERFLOW"))?.to_string();
            }
            // A release that beats acquire creates a tombstone, preventing a
            // delayed request from reviving a completed/cancelled query.
            change.reference = Some(Reference { key: key.clone(), generation: generation.clone(),
                reference_id: reference_id.clone(), released: !acquire });
        }
        Claim { .. } | Create { .. } => unreachable!(),
    }
    Ok(change)
}

fn check_binding(db: &DbTableRef, table_id: u64, record: &LedgerRecord) -> Result<(), CubeError> {
    let binding: Binding = read(db, TableId::PreAggregationBindings, table_id)?.ok_or_else(|| reject("BINDING_REQUIRED"))?;
    if binding.table_id != table_id || binding.key != record.key || binding.generation != record.generation {
        return Err(reject("TABLE_ALREADY_BOUND"));
    }
    Ok(())
}

pub(super) fn guard_table_delete(db: &DbTableRef, table_id: u64) -> Result<(), CubeError> {
    query_refs::ensure_no_readers(db, table_id)?;
    let binding: Option<Binding> = read(db, TableId::PreAggregationBindings, table_id)?;
    if let Some(binding) = binding {
        let record = required(db, &binding.key, &binding.generation)?;
        if DROPPING.try_with(|g| g == &record.generation).unwrap_or(false) == false
            || binding.table_id != table_id || record.state != LedgerState::Retired
            || number(&record.references)? != 0
            || record.manifest.as_ref().map(|m| m.table_id.as_str()) != Some(table_id.to_string().as_str()) {
            return Err(reject("TABLE_DELETE_REQUIRES_LEDGER_DROP"));
        }
    }
    Ok(())
}

pub(super) fn mutate(db: DbTableRef, pipe: &mut BatchPipe<'_, RocksMetaStore>, request: LedgerRequest) -> Result<LedgerResult, CubeError> {
    identifier(&request.request_id)?;
    if serde_json::to_vec(&request).map_err(|e| CubeError::internal(e.to_string()))?.len() > 65_536 {
        return Err(reject("REQUEST_TOO_LARGE"));
    }
    let id = slot(&[&request.request_id]);
    if let Some(receipt) = read::<Receipt>(&db, TableId::PreAggregationRequests, id)? {
        if receipt.request != request { return Err(reject("REQUEST_ID_CONFLICT")); }
        return Ok(receipt.result);
    }
    let prepared = match &request.command {
        LedgerCommand::Create { key, generation, owner, create } => {
            prepare_create(&db, key, generation, owner, create)
        }
        command => prepare(&db, command),
    };
    let result = match prepared {
        Ok(change) => {
            let mut change = change;
            if let LedgerCommand::Create { create, .. } = &request.command {
                let columns = create.columns.iter().enumerate().map(|(i, c)| {
                    Ok(Column::new(c.name.clone(), ColumnType::from_string(&c.column_type)?, i))
                }).collect::<Result<Vec<_>, CubeError>>()?;
                let indexes = create.indexes.iter().map(|i| IndexDef {
                    name: i.name.clone(), columns: i.columns.clone(), multi_index: None, index_type: IndexType::Regular,
                }).collect();
                let has_header = create.import_format == "csv";
                let format = if create.delimiter.is_some() || create.disable_quoting.unwrap_or(false) {
                    ImportFormat::CSVOptions { delimiter: create.delimiter.as_ref().and_then(|d| d.chars().next()),
                        has_header, escape: None, quote: if create.disable_quoting.unwrap_or(false) { None } else { Some('"') } }
                } else if has_header { ImportFormat::CSV } else { ImportFormat::CSVNoHeader };
                let build_range_end = create.build_range_end.as_ref().map(|v| {
                    DateTime::parse_from_rfc3339(v).map(|v| v.with_timezone(&Utc))
                        .map_err(|_| reject("INVALID_BUILD_RANGE"))
                }).transpose()?;
                let table = RocksMetaStore::create_table_in_batch(db.clone(), pipe,
                    create.schema.clone(), create.table.clone(), columns, Some(create.locations.clone()),
                    Some(format), indexes, false, build_range_end, None, None, None, None, None, None,
                    None, None, false, None)?;
                let table_id = table.get_id();
                change.record.manifest = Some(LedgerManifest {
                    table_id: table_id.to_string(), schema: create.schema.clone(),
                    table: table.get_row().get_table_name().clone(),
                    locations: table.get_row().locations().unwrap_or_default().into_iter().cloned().collect(),
                    content_version: create.content_version.clone(), structure_version: create.structure_version.clone(),
                });
                change.binding = Some(Binding { table_id, key: change.record.key.clone(),
                    generation: change.record.generation.clone(), created: true });
            }
            if let Some(table_id) = change.ready_table {
                TableRocksTable::new(db.clone()).update_with_fn(table_id, |r| r.update_is_ready(true), pipe)?;
                pipe.set_post_commit_callback(|store| store.cached_tables.reset());
            }
            if let Some(table_id) = change.drop_table {
                DROPPING.sync_scope(change.record.generation.clone(), || {
                    RocksMetaStore::drop_table_impl(table_id, db.clone(), pipe)
                })?;
                pipe.set_post_commit_callback(|store| store.cached_tables.reset());
            }
            let generation = number(&change.record.generation)?;
            if change.advance {
                pipe.batch().put(RowKey::Sequence(TableId::PreAggregationBuilds).to_bytes(), generation.to_be_bytes());
                put(pipe, TableId::PreAggregationHeads, slot(&[&change.record.key]),
                    &Head { key: change.record.key.clone(), generation: change.record.generation.clone() })?;
            }
            if let Some(binding) = change.binding { put(pipe, TableId::PreAggregationBindings, binding.table_id, &binding)?; }
            if let Some(reference) = change.reference {
                put(pipe, TableId::PreAggregationReferences, slot(&[&reference.key, &reference.generation, &reference.reference_id]), &reference)?;
            }
            put(pipe, TableId::PreAggregationBuilds, generation, &change.record)?;
            LedgerResult { record: Some(change.record), rejection: None }
        }
        Err(error) if error.message.starts_with("LEDGER_") => LedgerResult { record: None, rejection: Some(error.message) },
        Err(error) => return Err(error),
    };
    put(pipe, TableId::PreAggregationRequests, id, &Receipt { request, result: result.clone() })?;
    Ok(result)
}

fn prepare_create(db: &DbTableRef, key: &str, generation: &str, owner: &str, create: &LedgerCreate) -> Result<Change, CubeError> {
    let mut record = required(db, key, generation)?;
    owned(&record, owner)?;
    live(db, &record, u64::try_from(Utc::now().timestamp_millis()).map_err(|_| reject("CLOCK_INVALID"))?)?;
    if record.state != LedgerState::Claimed || record.manifest.is_some() { return Err(reject("INVALID_STATE")); }
    validate_manifest(&LedgerManifest { table_id: "0".into(), schema: create.schema.clone(), table: create.table.clone(),
        locations: create.locations.clone(), content_version: create.content_version.clone(), structure_version: create.structure_version.clone() })?;
    if create.columns.is_empty() || create.columns.len() > 1024 || create.indexes.len() > 64
        || create.aggregations.as_ref().map(|a| !a.is_empty()).unwrap_or(false)
        || !matches!(create.import_format.as_str(), "csv" | "csvNoHeader") {
        return Err(reject("UNSUPPORTED_CREATE"));
    }
    if let Some(delimiter) = &create.delimiter {
        if delimiter.len() != 1 || !delimiter.is_ascii() || matches!(delimiter.as_str(), "\n" | "\r" | "\0") {
            return Err(reject("INVALID_DELIMITER"));
        }
    }
    if let Some(range) = &create.build_range_end {
        DateTime::parse_from_rfc3339(range).map_err(|_| reject("INVALID_BUILD_RANGE"))?;
    }
    let mut columns = HashSet::new();
    for column in &create.columns {
        identifier(&column.name)?;
        if !columns.insert(column.name.as_str()) { return Err(reject("DUPLICATE_COLUMN")); }
        ColumnType::from_string(&column.column_type).map_err(|_| reject("UNSUPPORTED_COLUMN_TYPE"))?;
    }
    let mut indexes = HashSet::new();
    for index in &create.indexes {
        identifier(&index.name)?;
        if index.index_type != "regular" { return Err(reject("UNSUPPORTED_CREATE")); }
        if index.name == "default" || !indexes.insert(index.name.as_str()) || index.columns.is_empty() {
            return Err(reject("INVALID_INDEX"));
        }
        let mut seen = HashSet::new();
        for column in &index.columns {
            if !columns.contains(column.as_str()) || !seen.insert(column) { return Err(reject("INVALID_INDEX")); }
        }
    }
    if SchemaRocksTable::new(db.clone()).get_rows_by_index(&create.schema, &SchemaRocksIndex::Name)?.is_empty() {
        return Err(reject("SCHEMA_NOT_FOUND"));
    }
    if get_table_impl(db.clone(), create.schema.clone(), create.table.clone()).is_ok() { return Err(reject("TABLE_ALREADY_EXISTS")); }
    record.state = LedgerState::Bound;
    Ok(Change::new(record))
}
