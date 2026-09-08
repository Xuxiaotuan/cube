use super::*;

#[derive(Serialize, Deserialize)]
struct Readers { table_id: u64, count: u64 }

fn readers(db: &DbTableRef, table_id: u64) -> Result<u64, CubeError> {
    let row: Option<Readers> = read(db, TableId::PreAggregationReaders, table_id)?;
    match row {
        Some(row) if row.table_id != table_id => Err(reject("READER_IDENTITY_MISMATCH")),
        Some(row) => Ok(row.count),
        None => Ok(0),
    }
}

pub(super) fn ensure_no_readers(db: &DbTableRef, table_id: u64) -> Result<(), CubeError> {
    if readers(db, table_id)? != 0 { return Err(reject("TABLE_REFERENCED")); }
    Ok(())
}

pub(in crate::metastore) fn acquire_query_refs(
    db: &DbTableRef, pipe: &mut BatchPipe<'_, RocksMetaStore>, query_id: String, table_ids: Vec<String>,
) -> Result<LedgerQueryRefs, CubeError> {
    identifier(&query_id)?;
    if table_ids.len() > 256 { return Err(reject("TOO_MANY_QUERY_TABLES")); }
    let mut ids = table_ids.iter().map(|s| number(s)).collect::<Result<Vec<_>, _>>()?;
    ids.sort_unstable(); ids.dedup();
    let table_ids = ids.iter().map(u64::to_string).collect::<Vec<_>>();
    let id = slot(&[&query_id]);
    if let Some(old) = read::<LedgerQueryRefs>(db, TableId::PreAggregationQueries, id)? {
        if old.query_id != query_id { return Err(reject("INDEX_COLLISION")); }
        if old.released { return Err(reject("QUERY_ALREADY_RELEASED")); }
        if old.table_ids != table_ids { return Err(reject("QUERY_ID_CONFLICT")); }
        return Ok(old);
    }
    let mut counts = Vec::new();
    let mut records = Vec::new();
    let mut bindings = Vec::new();
    // No writes before the last table has passed validation: all or nothing.
    for table_id in ids {
        let table = TableRocksTable::new(db.clone()).get_row(table_id)?.ok_or_else(|| reject("TABLE_NOT_FOUND"))?;
        if !table.get_row().is_ready() { return Err(reject("TABLE_NOT_READY")); }
        let count = readers(db, table_id)?.checked_add(1).ok_or_else(|| reject("OVERFLOW"))?;
        counts.push(Readers { table_id, count });
        if let Some(binding) = read::<Binding>(db, TableId::PreAggregationBindings, table_id)? {
            let mut record = required(db, &binding.key, &binding.generation)?;
            if record.state != LedgerState::Published { return Err(reject("NOT_PUBLISHED")); }
            check_binding(db, table_id, &record)?;
            let manifest = record.manifest.as_ref().ok_or_else(|| reject("MANIFEST_REQUIRED"))?;
            if number(&manifest.table_id)? != table_id { return Err(reject("TABLE_MANIFEST_MISMATCH")); }
            validate_table(db, manifest, true)?;
            record.references = number(&record.references)?.checked_add(1).ok_or_else(|| reject("OVERFLOW"))?.to_string();
            bindings.push(LedgerQueryBinding { table_id: table_id.to_string(), key: binding.key, generation: binding.generation });
            records.push(record);
        }
    }
    let result = LedgerQueryRefs { query_id, table_ids, bindings, released: false };
    for count in counts { put(pipe, TableId::PreAggregationReaders, count.table_id, &count)?; }
    for record in records { put(pipe, TableId::PreAggregationBuilds, number(&record.generation)?, &record)?; }
    put(pipe, TableId::PreAggregationQueries, id, &result)?;
    Ok(result)
}

pub(in crate::metastore) fn release_query_refs(
    db: &DbTableRef, pipe: &mut BatchPipe<'_, RocksMetaStore>, query_id: String,
) -> Result<LedgerQueryRefs, CubeError> {
    identifier(&query_id)?;
    let id = slot(&[&query_id]);
    let mut result = match read::<LedgerQueryRefs>(db, TableId::PreAggregationQueries, id)? {
        Some(row) => row,
        None => LedgerQueryRefs { query_id: query_id.clone(), table_ids: vec![], bindings: vec![], released: true },
    };
    if result.query_id != query_id { return Err(reject("INDEX_COLLISION")); }
    if !result.released {
        let mut counts = Vec::new();
        let mut records = Vec::new();
        for table_id in &result.table_ids {
            let table_id = number(table_id)?;
            counts.push(Readers { table_id, count: readers(db, table_id)?.checked_sub(1).ok_or_else(|| reject("REFERENCE_UNDERFLOW"))? });
        }
        for binding in &result.bindings {
            let mut record = required(db, &binding.key, &binding.generation)?;
            check_binding(db, number(&binding.table_id)?, &record)?;
            record.references = number(&record.references)?.checked_sub(1).ok_or_else(|| reject("REFERENCE_UNDERFLOW"))?.to_string();
            records.push(record);
        }
        for count in counts { put(pipe, TableId::PreAggregationReaders, count.table_id, &count)?; }
        for record in records { put(pipe, TableId::PreAggregationBuilds, number(&record.generation)?, &record)?; }
    }
    result.released = true;
    // Tombstone even when release arrived before acquire. No TTL, no revival.
    put(pipe, TableId::PreAggregationQueries, id, &result)?;
    Ok(result)
}
