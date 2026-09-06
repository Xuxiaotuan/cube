use crate::metastore::job::{JobStatus, JobType};
use crate::metastore::table::Table;
use crate::metastore::{MetaStore, RowKey, TableId};
use crate::CubeError;
use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildStatus {
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locations: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub async fn lookup(db: &dyn MetaStore, name: &str) -> Result<BuildStatus, CubeError> {
    let (schema, table_name) = name
        .split_once('.')
        .filter(|(schema, table)| !schema.is_empty() && !table.is_empty())
        .ok_or_else(|| CubeError::user("table must be schema.table".into()))?;
    // include_non_ready=true bypasses the ready-table cache and includes imports.
    let tables = db.get_tables_with_path(true).await?;
    let path = match tables.iter().find(|t| {
        t.schema.get_row().get_name() == schema && t.table.get_row().get_table_name() == table_name
    }) {
        Some(path) => path,
        None => {
            return Ok(BuildStatus {
                state: "absent",
                table_id: None,
                locations: None,
                error: None,
            })
        }
    };
    let table = path.table.get_row();
    let id = path.table.get_id();
    let state = table_status(table, id);
    if state.state != "building" {
        return Ok(state);
    }
    for job in db.all_jobs().await? {
        if job.get_row().row_reference() == &RowKey::Table(TableId::Tables, id)
            && matches!(
                job.get_row().job_type(),
                JobType::TableImport | JobType::TableImportCSV(_)
            )
        {
            let error = match job.get_row().status() {
                JobStatus::Error(error) => Some(error.clone()),
                JobStatus::Timeout => Some("Table import timed out".into()),
                _ => None,
            };
            if error.is_some() {
                return Ok(BuildStatus {
                    state: "failed",
                    table_id: Some(id),
                    locations: table
                        .locations()
                        .map(|locations| locations.into_iter().cloned().collect()),
                    error,
                });
            }
        }
    }
    Ok(BuildStatus {
        state: "building",
        table_id: Some(id),
        locations: table
            .locations()
            .map(|locations| locations.into_iter().cloned().collect()),
        error: None,
    })
}

fn table_status(table: &Table, id: u64) -> BuildStatus {
    let is_file_import = table
        .locations()
        .map(|locations| {
            !locations.is_empty() && locations.iter().all(|l| !Table::is_stream_location(l))
        })
        .unwrap_or(false);
    if !is_file_import {
        return BuildStatus { state: "unknown", table_id: Some(id), locations: table.locations().map(|locations| locations.into_iter().cloned().collect()), error: Some(
            "Only external-file CREATE import completion is supported; INSERT/stream state is not resumable".into()
        ) };
    }
    if let Some(error) = table.import_error() {
        return BuildStatus {
            state: "failed",
            table_id: Some(id),
            locations: table
                .locations()
                .map(|locations| locations.into_iter().cloned().collect()),
            error: Some(error.clone()),
        };
    }
    BuildStatus {
        state: if table.is_ready() {
            "ready"
        } else {
            "building"
        },
        table_id: Some(id),
        locations: table
            .locations()
            .map(|locations| locations.into_iter().cloned().collect()),
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn table(locations: serde_json::Value, ready: bool, error: Option<&str>) -> Table {
        serde_json::from_value(serde_json::json!({
            "table_name": "t", "schema_id": 1, "columns": [],
            "locations": locations, "import_format": null,
            "is_ready": ready, "import_error": error
        }))
        .unwrap()
    }

    #[test]
    fn table_existence_is_not_import_completion() {
        let table = table(serde_json::json!(["temp://upload.csv"]), false, None);
        let state = table_status(&table, 17);
        assert_eq!(state.state, "building");
        assert_eq!(state.table_id, Some(17));
        assert_eq!(state.locations, Some(vec!["temp://upload.csv".to_string()]));
        assert_eq!(
            table_status(&table.update_is_ready(true), 17).state,
            "ready"
        );
    }

    #[test]
    fn durable_failure_wins_over_a_stale_ready_bit() {
        let table = table(
            serde_json::json!(["temp://upload.csv"]),
            true,
            Some("checksum mismatch"),
        );
        let state = table_status(&table, 17);
        assert_eq!(state.state, "failed");
        assert_eq!(state.error.as_deref(), Some("checksum mismatch"));
        assert_eq!(state.locations, Some(vec!["temp://upload.csv".to_string()]));
    }

    #[test]
    fn insert_targets_and_streams_never_report_recoverable_ready() {
        assert_eq!(
            table_status(&table(serde_json::Value::Null, true, None), 17).state,
            "unknown"
        );
        assert_eq!(
            table_status(&table(serde_json::json!([]), true, None), 17).state,
            "unknown"
        );
        assert_eq!(
            table_status(
                &table(serde_json::json!(["stream://kafka/topic"]), true, None),
                17
            )
            .state,
            "unknown"
        );
    }
}
