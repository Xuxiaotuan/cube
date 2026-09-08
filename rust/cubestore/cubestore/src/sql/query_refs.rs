//! References belong to the actual executing task, not the HTTP/SQL awaiter.
//! A cancelled awaiter detaches the supervisor. An error/crash/failed release
//! retains durable pins; no Drop implementation pretends remote work stopped.
use super::ha::MutationGate;
use crate::metastore::MetaStore;
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::CubeError;
use std::future::Future;
use std::sync::Arc;

pub(super) async fn execute<T, F>(
    db: Arc<dyn MetaStore>, gate: Arc<MutationGate>, plan: SerializedPlan,
    exec: impl FnOnce(SerializedPlan) -> F + Send + 'static,
) -> Result<T, CubeError>
where T: Send + 'static, F: Future<Output = Result<T, CubeError>> + Send + 'static {
    let guard = gate.begin()?;
    let table_ids = plan.index_snapshots().iter().map(|i| i.table().get_id().to_string()).collect::<Vec<_>>();
    let query_id = uuid::Uuid::new_v4().to_string();
    tokio::spawn(async move {
        guard.scope(async {
            guard.check()?;
            db.acquire_pre_aggregation_query_refs(query_id.clone(), table_ids).await?;
            #[cfg(test)]
            let finish = pause_for_test(plan.trace_obj()).await;
            // scope(), not run(): lease loss must not abandon accepted remote IO.
            let result = exec(plan).await?;
            guard.check()?;
            db.release_pre_aggregation_query_refs(query_id).await?;
            #[cfg(test)]
            if let Some(finish) = finish { let _ = finish.send(()); }
            Ok(result)
        }).await
    }).await.map_err(|e| CubeError::internal(format!("Query supervisor failed; references retained: {}", e)))?
}

#[cfg(test)]
type Hook = (tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>);
#[cfg(test)]
lazy_static::lazy_static! {
    static ref HOOKS: std::sync::Mutex<std::collections::HashMap<String, Hook>> = Default::default();
}
#[cfg(test)]
async fn pause_for_test(trace: Option<String>) -> Option<tokio::sync::oneshot::Sender<()>> {
    let hook = trace.and_then(|t| HOOKS.lock().unwrap().remove(&t));
    if let Some((arrived, release, finish)) = hook {
        let _ = arrived.send(());
        let _ = release.await;
        Some(finish)
    } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::sql::SqlQueryContext;
    use std::time::Duration;

    #[tokio::test]
    async fn query_refs_real_sql_cancelled_awaiter_does_not_release_executing_task() {
        Config::test("query_refs_real_sql_cancel").start_test(async move |services| {
            let sql = services.sql_service;
            let db = services.meta_store;
            sql.exec_query("CREATE SCHEMA ref_test").await?.collect().await?;
            sql.exec_query("CREATE TABLE ref_test.values (n int)").await?.collect().await?;
            sql.exec_query("INSERT INTO ref_test.values (n) VALUES (7)").await?.collect().await?;
            let table_id = db.get_table("ref_test".into(), "values".into()).await?.get_id();
            let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
            let trace = "query_refs_real_sql_cancel".to_string();
            HOOKS.lock().unwrap().insert(trace.clone(), (arrived_tx, release_rx, finish_tx));
            let task = tokio::spawn(async move {
                sql.exec_query_with_context(SqlQueryContext { trace_obj: Some(trace), ..Default::default() },
                    "SELECT sum(n) FROM ref_test.values").await?.collect().await
            });
            tokio::time::timeout(Duration::from_secs(10), arrived_rx).await.unwrap().unwrap();
            assert!(db.drop_table(table_id).await.is_err());
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(db.drop_table(table_id).await.is_err(), "cancelling the awaiter must not unpin the task");
            release_tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(30), finish_rx).await.unwrap().unwrap();
            db.drop_table(table_id).await?;
            Ok::<_, CubeError>(())
        }).await;
    }
}
