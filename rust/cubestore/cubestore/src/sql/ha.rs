//! Shared HTTP/MySQL admission and lease-snapshot revalidation.
//!
//! This is NOT an atomic storage fence. Detached jobs must propagate the guard
//! and durable publication must also be fenced by the MetaStore/job protocol.
use super::ha_admission::{Admission, Permit};
use crate::http::HttpServer;
use crate::sql::parser::{CubeStoreParser, Statement as CubeStatement};
use crate::CubeError;
use serde::Serialize;
use sqlparser::ast::Statement;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

type Check = dyn Fn() -> Result<String, CubeError> + Send + Sync;

pub struct MutationGate {
    admission: Arc<Admission>,
    check: Arc<Check>,
    pub strict: bool,
}

struct AcceptedMutation {
    gate: Arc<MutationGate>,
    identity: String,
    _permit: Permit,
}

#[derive(Clone)]
pub struct MutationGuard(Arc<AcceptedMutation>);

tokio::task_local! {
    static CURRENT_MUTATION: MutationGuard;
}

/// Capture before spawning; Tokio task locals do not propagate to new tasks.
pub fn current_mutation() -> Option<MutationGuard> {
    CURRENT_MUTATION.try_with(Clone::clone).ok()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainStatus {
    pub draining: bool,
    pub in_flight: usize,
    pub drained: bool,
}

impl MutationGate {
    pub fn new(leadership_file: String, promotion_file: String) -> Arc<Self> {
        let strict = std::env::var("CUBESTORE_ROUTER_ROLE_STRICT")
            .map(|v| v != "false" && v != "0")
            .unwrap_or_else(|_| std::env::var_os("CUBESTORE_ROUTER_LEADERSHIP_FILE").is_some());
        Self::with_checker(
            strict,
            Arc::new(move || {
                if strict {
                    HttpServer::write_fence_identity(&leadership_file, &promotion_file)
                        .map_err(CubeError::wrong_connection)
                } else {
                    Ok("standalone".to_string())
                }
            }),
        )
    }

    pub(crate) fn with_checker(strict: bool, check: Arc<Check>) -> Arc<Self> {
        Arc::new(Self {
            admission: Arc::new(Admission::default()),
            check,
            strict,
        })
    }

    pub fn begin(self: &Arc<Self>) -> Result<MutationGuard, CubeError> {
        if let Some(guard) = current_mutation() {
            if Arc::ptr_eq(&guard.0.gate, self) {
                guard.check()?;
                return Ok(guard);
            }
        }
        let permit = self
            .admission
            .admit()
            .map_err(|_| CubeError::wrong_connection("Router is draining".to_string()))?;
        let identity = (self.check)()?;
        Ok(MutationGuard(Arc::new(AcceptedMutation {
            gate: self.clone(),
            identity,
            _permit: permit,
        })))
    }

    pub fn status(&self) -> DrainStatus {
        let (draining, in_flight) = self.admission.snapshot();
        DrainStatus {
            draining,
            in_flight,
            drained: draining && in_flight == 0,
        }
    }

    pub async fn drain(&self, wait: Duration) -> DrainStatus {
        self.admission.drain();
        let _ = tokio::time::timeout(wait, async {
            while self.status().in_flight != 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        self.status()
    }
}

impl MutationGuard {
    /// Propagate admission to a child without changing its output type. Publication
    /// must still revalidate this identity; merely holding a permit is not a fence.
    pub(crate) async fn scope<T>(&self, future: impl Future<Output = T>) -> T {
        CURRENT_MUTATION.scope(self.clone(), future).await
    }

    pub fn check(&self) -> Result<(), CubeError> {
        if (self.0.gate.check)()? != self.0.identity {
            return Err(CubeError::wrong_connection(
                "Lease changed during accepted operation; reconcile durable state before retry"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub async fn run<T>(
        &self,
        future: impl Future<Output = Result<T, CubeError>>,
    ) -> Result<T, CubeError> {
        CURRENT_MUTATION
            .scope(self.clone(), async {
                self.check()?;
                tokio::pin!(future);
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                loop {
                    tokio::select! {
                        biased;
                        _ = interval.tick() => self.check()?,
                        result = &mut future => {
                            self.check()?;
                            return result;
                        }
                    }
                }
            })
            .await
    }
}

/// Parse rather than trusting a first keyword. Unknown syntax is a mutation.
pub fn is_read_query(query: &str) -> bool {
    let ast = CubeStoreParser::new(query, None).and_then(|mut p| p.parse_statement());
    match ast {
        Ok(CubeStatement::Statement(Statement::Query(_)))
        | Ok(CubeStatement::Statement(Statement::ShowVariable { .. }))
        | Ok(CubeStatement::Statement(Statement::ShowSchemas { .. }))
        | Ok(CubeStatement::Statement(Statement::SetVariable { .. }))
        | Ok(CubeStatement::ExplainAnalyzeDetailed(_)) => true,
        Ok(CubeStatement::Statement(Statement::Explain { statement, .. })) => {
            matches!(*statement, Statement::Query(_))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn fence_rejects_changed_epoch_even_if_new_epoch_is_valid() {
        let epoch = Arc::new(AtomicUsize::new(1));
        let value = epoch.clone();
        let gate = MutationGate::with_checker(
            true,
            Arc::new(move || Ok(value.load(Ordering::SeqCst).to_string())),
        );
        let guard = gate.begin().unwrap();
        let result = guard
            .run(async {
                epoch.store(2, Ordering::SeqCst);
                Ok(())
            })
            .await;
        assert!(result.is_err());
        assert!(gate.begin().is_ok());
    }

    #[tokio::test]
    async fn drain_waits_for_cloned_guard_and_does_not_cancel_accepted_work() {
        let gate = MutationGate::with_checker(false, Arc::new(|| Ok("one".into())));
        let guard = gate.begin().unwrap();
        let child = guard.clone();
        drop(guard);
        let status = gate.drain(Duration::from_millis(1)).await;
        assert!(!status.drained);
        assert_eq!(status.in_flight, 1);
        assert!(gate.begin().is_err());
        assert!(child.run(async { Ok(()) }).await.is_ok());
        drop(child);
        assert!(gate.drain(Duration::from_millis(1)).await.drained);
    }

    #[tokio::test]
    async fn lost_fence_cancels_pending_operation_without_reporting_success() {
        let valid = Arc::new(AtomicUsize::new(1));
        let value = valid.clone();
        let gate = MutationGate::with_checker(
            true,
            Arc::new(move || {
                if value.load(Ordering::SeqCst) == 1 {
                    Ok("one".into())
                } else {
                    Err(CubeError::wrong_connection("expired".into()))
                }
            }),
        );
        let guard = gate.begin().unwrap();
        valid.store(0, Ordering::SeqCst);
        assert!(guard
            .run(std::future::pending::<Result<(), CubeError>>())
            .await
            .is_err());
        drop(guard);
        assert_eq!(gate.status().in_flight, 0);
    }

    #[test]
    fn sql_classification_handles_comments_and_fails_closed() {
        assert!(is_read_query("SHOW SCHEMAS"));
        assert!(is_read_query("/* comment */ SELECT 1"));
        assert!(!is_read_query("/* comment */ DROP TABLE foo.bar"));
        assert!(!is_read_query("INSERT INTO foo.bar VALUES (1)"));
        assert!(!is_read_query("SYSTEM KILL ALL JOBS"));
        assert!(!is_read_query("nonsense"));
    }
}
