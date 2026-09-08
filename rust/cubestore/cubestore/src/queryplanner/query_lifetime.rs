//! Every started physical read/remote stream is drained even if its consumer
//! stops at LIMIT. The executor may acknowledge completion only after quiescence.
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use futures::{StreamExt, stream};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, Notify};

#[derive(Default, Debug)]
struct State { active: usize, sealed: bool, uncertain: bool }

#[derive(Default, Debug)]
pub(crate) struct QueryDrain { state: Mutex<State>, changed: Notify }

impl QueryDrain {
    pub(crate) fn new() -> Arc<Self> { Arc::new(Self::default()) }

    fn start(self: &Arc<Self>) -> Result<Ticket> {
        let mut state = self.state.lock().unwrap();
        if state.sealed { return Err(DataFusionError::Execution("Query execution already sealed".into())); }
        state.active += 1;
        Ok(Ticket { drain: self.clone(), complete: false })
    }

    pub(crate) async fn finish(&self) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            {
                let mut state = self.state.lock().unwrap();
                if state.active == 0 {
                    state.sealed = true;
                    return if state.uncertain {
                        Err(DataFusionError::Execution("Query termination is unproven; persistent references retained".into()))
                    } else { Ok(()) };
                }
            }
            changed.await;
        }
    }
}

struct Ticket { drain: Arc<QueryDrain>, complete: bool }

impl Drop for Ticket {
    fn drop(&mut self) {
        let mut state = self.drain.state.lock().unwrap();
        state.active -= 1;
        state.uncertain |= !self.complete;
        drop(state);
        self.drain.changed.notify_one();
    }
}

pub(crate) fn track(
    context: Arc<TaskContext>, make: impl FnOnce() -> Result<SendableRecordBatchStream>,
) -> Result<SendableRecordBatchStream> {
    let Some(drain) = context.session_config().get_extension::<QueryDrain>() else { return make(); };
    // Registration precedes execute(), so sealed plans cannot initiate late IO.
    let mut ticket = drain.start()?;
    let mut input = make()?;
    let schema = input.schema();
    let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(1);
    tokio::spawn(async move {
        let mut forwarding = true;
        let mut clean = true;
        while let Some(item) = input.next().await {
            clean &= item.is_ok();
            if forwarding && tx.send(item).await.is_err() { forwarding = false; }
            // A dropped receiver only disables forwarding, NEVER the real read.
        }
        drop(input);
        ticket.complete = clean;
        drop(ticket);
    });
    let output = stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|v| (v, rx)) });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
}
