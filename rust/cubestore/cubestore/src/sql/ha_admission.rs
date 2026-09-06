//! Process-local admission only; leadership and durable state live elsewhere.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const DRAINING: usize = 1 << (usize::BITS - 1);

#[derive(Default)]
pub struct Admission(AtomicUsize);

impl Admission {
    pub fn admit(self: &Arc<Self>) -> Result<Permit, ()> {
        self.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                if state & DRAINING != 0 || state == DRAINING - 1 {
                    None
                } else {
                    Some(state + 1)
                }
            })
            .map(|_| Permit(self.clone()))
            .map_err(|_| ())
    }

    pub fn drain(&self) {
        self.0.fetch_or(DRAINING, Ordering::AcqRel);
    }

    pub fn snapshot(&self) -> (bool, usize) {
        let state = self.0.load(Ordering::Acquire);
        (state & DRAINING != 0, state & !DRAINING)
    }
}

pub struct Permit(Arc<Admission>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0 .0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_preserves_accepted_work_and_rejects_new_work() {
        let gate = Arc::new(Admission::default());
        let first = gate.admit().unwrap();
        let second = gate.admit().unwrap();
        gate.drain();
        gate.drain();
        assert_eq!(gate.snapshot(), (true, 2));
        assert!(gate.admit().is_err());
        drop(first);
        assert_eq!(gate.snapshot(), (true, 1));
        drop(second);
        assert_eq!(gate.snapshot(), (true, 0));
        assert!(gate.admit().is_err());
    }

    #[test]
    fn concurrent_admission_and_drain_cannot_reopen_gate_or_lose_permits() {
        let gate = Arc::new(Admission::default());
        let barrier = Arc::new(std::sync::Barrier::new(17));
        let threads: Vec<_> = (0..16)
            .map(|_| {
                let gate = gate.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let permit = gate.admit();
                    std::thread::yield_now();
                    drop(permit);
                })
            })
            .collect();
        barrier.wait();
        gate.drain();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(gate.snapshot(), (true, 0));
        assert!(gate.admit().is_err());
    }

    #[test]
    fn unwinding_releases_in_flight_work() {
        let gate = Arc::new(Admission::default());
        let result = std::panic::catch_unwind(|| {
            let _permit = gate.admit().unwrap();
            panic!("simulated request panic");
        });
        assert!(result.is_err());
        assert_eq!(gate.snapshot(), (false, 0));
    }
}
