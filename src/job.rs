use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

type Counter = Arc<(Mutex<u32>, Condvar)>;

pub(crate) struct Job<Arg>(pub(crate) Arg, pub(crate) JobCounter);

pub(crate) struct JobCounter(pub(crate) Counter);

pub(crate) struct JobHandle {
    pub(crate) join_handle: JoinHandle<()>,
    pub(crate) job_counter: Counter,
}

impl JobCounter {
    pub(crate) fn new(job_counter: Counter) -> Self {
        {
            let (lock, _) = &*job_counter;
            let mut job_counter = lock.lock().unwrap();
            *job_counter += 1;
        }

        Self(job_counter)
    }
}

impl Drop for JobCounter {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.0;
        let mut job_counter = lock.lock().unwrap();
        *job_counter -= 1;
        cvar.notify_all();
    }
}

impl JobHandle {
    pub(crate) fn new(join_handle: JoinHandle<()>, job_counter: Counter) -> Self {
        Self {
            join_handle,
            job_counter,
        }
    }

    pub(crate) fn join(self) -> thread::Result<()> {
        self.join_handle.join()?;

        let (lock, cvar) = &*self.job_counter;
        let mut job_counter = lock.lock().unwrap();
        while *job_counter > 0 {
            job_counter = cvar.wait(job_counter).unwrap();
        }

        Ok(())
    }
}
