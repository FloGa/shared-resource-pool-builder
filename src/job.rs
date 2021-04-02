use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

type Counter = Arc<(Mutex<u32>, Condvar)>;

pub struct Job<Arg>(pub(crate) Arg, pub(crate) JobCounter);

pub(crate) struct JobCounter(pub(crate) Counter);

/// The handle that can be used to wait for a pool to finish
pub struct JobHandle {
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

    /// Waits for a specific pool to finish its work.
    ///
    /// This can be used if you need to create another pool, but only after a previous pool finishes
    /// its work.
    ///
    /// This method returns a [`thread::Result`]. Since the `Err` variant of this specialized
    /// `Result` does not implement the [`Error`](std::error::Error) trait, the `?`-operator does
    /// not work here. For more information about how to handle panics in this case, please refer
    /// to the documentation of [`thread::Result`].
    ///
    /// # Example:
    ///
    /// ```rust
    /// # use shared_resource_pool_builder::SharedResourcePoolBuilder;
    /// #
    /// # fn example() {
    /// let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |v, i| v.push(i));
    ///
    /// // It is crucial that all additions finish their work, hence the `join`.
    /// pool_builder.create_pool(
    ///     |tx| (0..10).for_each(|i| tx.send(i).unwrap()),
    ///     |i| i + 3,
    /// ).join().unwrap();
    ///
    /// // Now we are safe to run the multiplications.
    /// pool_builder.create_pool(
    ///     |tx| (0..10).for_each(|i| tx.send(i).unwrap()),
    ///     |i| i * 2,
    /// );
    ///
    /// let result = pool_builder.join().unwrap();
    /// # }
    /// ```
    pub fn join(self) -> thread::Result<()> {
        self.join_handle.join()?;

        let (lock, cvar) = &*self.job_counter;
        let mut job_counter = lock.lock().unwrap();
        while *job_counter > 0 {
            job_counter = cvar.wait(job_counter).unwrap();
        }

        Ok(())
    }
}
