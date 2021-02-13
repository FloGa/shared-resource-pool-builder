use std::sync::mpsc::{channel, sync_channel, SendError, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

use rayon::prelude::*;

type Counter = Arc<(Mutex<u32>, Condvar)>;

trait SenderLike {
    type Item;
    fn send(&self, t: Self::Item) -> Result<(), SendError<Self::Item>>;
}

impl<T> SenderLike for Sender<T> {
    type Item = T;
    fn send(&self, t: Self::Item) -> Result<(), SendError<Self::Item>> {
        Sender::send(self, t)
    }
}

impl<T> SenderLike for SyncSender<T> {
    type Item = T;
    fn send(&self, t: Self::Item) -> Result<(), SendError<Self::Item>> {
        SyncSender::send(self, t)
    }
}

struct Job<Arg>(Arg, JobCounter);

struct JobCounter(Counter);

struct JobHandle {
    join_handle: JoinHandle<()>,
    job_counter: Counter,
}

impl JobCounter {
    fn new(job_counter: Counter) -> Self {
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

struct SharedResourcePoolBuilder<SType, SR> {
    tx: SType,
    handler_thread: JoinHandle<SR>,
}

impl<SType, Arg, SR> SharedResourcePoolBuilder<SType, SR>
where
    Arg: Send + 'static,
    SType: SenderLike<Item = Job<Arg>> + Clone + Send + 'static,
{
    fn create_pool<P, PArg, C>(
        &self,
        producer_fn: P,
        consumer_fn: C,
    ) -> Result<JobHandle, std::io::Error>
    where
        P: Fn(Sender<PArg>) -> () + Send + 'static,
        PArg: Send + 'static,
        C: Fn(PArg) -> Arg + Send + Sync + 'static,
    {
        let rx = {
            let (tx, rx) = channel::<PArg>();
            thread::spawn(move || producer_fn(tx));
            rx
        };

        let job_counter = Arc::new((Mutex::new(0), Condvar::new()));
        let join_handle = {
            let job_counter = Arc::clone(&job_counter);
            let tx = self.tx.clone();
            thread::spawn(move || {
                rx.into_iter()
                    .par_bridge()
                    .for_each_with(tx.clone(), |tx, item| {
                        let job_counter = Arc::clone(&job_counter);
                        let job = Job(consumer_fn(item), JobCounter::new(job_counter));
                        tx.send(job).unwrap();
                    });
            })
        };

        Ok(JobHandle::new(join_handle, job_counter))
    }

    fn join(self) -> std::thread::Result<SR> {
        drop(self.tx);
        self.handler_thread.join()
    }
}

impl<Arg, SR> SharedResourcePoolBuilder<Sender<Job<Arg>>, SR>
where
    Arg: Send + 'static,
    SR: Send + 'static,
{
    fn new<SC>(mut shared_resource: SR, mut shared_consumer_fn: SC) -> Self
    where
        SC: FnMut(&mut SR, Arg) -> () + Send + Sync + 'static,
    {
        let (tx, rx) = channel::<Job<Arg>>();

        let join_handle = thread::spawn(move || {
            for job in rx {
                shared_consumer_fn(&mut shared_resource, job.0);
            }
            shared_resource
        });

        Self {
            tx,
            handler_thread: join_handle,
        }
    }
}

impl<Arg, SR> SharedResourcePoolBuilder<SyncSender<Job<Arg>>, SR>
where
    Arg: Send + 'static,
    SR: Send + 'static,
{
    fn new_bounded<SC>(bound: usize, mut shared_resource: SR, mut shared_consumer_fn: SC) -> Self
    where
        SC: FnMut(&mut SR, Arg) -> () + Send + Sync + 'static,
    {
        let (tx, rx) = sync_channel::<Job<Arg>>(bound);

        let join_handle = thread::spawn(move || {
            for job in rx {
                shared_consumer_fn(&mut shared_resource, job.0);
            }
            shared_resource
        });

        Self {
            tx,
            handler_thread: join_handle,
        }
    }
}

impl JobHandle {
    fn new(join_handle: JoinHandle<()>, job_counter: Counter) -> Self {
        Self {
            join_handle,
            job_counter,
        }
    }

    fn join(self) -> thread::Result<()> {
        self.join_handle.join()?;

        let (lock, cvar) = &*self.job_counter;
        let mut job_counter = lock.lock().unwrap();
        while *job_counter > 0 {
            job_counter = cvar.wait(job_counter).unwrap();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestResource(Vec<TestElem>);

    impl TestResource {
        fn add(&mut self, elem: TestElem) {
            self.0.push(elem);
        }
    }

    struct TestElem(String);

    #[test]
    fn test_few_integers() {
        let consumer_fn = |i| i * 10;

        let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |vec, i| vec.push(i));
        pool_builder
            .create_pool(|tx| (0..5).for_each(|i| tx.send(i).unwrap()), consumer_fn)
            .unwrap();

        let result = {
            let mut result = pool_builder.join().unwrap();
            result.sort();
            result
        };

        assert_eq!(result, (0..5).map(consumer_fn).collect::<Vec<_>>());
    }

    #[test]
    fn test_many_integers() {
        let consumer_fn = |i| i * 10;

        let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |vec, i| vec.push(i));
        pool_builder
            .create_pool(
                |tx| (0..1000).for_each(|i| tx.send(i).unwrap()),
                consumer_fn,
            )
            .unwrap();

        let result = {
            let mut result = pool_builder.join().unwrap();
            result.sort();
            result
        };

        assert_eq!(result, (0..1000).map(consumer_fn).collect::<Vec<_>>());
    }

    #[test]
    fn test_integers_multiple_pools() {
        let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |vec, i| vec.push(i));
        let pools = vec![
            pool_builder
                .create_pool(|tx| (0..2).for_each(|i| tx.send(i).unwrap()), |i| i * 10)
                .unwrap(),
            pool_builder
                .create_pool(|tx| (2..5).for_each(|i| tx.send(i).unwrap()), |i| i * 10)
                .unwrap(),
        ];

        for pool in pools {
            pool.join().unwrap();
        }

        let result = {
            let mut result = pool_builder.join().unwrap();
            result.sort();
            result
        };

        assert_eq!(result, vec![0, 10, 20, 30, 40])
    }

    #[test]
    fn test_custom_types() {
        let producer_fn = || ('a'..='z').map(|c| String::from(c));

        let pool_builder =
            SharedResourcePoolBuilder::new(TestResource(Vec::new()), |tres, e| tres.add(e));
        pool_builder
            .create_pool(
                move |tx| producer_fn().for_each(|i| tx.send(i).unwrap()),
                |c| TestElem(c),
            )
            .unwrap();

        let result = {
            let mut result = pool_builder
                .join()
                .unwrap()
                .0
                .iter()
                .map(|e| e.0.clone())
                .collect::<Vec<_>>();
            result.sort();
            result
        };

        assert_eq!(result, producer_fn().collect::<Vec<_>>());
    }
}
