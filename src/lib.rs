use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

use rayon::prelude::*;

use crate::job::{Job, JobCounter, JobHandle};
use crate::senderlike::SenderLike;

mod job;
mod senderlike;

struct SharedResourcePoolBuilder<SType, SR> {
    tx: SType,
    handler_thread: JoinHandle<SR>,
}

impl<SType, Arg, SR> SharedResourcePoolBuilder<SType, SR>
where
    Arg: Send + 'static,
    SType: SenderLike<Item = Job<Arg>> + Clone + Send + 'static,
    SR: Send + 'static,
{
    fn init<SC>(
        tx: SType,
        rx: Receiver<Job<Arg>>,
        mut shared_resource: SR,
        mut shared_consumer_fn: SC,
    ) -> Self
    where
        SC: FnMut(&mut SR, Arg) -> () + Send + Sync + 'static,
    {
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
        let (tx, rx) = channel::<PArg>();
        self.init_pool(tx, rx, producer_fn, consumer_fn)
    }

    fn create_pool_bounded<P, PArg, C>(
        &self,
        bound: usize,
        producer_fn: P,
        consumer_fn: C,
    ) -> Result<JobHandle, std::io::Error>
    where
        P: Fn(SyncSender<PArg>) -> () + Send + 'static,
        PArg: Send + 'static,
        C: Fn(PArg) -> Arg + Send + Sync + 'static,
    {
        let (tx, rx) = sync_channel::<PArg>(bound);
        self.init_pool(tx, rx, producer_fn, consumer_fn)
    }

    fn init_pool<P, PArg, PType, C>(
        &self,
        tx: PType,
        rx: Receiver<PArg>,
        producer_fn: P,
        consumer_fn: C,
    ) -> Result<JobHandle, std::io::Error>
    where
        PType: SenderLike<Item = PArg> + Send + 'static,
        P: Fn(PType) -> () + Send + 'static,
        PArg: Send + 'static,
        C: Fn(PArg) -> Arg + Send + Sync + 'static,
    {
        let rx = {
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
    fn new<SC>(shared_resource: SR, shared_consumer_fn: SC) -> Self
    where
        SC: FnMut(&mut SR, Arg) -> () + Send + Sync + 'static,
    {
        let (tx, rx) = channel();
        Self::init(tx, rx, shared_resource, shared_consumer_fn)
    }
}

impl<Arg, SR> SharedResourcePoolBuilder<SyncSender<Job<Arg>>, SR>
where
    Arg: Send + 'static,
    SR: Send + 'static,
{
    fn new_bounded<SC>(bound: usize, shared_resource: SR, shared_consumer_fn: SC) -> Self
    where
        SC: FnMut(&mut SR, Arg) -> () + Send + Sync + 'static,
    {
        let (tx, rx) = sync_channel(bound);
        Self::init(tx, rx, shared_resource, shared_consumer_fn)
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
    fn test_many_integers_with_bounded_shared_producer() {
        let consumer_fn = |i| i * 10;

        let pool_builder =
            SharedResourcePoolBuilder::new_bounded(10, Vec::new(), |vec, i| vec.push(i));
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
    fn test_many_integers_with_bounded_item_producer() {
        let consumer_fn = |i| i * 10;

        let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |vec, i| vec.push(i));
        pool_builder
            .create_pool_bounded(
                10,
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
    fn test_many_integers_with_bounded_shared_and_item_producer() {
        let consumer_fn = |i| i * 10;

        let pool_builder =
            SharedResourcePoolBuilder::new_bounded(10, Vec::new(), |vec, i| vec.push(i));
        pool_builder
            .create_pool_bounded(
                10,
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
