use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

use threadpool::ThreadPool;

use crate::job::{Job, JobCounter, JobHandle};
use crate::senderlike::SenderLike;

mod job;
mod senderlike;

pub struct SharedResourcePoolBuilder<SType, SR> {
    tx: SType,
    handler_thread: JoinHandle<SR>,
    consumer_pool: ThreadPool,
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

        let consumer_pool = threadpool::Builder::new().build();

        Self {
            tx,
            handler_thread: join_handle,
            consumer_pool,
        }
    }

    pub fn create_pool<P, PArg, C>(&self, producer_fn: P, consumer_fn: C) -> JobHandle
    where
        P: Fn(Sender<PArg>) -> () + Send + 'static,
        PArg: Send + 'static,
        C: Fn(PArg) -> Arg + Clone + Send + Sync + 'static,
    {
        let (tx, rx) = channel::<PArg>();
        self.init_pool(tx, rx, producer_fn, consumer_fn)
    }

    pub fn create_pool_bounded<P, PArg, C>(
        &self,
        bound: usize,
        producer_fn: P,
        consumer_fn: C,
    ) -> JobHandle
    where
        P: Fn(SyncSender<PArg>) -> () + Send + 'static,
        PArg: Send + 'static,
        C: Fn(PArg) -> Arg + Clone + Send + Sync + 'static,
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
    ) -> JobHandle
    where
        PType: SenderLike<Item = PArg> + Send + 'static,
        P: Fn(PType) -> () + Send + 'static,
        PArg: Send + 'static,
        C: Fn(PArg) -> Arg + Clone + Send + Sync + 'static,
    {
        let producer_thread = thread::spawn(move || producer_fn(tx));

        let job_counter = Arc::new((Mutex::new(0), Condvar::new()));

        let consumer_thread = {
            let pool = self.consumer_pool.clone();
            let job_counter = Arc::clone(&job_counter);
            let tx = self.tx.clone();
            thread::spawn(move || {
                for item in rx {
                    let tx = tx.clone();
                    let consumer_fn = consumer_fn.clone();
                    let job_counter = JobCounter::new(Arc::clone(&job_counter));
                    pool.execute(move || {
                        let job = Job(consumer_fn(item), job_counter);
                        tx.send(job).unwrap();
                    });
                }
            })
        };

        let join_handle = thread::spawn(move || {
            producer_thread.join().unwrap();
            consumer_thread.join().unwrap();
        });

        JobHandle::new(join_handle, job_counter)
    }

    pub fn join(self) -> thread::Result<SR> {
        drop(self.tx);
        self.handler_thread.join()
    }
}

impl<Arg, SR> SharedResourcePoolBuilder<Sender<Job<Arg>>, SR>
where
    Arg: Send + 'static,
    SR: Send + 'static,
{
    pub fn new<SC>(shared_resource: SR, shared_consumer_fn: SC) -> Self
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
    pub fn new_bounded<SC>(bound: usize, shared_resource: SR, shared_consumer_fn: SC) -> Self
    where
        SC: FnMut(&mut SR, Arg) -> () + Send + Sync + 'static,
    {
        let (tx, rx) = sync_channel(bound);
        Self::init(tx, rx, shared_resource, shared_consumer_fn)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    struct TestResource(Vec<TestElem>);

    impl TestResource {
        fn add(&mut self, elem: TestElem) {
            self.0.push(elem);
        }
    }

    struct TestElem(String);

    #[test]
    fn test_one_integer() {
        let consumer_fn = |i| i * 10;

        let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |vec, i| vec.push(i));
        pool_builder.create_pool(
            |tx| vec![1].into_iter().for_each(|i| tx.send(i).unwrap()),
            consumer_fn,
        );

        let result = {
            let mut result = pool_builder.join().unwrap();
            result.sort();
            result
        };

        assert_eq!(result, vec![10]);
    }

    #[test]
    fn test_few_integers() {
        let consumer_fn = |i| i * 10;

        let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |vec, i| vec.push(i));
        pool_builder.create_pool(|tx| (0..5).for_each(|i| tx.send(i).unwrap()), consumer_fn);

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
        pool_builder.create_pool(
            |tx| (0..1000).for_each(|i| tx.send(i).unwrap()),
            consumer_fn,
        );

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
        pool_builder.create_pool_bounded(
            10,
            |tx| (0..1000).for_each(|i| tx.send(i).unwrap()),
            consumer_fn,
        );

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
        pool_builder.create_pool_bounded(
            10,
            |tx| (0..1000).for_each(|i| tx.send(i).unwrap()),
            consumer_fn,
        );

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
        pool_builder.create_pool(
            |tx| (0..1000).for_each(|i| tx.send(i).unwrap()),
            consumer_fn,
        );

        let result = {
            let mut result = pool_builder.join().unwrap();
            result.sort();
            result
        };

        assert_eq!(result, (0..1000).map(consumer_fn).collect::<Vec<_>>());
    }

    #[test]
    fn test_wait_until_pool_finishes() {
        let consumer_fn = |i| i * 10;

        let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |vec, i| vec.push(i));
        let pool = pool_builder.create_pool(
            |tx| tx.send(1).unwrap(),
            move |parg| {
                thread::sleep(Duration::from_millis(10));
                consumer_fn(parg)
            },
        );

        pool.join().unwrap();

        let now = Instant::now();

        let result = {
            let mut result = pool_builder.join().unwrap();
            result.sort();
            result
        };

        let millis = now.elapsed().as_millis();
        assert!(millis < 10);

        assert_eq!(result, vec![10]);
    }

    #[test]
    fn test_integers_multiple_pools_parallel() {
        let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |vec, i| vec.push(i));
        let pools = vec![
            pool_builder.create_pool(|tx| (0..2).for_each(|i| tx.send(i).unwrap()), |i| i * 10),
            pool_builder.create_pool(|tx| (2..5).for_each(|i| tx.send(i).unwrap()), |i| i * 10),
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
    fn test_integers_multiple_pools_sequential() {
        let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |vec, i| vec.push(i));

        let pool =
            pool_builder.create_pool(|tx| (2..5).for_each(|i| tx.send(i).unwrap()), |i| i * 10);

        pool.join().unwrap();

        let pool =
            pool_builder.create_pool(|tx| (0..2).for_each(|i| tx.send(i).unwrap()), |i| i * 10);

        pool.join().unwrap();

        let result = {
            let mut result = pool_builder.join().unwrap();
            result.sort();
            result
        };

        assert_eq!(result, vec![0, 10, 20, 30, 40])
    }

    #[test]
    fn test_integers_multiple_pools_with_parallel_producers() {
        let producer_pool = threadpool::Builder::new().build();

        let pool_builder = SharedResourcePoolBuilder::new(Vec::new(), |vec, i| vec.push(i));
        let pools = vec![
            {
                let producer_pool = producer_pool.clone();
                pool_builder.create_pool(
                    move |tx| {
                        (0..50).into_iter().for_each(|i| {
                            let tx = tx.clone();
                            producer_pool.execute(move || tx.send(i).unwrap())
                        });
                    },
                    |i| i * 10,
                )
            },
            {
                let producer_pool = producer_pool.clone();
                pool_builder.create_pool(
                    move |tx| {
                        (50..100).into_iter().for_each(|i| {
                            let tx = tx.clone();
                            producer_pool.execute(move || tx.send(i).unwrap())
                        });
                    },
                    |i| i * 10,
                )
            },
        ];

        for pool in pools {
            pool.join().unwrap();
        }

        let result = {
            let mut result = pool_builder.join().unwrap();
            result.sort();
            result
        };

        assert_eq!(
            result,
            (0..100).into_iter().map(|i| i * 10).collect::<Vec<_>>()
        )
    }

    #[test]
    fn test_custom_types() {
        let producer_fn = || ('a'..='z').map(|c| String::from(c));

        let pool_builder =
            SharedResourcePoolBuilder::new(TestResource(Vec::new()), |tres, e| tres.add(e));
        pool_builder.create_pool(
            move |tx| producer_fn().for_each(|i| tx.send(i).unwrap()),
            |c| TestElem(c),
        );

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
