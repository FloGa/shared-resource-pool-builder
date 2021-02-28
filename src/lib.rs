//! # Shared Resource Pool Builder
//!
//! A producer-consumer thread pool builder that will eventually consume all items
//! sequentially with a shared resource.
//!
//! ## Code example
//!
//! The purpose of this library is best described with an example, so let's get
//! straight into it:
//!
//! ```rust
//! use shared_resource_pool_builder::SharedResourcePoolBuilder;
//!
//! // Define a pool builder with a Vector as shared resource, and an action that
//! // should be executed sequentially over all items. Note that the second
//! // parameter of this function is a shared consumer function that takes two
//! // parameters: The shared resource and the items it will be used on. These
//! // items will be returned from the pool consumers.
//! let pool_builder = SharedResourcePoolBuilder::new(
//!     Vec::new(),
//!     |vec, i| vec.push(i)
//! );
//!
//! // Create a producer-consumer thread pool that does some work. In this case,
//! // send the numbers 0 to 2 to the consumer. There, they will be multiplied by
//! // 10. Note that the first parameter is a producer function that takes a
//! // Sender like object, like [`std::sync::mpsc::Sender`]. Every item that gets
//! // sent will be processed by the consumer, which is the second parameter to
//! // this function. This function expects an object of the same type that gets
//! // sent by the producer. The consumer is expected to return an object of the
//! // type that the shared consumer takes as the second parameter and will
//! // ultimately used on the shared resource.
//! let pool_1 = pool_builder
//!     .create_pool(
//!         |tx| (0..=2).for_each(|i| tx.send(i).unwrap()),
//!         |i| i * 10
//!     );
//!
//! // Create a second pool. Here, the numbers 3 to 5 will be produced, multiplied
//! // by 2, and being processed by the shared consumer function.
//! let pool_2 = pool_builder
//!     .create_pool(
//!         |tx| (3..=5).for_each(|i| tx.send(i).unwrap()),
//!         |i| i * 2
//!     );
//!
//! // Wait for a specific pool to finish before continuing ...
//! pool_1.join().unwrap();
//!
//! // ... or wait for all pools and the shared consumer to finish their work at
//! // once and return the shared resource. Afterwards, the pool builder can no
//! // longer be used, since the shared resource gets moved.
//! let result = {
//!     let mut result = pool_builder.join().unwrap();
//!
//!     // By default, the pool consumers run in as many threads as there are
//!     // cores in the machine, so the result may not be in the same order as
//!     // they were programmed.
//!     result.sort();
//!     result
//! };
//!
//! assert_eq!(result, vec![0, 6, 8, 10, 10, 20]);
//! ```
//!
//! ## Motivation
//!
//! Imagine you have a huge list of items that need to be processed. Every item
//! can be processed independently, so the natural thing to do is using a thread
//! pool to parallelize the work. But now imagine that the resulting items need to
//! be used together with a resource that does not work on multiple threads. The
//! resulting items will have to be processed sequentially, so the resource does
//! not have to be shared with multiple threads.
//!
//! As a more concrete example, imagine a program that uses a SQLite database for
//! storing computational intensive results. Before doing the actual work, you
//! want to remove obsolete results from the database. Then you need to collect
//! all new data that need to get processed. At last, you want all new data to be
//! actually processed.
//!
//! Normally, this involves three sequential steps: Cleaning, collecting, and
//! processing. These steps need to be sequential because writing operations on
//! the same SQLite database can only be done by one thread at a time. You might
//! want to implement synchronization, so that one thread writes and all
//! additional threads are blocked and wait for their turn. But this can be error
//! prone if not done correctly. You will encounter random panics of locked or
//! busy databases. Furthermore, you lose the ability to use prepared statements
//! efficiently.
//!
//! Enter `shared-resource-pool-builder`. With this, you have the ability to
//! define a *shared resource* and a *shared consumer function*. From this builder
//! you can create producer-consumer pools. The consumed items will then be
//! sequentially given to the shared consumer function to apply the result to the
//! shared resource.
//!
//! The pool consumers will all run within a shared thread pool, so you need not
//! to worry about thrashing your system. You can even limit the number of items
//! that get processed in parallel.
//!
//! To stay with the database example, this means you can define a pool builder
//! with the database connection as the shared resource and a shared consumer
//! function that will remove, insert, or update items. You can do that with an
//! enum, for example.
//!
//! Then, for the cleaning step, you create a pool that produces all items from
//! the database and consumes them by returning an object that causes the item to
//! be either deleted (if obsolete) or ignored by the shared consumer.
//!
//! Next, you can create a pool that produces all new items and consumes them by
//! creating an insert action with each item.
//!
//! Depending on the database layout, these two pools can even be run in parallel.
//! Thanks to the shared consumer, only one delete or insert will run at the same
//! time.
//!
//! Again, depending on the layout, you just need to wait for the inserting pool
//! to finish and create yet another pool that produces all new items that needs
//! to be processed, and consumes them by doing the actual work and returning an
//! appropriate update object for the shared consumer. In the meanwhile, the
//! cleaning pool might as well continue its work.
//!
//! Eventually, you wait for all pools to finish before terminating the program.
//!
//! This way, you do not need to worry about synchronizing multiple writer threads
//! or multiple open database connections. Just split the work so the
//! computational intensive tasks get done in the pool consumers and make the
//! shared consumer just do the finalization.

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
