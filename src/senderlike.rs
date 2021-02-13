use std::sync::mpsc::{SendError, Sender, SyncSender};

pub(crate) trait SenderLike {
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
