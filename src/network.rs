use std::{
    collections::HashMap,
    sync::{
        mpsc::{channel, Receiver, SendError, Sender},
        Arc, Mutex,
    },
};

use crate::types::{Message, Payload, Rpc, Try};

type Callbacks<P> = Arc<Mutex<HashMap<usize, Sender<Message<P>>>>>;

/// Network is an abstraction over communication between a node
#[derive(Debug, Clone)]
pub struct Network<P> {
    callbacks: Callbacks<P>,
    outbound: Sender<Message<P>>,
}

impl<P: Payload> Network<P> {
    pub fn new() -> (Self, Receiver<Message<P>>) {
        let (tx, rx) = channel();
        let network = Self {
            callbacks: Callbacks::default(),
            outbound: tx,
        };

        (network, rx)
    }

    pub fn send(&self, msg: Message<P>) -> Try {
        Ok(self
            .outbound
            .send(msg)
            .map_err(|_| "failed to send message")?)
    }

    pub fn rpc(&self, msg: Message<P>) -> Rpc<P> {
        let mut callbacks = self
            .callbacks
            .lock()
            .map_err(|_| "error locking callbacks")?;

        let (tx, rx) = channel();
        let msg_id = msg.body.msg_id.ok_or("rpc must have msg_id")?;
        if callbacks.insert(msg_id, tx).is_some() {
            return Err("duplicate message id use for rpc")?;
        }

        Ok(rx)
    }

    // if the message doesn't have a callback, we give it back to handle regularly
    pub fn check_callback(&self, msg: Message<P>) -> Option<Message<P>> {
        // todo: uhhh
        let Ok(mut callbacks) = self.callbacks.lock() else {
            return Some(msg);
        };

        let Some(msg_id) = msg.body.msg_id else {
            return Some(msg);
        };

        let Some(callback) = callbacks.remove(&msg_id) else {
            return Some(msg);
        };

        if let Err(SendError(msg)) = callback.send(msg) {
            return Some(msg);
        }

        None
    }
}
