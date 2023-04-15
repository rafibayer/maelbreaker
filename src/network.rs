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

        eprintln!("registered callback for RPC {msg_id}");
        self.send(msg)?;
        Ok(rx)
    }

    // if the message doesn't have a callback, we give it back to handle regularly
    pub fn check_callback(&self, msg: Message<P>) -> Option<Message<P>> {
        // todo: uhhh
        let Ok(mut callbacks) = self.callbacks.lock() else {
            return Some(msg);
        };

        let Some(replying_to) = msg.body.in_reply_to else {
            return Some(msg);
        };

        let Some(callback) = callbacks.remove(&replying_to) else {
            return Some(msg);
        };

        if let Err(SendError(msg)) = callback.send(msg) {
            return Some(msg);
        }

        eprintln!("sent callback for rpc {replying_to}");
        None
    }
}

#[cfg(test)]
mod tests {

    use crate::{payload, types::Body};

    use super::*;

    payload!(
        enum PingPong {
            Ping(usize),
            Pong(usize),
        }
    );

    #[test]
    fn test_pingpong() -> Try {
        let (n1_net, n1_out) = Network::new();
        let (n2_net, n2_out) = Network::new();

        let n2_resp = n1_net.rpc(Message {
            src: "n1".into(),
            dest: "n2".into(),
            body: Body {
                msg_id: Some(1),
                in_reply_to: None,
                payload: PingPong::Ping(0),
            },
        })?;

        let n2_reply = n1_out.recv()?.into_reply(PingPong::Pong(0));
        n2_net.send(n2_reply.clone())?;

        assert_eq!(None, n1_net.check_callback(n2_out.recv()?));

        assert_eq!(n2_resp.recv()?, n2_reply);

        Ok(())
    }

    #[test]
    fn test_send() -> Try {
        let msg = Message {
            src: "c1".into(),
            dest: "n1".into(),
            body: Body {
                msg_id: None,
                in_reply_to: None,
                payload: PingPong::Ping(0),
            },
        };

        let (network, outbound) = Network::new();
        network.send(msg.clone())?;
        let sent = outbound.recv()?;
        assert_eq!(msg, sent);

        Ok(())
    }

    #[test]
    fn test_rpc() -> Try {
        let msg = Message {
            src: "c1".into(),
            dest: "n1".into(),
            body: Body {
                msg_id: Some(0),
                in_reply_to: None,
                payload: PingPong::Ping(0),
            },
        };

        let (network, outbound) = Network::new();
        let response = network.rpc(msg.clone())?;

        let sent = outbound.recv()?;
        assert_eq!(msg, sent);
        let reply = msg.into_reply(PingPong::Pong(0));
        assert_eq!(None, network.check_callback(reply.clone()));
        assert_eq!(reply, response.recv()?);

        Ok(())
    }
}
