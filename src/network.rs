//! Defines the Network struct and implementation
use std::{
    collections::HashMap,
    sync::{
        mpsc::{channel, Receiver, SendError, Sender},
        Arc,
    },
};

use anyhow::{anyhow, bail};
use parking_lot::Mutex;

use crate::types::{Message, Payload, Rpc, Try};

type Callbacks<P> = Arc<Mutex<HashMap<usize, Sender<Message<P>>>>>;

/// Network is an abstraction used by Node to communicate with clients, other nodes, and Maelstrom services
#[derive(Debug, Clone)]
pub struct Network<P> {
    callbacks: Callbacks<P>,
    outbound: Sender<Message<P>>,
}

impl<P: Payload> Network<P> {
    /// Constructs a new network, returning it and a Receiver
    /// that will contain outbound messages sent by the Network.
    pub fn new() -> (Self, Receiver<Message<P>>) {
        let (tx, rx) = channel();
        let network = Self {
            callbacks: Callbacks::default(),
            outbound: tx,
        };

        (network, rx)
    }

    /// Try to send a message on the network,
    /// fails if the channel is closed.
    pub fn send(&self, msg: Message<P>) -> Try {
        self.outbound
            .send(msg)
            .map_err(|_| anyhow!("failed to send message"))
    }

    /// Sends a message on the network, returning a Receiver
    /// that will contain the response if one is received.
    /// fails if the message cannot be sent, or if there is no msg_id
    /// on the outbound message.
    pub fn rpc(&self, msg: Message<P>) -> Rpc<P> {
        let mut callbacks = self.callbacks.lock();

        let (tx, rx) = channel();
        let msg_id = msg.body.msg_id.ok_or(anyhow!("rpc must have msg_id"))?;
        if callbacks.insert(msg_id, tx).is_some() {
            bail!("duplicate message id use for rpc");
        }

        eprintln!("registered callback for RPC {msg_id}");
        self.send(msg)?;
        Ok(rx)
    }

    /// Checks if an incoming message is a response to a previously sent RPC.
    /// sends the message as a callback and returns None if so, else
    /// returns the message to the caller
    pub fn check_callback(&self, msg: Message<P>) -> Option<Message<P>> {
        let mut callbacks = self.callbacks.lock();

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
