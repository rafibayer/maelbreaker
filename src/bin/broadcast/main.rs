use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::Sender,
};

use maelbreaker::{
    node::Node,
    payload,
    runtime::run,
    types::{Message, Try},
};

payload!(
    enum Payload {
        Broadcast {
            message: usize,
        },
        BroadcastOk,
        Read,
        ReadOk {
            messages: Vec<usize>,
        },
        Topology {
            topology: HashMap<String, Vec<String>>,
        },
        TopologyOk,
    }
);

struct BroadcastNode {
    id: String,
    neighbors: Vec<String>,
    net: Sender<Message<Payload>>,

    messages: HashSet<usize>,
}

impl BroadcastNode {
    fn handle_broadcast(&mut self, request: Message<Payload>) -> Try {
        let Payload::Broadcast { message } = request.body.payload else {
            return Err("expected broadcast")?;
        };

        self.messages.insert(message);
        let reply = request.into_reply(Payload::BroadcastOk);
        Ok(self.net.send(reply)?)
    }

    fn handle_read(&self, request: Message<Payload>) -> Try {
        let reply = request.into_reply(Payload::ReadOk {
            messages: self.messages.clone().into_iter().collect(),
        });
        Ok(self.net.send(reply)?)
    }

    fn handle_topology(&self, request: Message<Payload>) -> Try {
        let reply = request.into_reply(Payload::TopologyOk);
        Ok(self.net.send(reply)?)
    }
}

impl Node<Payload> for BroadcastNode {
    fn from_init(
        network: Sender<Message<Payload>>,
        node_id: String,
        node_ids: Vec<String>,
    ) -> Self {
        Self {
            neighbors: node_ids.into_iter().filter(|id| id != &node_id).collect(),
            id: node_id,
            net: network,
            messages: HashSet::new(),
        }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        match &msg.body.payload {
            Payload::Broadcast { message: _ } => self.handle_broadcast(msg)?,
            Payload::Read => self.handle_read(msg)?,
            Payload::Topology { topology: _ } => self.handle_topology(msg)?,
            _ => {}
        };

        Ok(())
    }
}

fn main() -> Try {
    run::<Payload, BroadcastNode>()
}
