use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{mpsc::Sender, Arc, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

use maelbreaker::{
    node::Node,
    payload,
    runtime::Runtime,
    types::{Body, Message, SyncTry, Try},
};
use rand::{thread_rng, Rng};

payload!(
    enum Payload {
        Broadcast {
            message: usize,
        },
        BroadcastOk,
        Replicate {
            messages: Vec<usize>,
            seq: usize,
        },
        ReplicateOk {
            seq: usize,
        },
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

type Unreplicated = Arc<Mutex<HashMap<String, BTreeMap<usize, usize>>>>;

#[derive(Debug)]
struct BroadcastNode {
    neighbors: Vec<String>,
    net: Sender<Message<Payload>>,
    seq: usize,

    messages: HashSet<usize>,
    // neighbor -> seq -> message
    unreplicated: Unreplicated,
}

impl BroadcastNode {
    fn handle_broadcast(&mut self, request: Message<Payload>) -> Try {
        let Payload::Broadcast { message } = request.body.payload else {
            return Err("expected broadcast")?;
        };

        self.messages.insert(message);
        self.add_unreplicated(self.seq, message)?;
        self.seq += 1;

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

    fn handle_replicate(&mut self, request: Message<Payload>) -> Try {
        let Payload::Replicate { messages, seq } = &request.body.payload else {
            return Err("expected replicate")?;
        };

        for message in messages {
            self.messages.insert(*message);
        }

        let seq = *seq;
        let reply = request.into_reply(Payload::ReplicateOk { seq });
        Ok(self.net.send(reply)?)
    }

    fn handle_replicate_ok(&mut self, request: Message<Payload>) -> Try {
        let Payload::ReplicateOk { seq } = &request.body.payload else {
            return Err("expected replicate_ok")?;
        };

        self.remove_unreplicated(&request.src, *seq)
    }

    fn add_unreplicated(&self, seq: usize, message: usize) -> Try {
        let mut unreplicated = self
            .unreplicated
            .lock()
            .map_err(|_| "error locking unreplicated data")?;

        for peer in &self.neighbors {
            unreplicated
                .entry(peer.clone())
                .or_insert(Default::default())
                .insert(seq, message);
        }

        Ok(())
    }

    fn remove_unreplicated(&self, peer: &str, seq: usize) -> Try {
        let mut unreplicated = self
            .unreplicated
            .lock()
            .map_err(|_| "error locking unreplicated data")?;

        // remove all unreplicated data <= acked sequence number from peer
        unreplicated
            .get_mut(peer)
            .ok_or("missing peer")?
            .retain(|sequence, _| *sequence > seq);

        Ok(())
    }

    fn replicator(
        network: Sender<Message<Payload>>,
        id: String,
        neighbors: Vec<String>,
        unreplicated: Unreplicated,
    ) -> JoinHandle<SyncTry> {
        thread::spawn::<_, SyncTry>(move || loop {
            thread::sleep(Duration::from_millis(600 + thread_rng().gen_range(0..100)));
            {
                let locked = unreplicated
                    .lock()
                    .map_err(|_| "error locking unreplicated data")?;
                for peer in &neighbors {
                    let Some(peer_unreplicated) = locked.get(peer) else {
                        continue;
                    };

                    let Some(highest_seq) = peer_unreplicated.keys().max() else {
                        continue;
                    };

                    let replicate = Message {
                        src: id.clone(),
                        dest: peer.clone(),
                        body: Body {
                            msg_id: None,
                            in_reply_to: None,
                            payload: Payload::Replicate {
                                messages: peer_unreplicated.values().into_iter().cloned().collect(),
                                seq: *highest_seq,
                            },
                        },
                    };

                    network.send(replicate)?;
                }
            }
        })
    }
}

impl Node<Payload> for BroadcastNode {
    fn from_init(
        network: Sender<Message<Payload>>,
        node_id: String,
        node_ids: Vec<String>,
    ) -> Self {
        let neighbors: Vec<String> = node_ids.into_iter().filter(|id| id != &node_id).collect();
        let unreplicated = Unreplicated::default();

        // start batch replicator
        BroadcastNode::replicator(
            network.clone(),
            node_id.clone(),
            neighbors.clone(),
            unreplicated.clone(),
        );

        Self {
            neighbors,
            net: network,
            seq: 0,
            messages: Default::default(),
            unreplicated,
        }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        match &msg.body.payload {
            Payload::Broadcast { message: _ } => self.handle_broadcast(msg)?,
            Payload::Read => self.handle_read(msg)?,
            Payload::Topology { topology: _ } => self.handle_topology(msg)?,
            Payload::Replicate {
                messages: _,
                seq: _,
            } => self.handle_replicate(msg)?,
            Payload::ReplicateOk { seq: _ } => self.handle_replicate_ok(msg)?,
            _ => {}
        };

        Ok(())
    }
}

fn main() -> Try {
    Runtime::<Payload, BroadcastNode>::run()
}
