use std::collections::{BTreeMap, HashMap};

use maelbreaker::{
    network::Network,
    node::Node,
    payload,
    runtime::Runtime,
    types::{Message, Try},
};

payload!(
    enum Payload {
        Send {
            key: String,
            msg: usize,
        },
        SendOk {
            offset: usize,
        },
        Poll {
            offsets: HashMap<String, usize>,
        },
        PollOk {
            msgs: HashMap<String, Vec<[usize; 2]>>,
        },
        CommitOffsets {
            offsets: HashMap<String, usize>,
        },
        CommitOffsetsOk,
        ListCommittedOffsets {
            keys: Vec<String>,
        },
        ListCommittedOffsetsOk {
            offsets: HashMap<String, usize>,
        },
    }
);

#[derive(Debug, Clone, Default)]
struct Log {
    commit_offset: usize,
    entries: BTreeMap<usize, usize>,
}

struct KafkaNode {
    node_id: String,
    node_ids: Vec<String>,
    network: Network<Payload>,
    logs: HashMap<String, Log>,
}

impl Node<Payload> for KafkaNode {
    fn from_init(network: Network<Payload>, node_id: String, node_ids: Vec<String>) -> Self {
        Self {
            node_id,
            node_ids,
            network,
            logs: Default::default(),
        }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        match &msg.body.payload {
            Payload::Send { .. } => self.handle_send(msg),
            Payload::Poll { .. } => self.handle_poll(msg),
            Payload::CommitOffsets { .. } => self.handle_commit_offsets(msg),
            Payload::ListCommittedOffsets { .. } => self.handle_list_committed_offsets(msg),
            _ => Ok(()),
        }
    }
}

impl KafkaNode {
    fn handle_send(&mut self, msg: Message<Payload>) -> Try {
        let Payload::Send { key, msg: message } = &msg.body.payload else {
            return Err("expected send")?;
        };

        let log = self.logs.entry(key.clone()).or_default();
        let offset = log.entries.keys().max().map(|i| i + 1).unwrap_or(0);
        log.entries.insert(offset, *message);

        let reply = msg.into_reply(Payload::SendOk { offset });
        self.network.send(reply)
    }

    fn handle_poll(&mut self, msg: Message<Payload>) -> Try {
        let Payload::Poll { offsets } = &msg.body.payload else {
            return Err("expected poll")?;
        };

        let mut msgs = HashMap::<String, Vec<[usize; 2]>>::new();
        for (log_key, min_offset) in offsets {
            let log = self.logs.entry(log_key.clone()).or_default();
            let log_msgs = msgs.entry(log_key.clone()).or_default();

            for (offset, value) in &log.entries {
                if offset >= min_offset {
                    log_msgs.push([*offset, *value]);
                }
            }
        }

        let reply = msg.into_reply(Payload::PollOk { msgs });
        self.network.send(reply)
    }

    fn handle_commit_offsets(&mut self, msg: Message<Payload>) -> Try {
        let Payload::CommitOffsets { offsets } = &msg.body.payload else {
            return Err("expected commit_offsets")?
        };

        for (log_key, commit_offset) in offsets {
            (*self.logs.entry(log_key.clone()).or_default()).commit_offset = *commit_offset;
        }

        let reply = msg.into_reply(Payload::CommitOffsetsOk);
        self.network.send(reply)
    }

    fn handle_list_committed_offsets(&mut self, msg: Message<Payload>) -> Try {
        let Payload::ListCommittedOffsets { keys } = &msg.body.payload else {
            return Err("expected list_committed_offsets")?
        };

        let mut offsets = HashMap::new();
        for key in keys.clone() {
            offsets.insert(key.clone(), self.logs.entry(key).or_default().commit_offset);
        }

        let reply = msg.into_reply(Payload::ListCommittedOffsetsOk { offsets });
        self.network.send(reply)
    }
}

fn main() -> Try {
    Runtime::<Payload, KafkaNode>::run()
}
