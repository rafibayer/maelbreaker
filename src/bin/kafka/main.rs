use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{channel, Sender},
        Arc,
    },
    thread,
};

use maelbreaker::{
    network::Network,
    node::Node,
    payload,
    runtime::Runtime,
    types::{BodyBuilder, Message, Try},
};

/*

implementation: partitioned kafka

    each log is owned by exactly 1 KafkaNode.
        we partition a given log_key across node_ids as follows
            owner = node_ids[ hash(log_key) % node_ids ]

        we perform this process for each request, and then make a decision
            req = Request()
            res = Response()
            remote = false
            for each log_key needed to fulfill req
                owner = partition(log_key)
                if owner is self
                    res += self.handle(req)
                else
                    remote = true
                    continue

            if remote
                enqueue remote job (req, res)
            else
                req.respond(res)

        in another thread, we dequeue (req, res)
            for each log_key needed to fulfill req
                owner = partition(log_key)
                    if owner is self
                        continue
                    else
                        remote_result = rpc(subset(req, log_key))
                        res += remote_result

            req.respond(res)

    In plain english:
        for each request, we collect any data owned by the local node.
        if we need data from other nodes, we push our local data and the request to a background task.
        in the background task, we reach out to other nodes, and ask for their subset of data
        we merge the response, and finally reply to the client

    Preventing deadlocks:
    we use background threads to prevent deadlocks.
        If we make an RPC directly in a request (ex. send)
        the following scenario can happen:

            assume Log Kx is owned by Node Nx

            c1 Send(k1, 123) -> n2
            c2 Send(k2, 456) -> n1

            now n1 needs k2 data from n2,
            and n2 needs k1 data from n1.

            both nodes send RPCs and await responses.
            neither node will process the incoming forwarded send until they finish
            processing the current requests.

            deadlock!

        hence, we use a background task, a node won't block handling of a message
        if it requires data from a remote node. Conversely, a node can handle a request
        only requring local data immediately. Since we only forward the subsets of requests
        to the node that owns the log, they can serve the resonse while they assemble cross-partition
        responses in the background.
*/

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

#[derive(Clone, Default)]
struct Sequence {
    shared: Arc<AtomicUsize>,
}

impl Sequence {
    fn get(&self) -> usize {
        self.shared.fetch_add(1, Ordering::SeqCst)
    }
}

fn get_partition(key: &str, nodes: &[String]) -> String {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish() as usize;
    nodes[hash % nodes.len()].clone()
}

struct PollJob {
    client_poll: Message<Payload>,
    msgs: HashMap<String, Vec<[usize; 2]>>,
}

struct SendJob {
    client_send: Message<Payload>,
    partition: String,
}

struct ListCommittedOffsetsJob {
    client_list_committed: Message<Payload>,
    offsets: HashMap<String, usize>,
}

#[derive(Debug, Clone, Default)]
struct Log {
    commit_offset: usize,
    entries: BTreeMap<usize, usize>,
}

struct KafkaNode {
    sequence: Sequence,

    node_id: String,
    node_ids: Vec<String>,
    network: Network<Payload>,
    logs: HashMap<String, Log>,

    poll_worker: Sender<PollJob>,
    send_worker: Sender<SendJob>,
    list_committed_worker: Sender<ListCommittedOffsetsJob>,
}

impl Node<Payload> for KafkaNode {
    fn from_init(network: Network<Payload>, node_id: String, node_ids: Vec<String>) -> Self {
        let sequence = Sequence::default();

        let poll_worker = KafkaNode::poll_worker(
            sequence.clone(),
            node_id.clone(),
            node_ids.clone(),
            network.clone(),
        );

        let send_worker = KafkaNode::send_worker(
            sequence.clone(),
            node_id.clone(),
            node_ids.clone(),
            network.clone(),
        );

        let list_committed_worker = KafkaNode::list_committed_worker(
            sequence.clone(),
            node_id.clone(),
            node_ids.clone(),
            network.clone(),
        );

        Self {
            sequence,
            node_id,
            node_ids,
            network,
            logs: Default::default(),

            poll_worker,
            send_worker,
            list_committed_worker,
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

        let partition = get_partition(key, &self.node_ids);

        // send to remote partition
        if partition != self.node_id {
            eprintln!("send for log {key} owned by remote partition {partition}");
            // we should never get a request belonging to a different node
            // from a server, only a client. else our hashing is busted.
            assert!(msg.src.starts_with('c'));

            let job = SendJob {
                client_send: msg,
                partition,
            };

            return Ok(self
                .send_worker
                .send(job)
                .map_err(|_| "failed to run poll job")?);
        }

        // apply locally
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

        let mut remote_logs = false;
        let mut msgs = HashMap::<String, Vec<[usize; 2]>>::new();
        for (log_key, min_offset) in offsets {
            let partition = get_partition(log_key, &self.node_ids);
            if partition != self.node_id {
                eprintln!("poll includes remote log {log_key} owned by partition {partition}");
                remote_logs = true;
                continue;
            }
            let log = self.logs.entry(log_key.clone()).or_default();
            let log_msgs = msgs.entry(log_key.clone()).or_default();

            for (offset, value) in &log.entries {
                if offset >= min_offset {
                    log_msgs.push([*offset, *value]);
                }
            }
        }

        if remote_logs {
            // finish assembling the response from remote worker
            let job = PollJob {
                client_poll: msg,
                msgs,
            };

            Ok(self
                .poll_worker
                .send(job)
                .map_err(|_| "failed to run poll job")?)
        } else {
            // case for when we only have local logs to serve
            let reply = msg.into_reply(Payload::PollOk { msgs });
            self.network.send(reply)
        }
    }

    fn handle_commit_offsets(&mut self, msg: Message<Payload>) -> Try {
        let Payload::CommitOffsets { offsets } = &msg.body.payload else {
            return Err("expected commit_offsets")?
        };

        for (log_key, commit_offset) in offsets {
            let partition = get_partition(log_key, &self.node_ids);
            if partition == self.node_id {
                self.logs.entry(log_key.clone()).or_default().commit_offset = *commit_offset;
            } else {
                eprintln!("commit for log {log_key} owned by partition {partition}");
                let remote_offset = HashMap::from([(log_key.clone(), *commit_offset)]);
                let payload = Payload::CommitOffsets {
                    offsets: remote_offset,
                };
                let remote_commit = Message::new(
                    self.node_id.clone(),
                    partition,
                    BodyBuilder::new(payload)
                        .msg_id(self.sequence.get())
                        .build(),
                );

                self.network.send(remote_commit)?;
            }
        }

        let reply = msg.into_reply(Payload::CommitOffsetsOk);
        self.network.send(reply)
    }

    fn handle_list_committed_offsets(&mut self, msg: Message<Payload>) -> Try {
        let Payload::ListCommittedOffsets { keys } = &msg.body.payload else {
            return Err("expected list_committed_offsets")?
        };

        let mut remote_commits = false;
        let mut offsets = HashMap::new();
        for key in keys.clone() {
            let partition = get_partition(&key, &self.node_ids);
            if partition != self.node_id {
                eprintln!("list committed includes log {key} owned by partition {partition}");
                remote_commits = true;
                continue;
            }

            offsets.insert(key.clone(), self.logs.entry(key).or_default().commit_offset);
        }

        if remote_commits {
            let job = ListCommittedOffsetsJob {
                client_list_committed: msg,
                offsets,
            };

            Ok(self
                .list_committed_worker
                .send(job)
                .map_err(|_| "failed to run list committed job")?)
        } else {
            let reply = msg.into_reply(Payload::ListCommittedOffsetsOk { offsets });
            self.network.send(reply)
        }
    }

    fn poll_worker(
        seq: Sequence,
        node_id: String,
        node_ids: Vec<String>,
        network: Network<Payload>,
    ) -> Sender<PollJob> {
        let (tx, rx) = channel();

        thread::spawn(move || {
            for job in rx {
                let PollJob {
                    client_poll,
                    mut msgs,
                } = job;
                let Payload::Poll { offsets } = &client_poll.body.payload else {
                    eprintln!("expected poll");
                    continue;
                };

                for (log_key, offset) in offsets {
                    let partition = get_partition(log_key, &node_ids);
                    if partition == node_id {
                        // we should already have local logs
                        continue;
                    }

                    let payload = Payload::Poll {
                        offsets: HashMap::from([(log_key.clone(), *offset)]),
                    };
                    let body = BodyBuilder::new(payload).msg_id(seq.get()).build();
                    let remote_poll = Message::new(&node_id, partition, body);
                    let Ok(result) = network.rpc(remote_poll) else {
                        eprintln!("failed to send remote poll rpc");
                        continue;
                    };

                    let result = result.recv().unwrap();
                    let Payload::PollOk { msgs: remote_msgs } = result.body.payload else {
                        eprintln!("expected poll_ok");
                        continue;
                    };

                    for (remote_key, remote_offsets) in remote_msgs {
                        msgs.insert(remote_key, remote_offsets);
                    }
                }

                // send the merged response
                let reply = client_poll.into_reply(Payload::PollOk { msgs });
                network.send(reply).unwrap();
            }
        });

        tx
    }

    fn send_worker(
        seq: Sequence,
        node_id: String,
        _: Vec<String>,
        network: Network<Payload>,
    ) -> Sender<SendJob> {
        let (tx, rx) = channel();
        thread::spawn(move || {
            for job in rx {
                let SendJob {
                    client_send,
                    partition,
                } = job;

                let mut fwd = client_send.clone();

                fwd.src = node_id.clone();
                fwd.dest = partition;
                fwd.body.msg_id = Some(seq.get());

                let Ok(result) = network.rpc(fwd) else {
                    eprintln!("failed to forward send to remote partition");
                    continue;
                };

                let Ok(result) = result.recv() else {
                    eprintln!("failed to recv forward send to remote partition");
                    continue;
                };

                let Payload::SendOk { offset } = result.body.payload else {
                    eprintln!("expected send_ok");
                    continue;
                };

                let reply = client_send.into_reply(Payload::SendOk { offset });
                network.send(reply).unwrap();
            }
        });

        tx
    }

    fn list_committed_worker(
        seq: Sequence,
        node_id: String,
        node_ids: Vec<String>,
        network: Network<Payload>,
    ) -> Sender<ListCommittedOffsetsJob> {
        let (tx, rx) = channel();

        thread::spawn(move || {
            for job in rx {
                let ListCommittedOffsetsJob {
                    client_list_committed,
                    mut offsets,
                } = job;

                let Payload::ListCommittedOffsets { keys } = &client_list_committed.body.payload else {
                    eprintln!("expected list_committed_offsets");
                    continue;
                };

                for log_key in keys {
                    let partition = get_partition(log_key, &node_ids);
                    if partition == node_id {
                        // we should already have local committs
                        continue;
                    }

                    let payload = Payload::ListCommittedOffsets {
                        keys: vec![log_key.clone()],
                    };

                    let body = BodyBuilder::new(payload).msg_id(seq.get()).build();
                    let remote_list_committed = Message::new(&node_id, partition, body);

                    let Ok(result) = network.rpc(remote_list_committed) else {
                        eprintln!("failed to send remote list committed rpc");
                        continue;
                    };

                    let result = result.recv().unwrap();
                    let Payload::ListCommittedOffsetsOk { offsets : remote_offsets } = result.body.payload else {
                        eprintln!("expected ListCommittedOffsetsOk");
                        continue;
                    };

                    for (remote_key, remote_offset) in remote_offsets {
                        offsets.insert(remote_key, remote_offset);
                    }
                }

                // send the merged response
                let reply =
                    client_list_committed.into_reply(Payload::ListCommittedOffsetsOk { offsets });
                network.send(reply).unwrap();
            }
        });

        tx
    }
}

fn main() -> Try {
    Runtime::<Payload, KafkaNode>::run()
}
