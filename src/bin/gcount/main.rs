use std::{
    collections::HashMap,
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    thread,
};

use maelbreaker::{
    error::ErrorCode,
    network::Network,
    node::Node,
    payload,
    runtime::Runtime,
    types::{Body, Message, Try},
};

// To use a service, simply send an RPC request to the node ID of the service you want to use:
// for instance, seq-kv. The service will send you a response message.
payload!(
    enum Payload {
        Add {
            delta: usize,
        },
        AddOk,
        Read,

        // shared by challenge and seq-kv
        ReadOk {
            value: usize,
        },

        #[serde(rename = "read")]
        KvRead {
            key: String,
        },

        #[serde(rename = "cas")]
        KvCas {
            key: String,
            from: usize,
            to: usize,
            create_if_not_exists: bool,
        },
        #[serde(rename = "cas_ok")]
        KvCasOk,

        Error {
            code: usize,
            text: String,
        },
    }
);

struct GCountNode {
    id: String,
    ids: Vec<String>,
    cache: HashMap<String, usize>, // last seed value for seq-db keys
    network: Network<Payload>,
    unapplied: Arc<AtomicUsize>,
    seq: Arc<AtomicUsize>,
}

impl Node<Payload> for GCountNode {
    fn from_init(network: Network<Payload>, id: String, ids: Vec<String>) -> Self {
        eprintln!("initializing gcount node {id}");
        let unapplied = Arc::new(AtomicUsize::new(0));
        let seq = Arc::new(AtomicUsize::new(5));

        GCountNode::worker(id.clone(), network.clone(), unapplied.clone(), seq.clone());
        Self {
            id,
            ids,
            cache: Default::default(),
            network,
            unapplied,
            seq,
        }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        match &msg.body.payload {
            Payload::Add { .. } => self.handle_add(msg),
            Payload::Read => self.handle_read(msg),
            _ => Ok(()),
        }
    }
}

/*
we will maintain a sum of unapplied writes.
    - we will read the current value in DB
    - we will try to CAS (current, current + unapplied)
        - we will keep trying until we get an ack
        - OR error = PreconditionFailed
            - at which point we will retry from the top
*/

impl GCountNode {
    fn worker(
        id: String,
        network: Network<Payload>,
        unapplied: Arc<AtomicUsize>,
        seq: Arc<AtomicUsize>,
    ) {
        thread::spawn(move || {
            // seed DB to ensure key is created, we don't care if we fail
            let seed = GCountNode::cas_db(&seq, &id, &id, 0, 0, &network);
            eprintln!("seed result: {seed:#?}");
            eprintln!("initializing gcount worker {id}");

            loop {
                let to_apply = unapplied.load(SeqCst);
                if to_apply > 0 {
                    let Ok(from) = GCountNode::read_db(&id, &network, seq.clone(), &id) else {
                        continue;
                    };

                    let to = from + to_apply;

                    // cas until we get cas_ok or precondition failed, either way
                    // we know our write was applied since we are the only node writing
                    // to this seq-kv key
                    loop {
                        let result = GCountNode::cas_db(&seq, &id, &id, from, to, &network);
                        if let Err(e) = result {
                            eprintln!("failed to send/recv cas: {e:#?}");
                            continue;
                        }

                        let result = result.unwrap();

                        match result.body.payload {
                            // todo: we are assuming error == precondition failed
                            Payload::KvCasOk | Payload::Error { .. } => {
                                unapplied.fetch_sub(to_apply, SeqCst);
                                break;
                            }
                            _ => continue,
                        }
                    }
                }
            }
        });
    }

    fn read_db(
        id: &str,
        network: &Network<Payload>,
        seq: Arc<AtomicUsize>,
        key: &str,
    ) -> Result<usize, Box<dyn Error>> {
        let seq = seq.fetch_add(1, SeqCst);
        eprintln!("reading from seq-kv {seq}");

        let read = Message {
            src: id.into(),
            dest: "seq-kv".into(),
            body: Body {
                msg_id: Some(seq),
                in_reply_to: None,
                payload: Payload::KvRead { key: key.into() },
            },
        };

        eprintln!("waiting for response from seq-kv {seq}");
        let Payload::ReadOk { value } = network
            .rpc(read)
            .map_err(|_| "failed to read")?
            .recv()?.body.payload else {
                // what about errors? maybe just log and continue; here?
                // should probably be recv_timeout due to partitions
                return Err("expected read_ok")?;
            };

        Ok(value)
    }

    fn cas_db(
        seq: &Arc<AtomicUsize>,
        id: &str,
        key: &str,
        previous: usize,
        target: usize,
        network: &Network<Payload>,
    ) -> Result<Message<Payload>, Box<dyn Error>> {
        let seq = seq.fetch_add(1, SeqCst);
        let cas = Message {
            src: id.into(),
            dest: "seq-kv".into(),
            body: Body {
                msg_id: Some(seq),
                in_reply_to: None,
                payload: Payload::KvCas {
                    key: key.into(),
                    from: previous,
                    to: target,
                    create_if_not_exists: true,
                },
            },
        };

        let cas_callback = network.rpc(cas).map_err(|_| "failed to send cas rpc")?;
        let cas_resp = cas_callback
            .recv()
            .map_err(|_| "failed to recv cas response")?;
        Ok(cas_resp)
    }

    fn handle_add(&self, msg: Message<Payload>) -> Try {
        let Payload::Add { delta } = &msg.body.payload else {
            return Err("expected add")?;
        };

        self.unapplied.fetch_add(*delta, SeqCst);
        let reply = msg.into_reply(Payload::AddOk);
        self.network.send(reply)
    }

    fn handle_read(&mut self, msg: Message<Payload>) -> Try {
        let mut value = 0;

        // read db entry for each node, or returned the cached value
        for id in &self.ids {
            let read_result = GCountNode::read_db(&self.id, &self.network, self.seq.clone(), id);
            let read = match read_result {
                Ok(read) => {
                    // update cache
                    self.cache.insert(id.clone(), read);
                    read
                }
                Err(_) => *self.cache.entry(id.clone()).or_insert(0),
            };
            value += read;
        }

        let reply = msg.into_reply(Payload::ReadOk { value });
        self.network.send(reply)
    }
}

fn main() -> Try {
    Runtime::<Payload, GCountNode>::run()
}
