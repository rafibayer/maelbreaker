use std::sync::mpsc::Sender;

use maelbreaker::{
    node::Node,
    payload,
    runtime::run,
    types::{Message, Try},
};

payload!(
    enum Payload {
        Generate,
        GenerateOk { id: String },
    }
);

struct UniqueNode {
    id: String,
    seq: usize,
    net: Sender<Message<Payload>>,
}

impl Node<Payload> for UniqueNode {
    fn from_init(net: Sender<Message<Payload>>, id: String, _: Vec<String>) -> Self {
        Self { id, seq: 0, net }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        let id = format!("{}-{}", self.id, self.seq);
        let reply = msg.into_reply(Payload::GenerateOk { id });

        self.seq += 1;
        self.net.send(reply)?;

        Ok(())
    }
}

fn main() {
    run::<Payload, UniqueNode>().unwrap();
}
