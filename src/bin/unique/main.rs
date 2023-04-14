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
    net: Sender<Message<Payload>>,
}

impl Node<Payload> for UniqueNode {
    fn from_init(net: Sender<Message<Payload>>, id: String, _: Vec<String>) -> Self {
        Self { id, net }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        let id = format!("{}-{}", self.id, msg.body.msg_id.ok_or("missing id")?);
        let reply = msg.into_reply(Payload::GenerateOk { id });
        Ok(self.net.send(reply)?)
    }
}

fn main() -> Try {
    run::<Payload, UniqueNode>()
}
