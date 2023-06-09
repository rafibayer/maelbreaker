use maelbreaker::{
    network::Network,
    node::Node,
    payload,
    runtime::Runtime,
    types::{Message, Try},
};

use anyhow::anyhow;

payload!(
    enum Payload {
        Generate,
        GenerateOk { id: String },
    }
);

struct UniqueNode {
    id: String,
    net: Network<Payload>,
}

impl Node<Payload> for UniqueNode {
    fn from_init(net: Network<Payload>, id: String, _: Vec<String>) -> Self {
        Self { id, net }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        let id = format!(
            "{}-{}",
            self.id,
            msg.body.msg_id.ok_or(anyhow!("missing id"))?
        );
        let reply = msg.into_reply(Payload::GenerateOk { id });
        self.net.send(reply)
    }
}

fn main() -> Try {
    Runtime::<Payload, UniqueNode>::run()
}
