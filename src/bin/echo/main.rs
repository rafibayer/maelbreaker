use std::sync::mpsc::Sender;

use maelbreaker::{
    node::Node,
    payload,
    runtime::run,
    types::{Message, Try},
};

payload!(
    enum Payload {
        Echo { echo: String },
        EchoOk { echo: String },
    }
);

struct EchoNode {
    network: Sender<Message<Payload>>,
    seq: usize,
}

impl Node<Payload> for EchoNode {
    fn from_init(
        network: Sender<Message<Payload>>,
        _node_id: String,
        _node_ids: Vec<String>,
    ) -> Self {
        EchoNode { network, seq: 0 }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        let Payload::Echo { echo } = &msg.body.payload else {
            return Err("expected echo")?;
        };

        let echo = echo.clone();
        let reply = msg.into_reply(Payload::EchoOk { echo });

        self.seq += 1;
        self.network.send(reply)?;
        Ok(())
    }
}

fn main() {
    run::<Payload, EchoNode>().unwrap();
}
