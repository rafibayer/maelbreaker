use maelbreaker::{
    node::Node,
    payload,
    runtime::Runtime,
    types::{Message, Try},
};
use std::sync::mpsc::Sender;

payload!(
    enum Payload {
        Echo { echo: String },
        EchoOk { echo: String },
    }
);

struct EchoNode {
    network: Sender<Message<Payload>>,
}

impl Node<Payload> for EchoNode {
    fn from_init(network: Sender<Message<Payload>>, _: String, _: Vec<String>) -> Self {
        EchoNode { network }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        let Payload::Echo { echo } = &msg.body.payload else {
            return Err("expected echo")?;
        };

        let echo = echo.clone();
        let reply = msg.into_reply(Payload::EchoOk { echo });
        Ok(self.network.send(reply)?)
    }
}

fn main() -> Try {
    Runtime::<Payload, EchoNode>::run()
}
