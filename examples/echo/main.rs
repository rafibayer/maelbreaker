use anyhow::bail;
use maelbreaker::{
    network::Network,
    node::Node,
    payload,
    runtime::Runtime,
    types::{Message, Try},
};
payload!(
    enum Payload {
        Echo { echo: String },
        EchoOk { echo: String },
    }
);

struct EchoNode {
    network: Network<Payload>,
}

impl Node<Payload> for EchoNode {
    fn from_init(network: Network<Payload>, _: String, _: Vec<String>) -> Self {
        EchoNode { network }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        let Payload::Echo { echo } = &msg.body.payload else {
            bail!("expected echo");
        };

        let echo = echo.clone();
        let reply = msg.into_reply(Payload::EchoOk { echo });
        self.network.send(reply)
    }
}

fn main() -> Try {
    Runtime::<Payload, EchoNode>::run()
}
