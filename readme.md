# Maelbreaker

<img src="maelbreaker.png"  width="200" height="200">

A Rust runtime library for [Maelstrom](https://github.com/jepsen-io/maelstrom)

[Sample Gossip Glomers Solutions](/src/bin)

## Features
- Deserialize messages into strongly typed enums
- Send and RPC support
- Flexible and extensible messaging
- Decoupled input/output threads

## Example: [Echo](https://fly.io/dist-sys/1/)
Example usage to solve the first of the Gossip Glomers challenges (*more examples in [/src/bin](/src/bin)*)
```rust
// main.rs
use maelbreaker::{
    network::Network,
    node::Node,
    payload,
    runtime::Runtime,
    types::{Message, Try},
};

// We derive all the traits needed for our message types
// using the `payload!` macro.
payload!(
    enum Payload {
        Echo { echo: String },
        EchoOk { echo: String },
    }
);

struct EchoNode {
    // Network is used to send messages from our node
    network: Network<Payload>,
}

impl Node<Payload> for EchoNode {
    // We are also passed the nodes ID, as well as a list of all node IDs in the cluster,
    // but we don't use them for this challenge.
    fn from_init(network: Network<Payload>, _node_id: String, _node_ids: Vec<String>) -> Self {
        EchoNode { network }
    }

    fn handle_message(&mut self, msg: Message<Payload>) -> Try {
        let Payload::Echo { echo } = &msg.body.payload else {
            return Err("expected echo")?;
        };

        let echo = echo.clone();
        // we use our handy into_reply method to consume a request
        // and produce a reply with the correct src, dest, and in_reply_to fields
        let reply = msg.into_reply(Payload::EchoOk { echo });

        // we send our response onto the network!
        self.network.send(reply)
    }
}

fn main() -> Try {
    // we start the runtime with our Node and Payload implementations
    Runtime::<Payload, EchoNode>::run()
}
```