use std::{
    io::{stdin, stdout, BufRead, Write},
    sync::mpsc::{channel, Receiver, Sender},
    thread::{self, JoinHandle},
};

const EOI: &str = "EOI";

use crate::{
    network::Network,
    node::Node,
    types::{Init, Message, Payload, SyncTry, Try},
};

pub struct Runtime<P, N>(std::marker::PhantomData<P>, std::marker::PhantomData<N>);
impl<P, N> Runtime<P, N>
where
    P: Payload,
    N: Node<P>,
{
    pub fn run() -> Try {
        let (stdin_tx, stdin_rx) = channel();
        let (stdout_tx, stdout_rx) = channel();

        // stdin thread: decouples stdin reads from node message processing
        thread::spawn(move || {
            let stdin = stdin().lock().lines();

            for line in stdin {
                let line = line.unwrap();
                stdin_tx.send(line).unwrap();
            }
        });

        // stdout thread: decouples stdout writes from node message processing
        thread::spawn(move || {
            let mut stdout = stdout().lock();

            for message in stdout_rx {
                writeln!(&mut stdout, "{message}").unwrap();
            }
        });

        // we give the node a Sender so it can pass outbound messages to stdout
        // and a receiver so it can pull inbound messages from stdin
        eprintln!("Starting runtime...\nWaiting for init message");
        Runtime::<P, N>::run_internal(stdout_tx, stdin_rx)?;
        Ok(())
    }

    fn run_internal(tx: Sender<String>, rx: Receiver<String>) -> Try {
        let init = &rx.recv()?;
        eprintln!("Got init: {init}");
        let init: Message<Init> = serde_json::from_str(init)?;
        let Init::Init { node_id, node_ids } = &init.body.payload else {
            return Err("expected init as first message")?;
        };

        // the network is how the node communicates with the runtime
        let (network, node_receiver) = Network::new();
        let node = N::from_init(network.clone(), node_id.clone(), node_ids.clone());

        // we are using a msg_id here that might be used by the node,
        // which is against protocol, but maelstrom doesn't seem to mind
        let reply = init.into_reply(Init::InitOk);

        eprintln!("Starting outbound processing and sending init_ok");
        Runtime::<P, N>::process_output(reply, tx, node_receiver);

        eprintln!("Starting inbound processing");
        if let Err(e) = Runtime::process_input(rx, network, node) {
            eprintln!("failed to process input: {e:#?}");
        }

        eprintln!("Shutting down...");
        Ok(())
    }

    fn process_output(
        reply: Message<Init>,
        tx: Sender<String>,
        node_receiver: Receiver<Message<P>>,
    ) -> JoinHandle<SyncTry> {
        // output thread: decouples node sending outbound messages from
        // node receiving inbound messages. This way, a node may be sending messages
        // even if it isn't receiving any.
        thread::spawn::<_, SyncTry>(move || {
            // send the init_ok
            let mut json = serde_json::to_string(&reply)?;
            eprintln!("Writing init_ok: {json}");
            tx.send(json)?;

            // reply to other messages
            loop {
                let outbound = node_receiver.recv()?;
                json = serde_json::to_string(&outbound)?;
                eprintln!("Writing outbound message: {json}");
                tx.send(json)?;
            }
        })
    }

    fn process_input(rx: Receiver<String>, network: Network<P>, mut node: N) -> Try {
        let (json_tx, json_rx) = channel();

        // callback thread: allows us to process input and check for pending
        // rpc callbacks even if the node is still handling a message.
        thread::spawn(move || {
            for line in rx {
                if line == EOI {
                    eprintln!("Got EOI");

                    break;
                }

                eprintln!("Got message: {line}");
                let message: Message<P> = serde_json::from_str(&line).unwrap();

                // we try checking for pending callbacks for the message, if not,
                // check_callback returns ownership of the message so that we may deliver
                // it to the node as a regular message rather than an RPC response
                if let Some(message) = network.check_callback(message) {
                    json_tx.send(message).unwrap();
                }
            }
        });

        for message in json_rx {
            node.handle_message(message)?;
        }

        eprintln!("done processing input");
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::{sync::mpsc::Sender, thread::JoinHandle};

    use crate::{payload, types::BodyBuilder};

    use super::*;

    payload!(
        enum EchoPayload {
            Echo { echo: String },
            EchoOk { echo: String },
        }
    );

    struct EchoNode {
        network: Network<EchoPayload>,
        seq: usize,
    }

    impl Node<EchoPayload> for EchoNode {
        fn from_init(network: Network<EchoPayload>, _: String, _: Vec<String>) -> Self {
            EchoNode { network, seq: 0 }
        }

        fn handle_message(&mut self, msg: Message<EchoPayload>) -> Try {
            let EchoPayload::Echo { echo } = &msg.body.payload else {
                return Err("expected echo")?;
            };

            let echo = echo.clone();
            let reply = msg.into_reply(EchoPayload::EchoOk { echo });

            self.seq += 1;
            self.network.send(reply)?;
            Ok(())
        }
    }

    #[test]
    fn test_basic_init() -> Try {
        let (_, input, output) = run_node();

        let init = Message::new(
            "c2",
            "n1",
            BodyBuilder::new(Init::Init {
                node_id: "n1".into(),
                node_ids: vec!["n1".into()],
            })
            .msg_id(3)
            .build(),
        );

        input.send(serde_json::to_string(&init)?)?;
        let _: Message<Init> = serde_json::from_str(&output.recv()?)?;
        Ok(())
    }

    #[test]
    fn test_basic_echo() -> Try {
        let (_, input, output) = run_node();

        let init = Message::new(
            "c2",
            "n1",
            BodyBuilder::new(Init::Init {
                node_id: "n1".into(),
                node_ids: vec!["n1".into()],
            })
            .msg_id(3)
            .build(),
        );

        input.send(serde_json::to_string(&init)?)?;
        let _: Message<Init> = serde_json::from_str(&output.recv()?)?;

        let echo = Message::new(
            "c2",
            "n1",
            BodyBuilder::new(EchoPayload::Echo {
                echo: "ding-dong!".into(),
            })
            .msg_id(3)
            .build(),
        );

        input.send(serde_json::to_string(&echo)?)?;
        let _: Message<EchoPayload> = serde_json::from_str(&output.recv()?)?;
        Ok(())
    }

    fn run_node() -> (JoinHandle<()>, Sender<String>, Receiver<String>) {
        let (stdout_tx, stdout_rx) = channel();
        let (stdin_tx, stdin_rx) = channel();

        let runtime = thread::spawn(move || {
            Runtime::<EchoPayload, EchoNode>::run_internal(stdout_tx, stdin_rx).unwrap();
        });

        (runtime, stdin_tx, stdout_rx)
    }
}
