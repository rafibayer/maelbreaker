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

        let stdin_handle = thread::spawn(move || {
            let stdin = stdin().lock().lines();

            for line in stdin {
                let line = line.unwrap();
                stdin_tx.send(line).unwrap();
            }
        });

        let stdout_handle = thread::spawn(move || {
            let mut stdout = stdout().lock();

            for message in stdout_rx {
                writeln!(&mut stdout, "{message}").unwrap();
            }
        });

        Runtime::<P, N>::run_internal(stdout_tx, stdin_rx)?;

        stdin_handle.join().unwrap();
        stdout_handle.join().unwrap();

        Ok(())
    }

    fn run_internal(tx: Sender<String>, rx: Receiver<String>) -> Try {
        eprintln!("Starting runtime...\nWaiting for init message");
        let init = &rx.recv()?;
        eprintln!("Got init: {init}");
        let init: Message<Init> = serde_json::from_str(init)?;
        let Init::Init { node_id, node_ids } = &init.body.payload else {
            return Err("expected init as first message")?;
        };

        let (network, node_receiver) = Network::new();
        let node = N::from_init(network.clone(), node_id.clone(), node_ids.clone());

        // we are using a msg_id here that might be used by the node,
        // which is against protocol, but maelstrom doesn't seem to mind
        let reply = init.into_reply(Init::InitOk);

        eprintln!("Starting outbound processing and sending init_ok");
        let outbound = Runtime::<P, N>::process_output(reply, tx, node_receiver);

        eprintln!("Starting inbound processing");
        if let Err(e) = Runtime::process_input(rx, network, node) {
            eprintln!("failed to process input: {e:#?}");
        }

        eprintln!("Shutting down...");
        // Runtime::cleanup(node, outbound)
        todo!()
    }

    fn process_output(
        reply: Message<Init>,
        tx: Sender<String>,
        node_receiver: Receiver<Message<P>>,
    ) -> JoinHandle<SyncTry> {
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

        thread::spawn(move || {
            for line in rx {
                if line == EOI {
                    eprintln!("Got EOI");

                    break;
                }

                eprintln!("Got message: {line}");
                let message: Message<P> = serde_json::from_str(&line).unwrap();

                // oops! RPCs don't work because they wait for response input
                // but process_input won't process said input until handle_message returns
                // deadlocked!.
                // we can make this work by actually passing to the node in another thread, and just
                // trying the callbacks before the message goes to that thread.
                // Thread A (read stdin) -> thread B (check_callback) -> thread C (handle_message)
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

    fn cleanup(node: N, outbound: JoinHandle<SyncTry>) -> Try {
        eprintln!("starting node cleanup");

        drop(node);
        outbound
            .join()
            .map_err(|e| format!("outbound thread error: {e:#?}"))?
            .map_err(|_| "error processing error :~)")?;

        eprintln!("finished node cleanup");
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::{sync::mpsc::Sender, thread::JoinHandle};

    use crate::{payload, types::Body};

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

        let init = Message {
            src: "c2".into(),
            dest: "n1".into(),
            body: Body {
                msg_id: Some(3),
                in_reply_to: None,
                payload: Init::Init {
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            },
        };

        input.send(serde_json::to_string(&init)?)?;
        let _: Message<Init> = serde_json::from_str(&output.recv()?)?;
        Ok(())
    }

    #[test]
    fn test_basic_echo() -> Try {
        let (_, input, output) = run_node();

        let init = Message {
            src: "c2".into(),
            dest: "n1".into(),
            body: Body {
                msg_id: Some(3),
                in_reply_to: None,
                payload: Init::Init {
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            },
        };

        input.send(serde_json::to_string(&init)?)?;
        let _: Message<Init> = serde_json::from_str(&output.recv()?)?;

        let echo = Message {
            src: "c2".into(),
            dest: "n1".into(),
            body: Body {
                msg_id: Some(3),
                in_reply_to: None,
                payload: EchoPayload::Echo {
                    echo: "ding-dong!".into(),
                },
            },
        };

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
